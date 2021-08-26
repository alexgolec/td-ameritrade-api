from abc import ABC, abstractmethod
from collections import defaultdict, deque
from enum import Enum

import asyncio
import copy
import datetime
import httpx
import inspect
import json
import logging
import tda
import urllib.parse

import websockets.legacy.client as ws_client

from .utils import EnumEnforcer


class StreamJsonDecoder(ABC):
    @abstractmethod
    def decode_json_string(self, raw):
        '''
        Parse a JSON-formatted string into a proper object. Raises 
        ``JSONDecodeError`` on parse failure.
        '''
        raise NotImplementedError()


class NaiveJsonStreamDecoder(StreamJsonDecoder):
    def decode_json_string(self, raw):
        return json.loads(raw)


def get_logger():
    return logging.getLogger(__name__)


class _BaseFieldEnum(Enum):
    @classmethod
    def all_fields(cls):
        return list(cls)

    @classmethod
    def key_mapping(cls):
        try:
            return cls._key_mapping
        except AttributeError:
            cls._key_mapping = dict(
                (str(enum.value), name)
                for name, enum in cls.__members__.items())
            return cls._key_mapping

    @classmethod
    def relabel_message(cls, old_msg, new_msg):
        # Make a copy of the items so we can modify the dict during iteration
        for old_key, value in list(old_msg.items()):
            if old_key in cls.key_mapping():
                new_key = cls.key_mapping()[old_key]
                new_msg[new_key] = new_msg.pop(old_key)


class UnexpectedResponse(Exception):
    def __init__(self, response, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.response = response


class UnexpectedResponseCode(Exception):
    def __init__(self, response, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.response = response


class UnparsableMessage(Exception):
    def __init__(self, raw_msg, json_parse_exception, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.raw_msg = raw_msg
        self.json_parse_exception = json_parse_exception


class _Handler:
    def __init__(self, func, field_enum_type):
        self._func = func
        self._field_enum_type = field_enum_type

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def label_message(self, msg):
        if 'content' in msg:
            new_msg = copy.deepcopy(msg)
            for idx in range(len(msg['content'])):
                self._field_enum_type.relabel_message(msg['content'][idx],
                                                      new_msg['content'][idx])
            return new_msg
        else:
            return msg


class StreamClient(EnumEnforcer):

    def __init__(self, client, *, account_id=None,
                 enforce_enums=True, ssl_context=None):
        super().__init__(enforce_enums)

        self._ssl_context = ssl_context
        self._client = client

        # If None, will be set by the login() function
        self._account_id = account_id

        # Set by the login() function
        self._account = None
        self._stream_key = None
        self._socket = None
        self._source = None

        # Internal fields
        self._request_id = 0
        self._handlers = defaultdict(list)

        # When listening for responses, we sometimes encounter non-response
        # messages. Since this happens outside the context of the handler
        # dispatcher, we cannot handle these messages. However, we still need to
        # deliver these messages. This list records the messages that were read
        # from the stream but not handled yet. Messages should be read from this
        # list before they are read from the stream.
        self._overflow_items = deque()

        # Logging-related fields
        self.logger = get_logger()
        self.request_number = 0

        # Initialize the JSON parser to be the naive parser which directly calls 
        # ``json.loads``
        self.json_decoder = NaiveJsonStreamDecoder()


    def set_json_decoder(self, json_decoder):
        '''
        Sets a custom JSON decoder.

        :param json_decoder: Custom JSON decoder to use for to decode all 
                             incoming JSON strings. See
                             :class:`StreamJsonDecoder` for details.
        '''
        if not isinstance(json_decoder, tda.contrib.util.StreamJsonDecoder):
            raise ValueError('Custom JSON parser must be a subclass of ' +
                    'tda.contrib.util.StreamJsonDecoder')
        self.json_decoder = json_decoder


    def req_num(self):
        self.request_number += 1
        return self.request_number

    async def _send(self, obj):
        if self._socket is None:
            raise ValueError(
                'Socket not open. Did you forget to call login()?')

        self.logger.debug('Send {}: Sending {}'.format(
            self.req_num(), json.dumps(obj, indent=4)))

        await self._socket.send(json.dumps(obj))

    async def _receive(self):
        if self._socket is None:
            raise ValueError(
                'Socket not open. Did you forget to call login()?')

        if len(self._overflow_items) > 0:
            ret = self._overflow_items.pop()

            self.logger.debug(
                'Receive {}: Returning message from overflow: {}'.format(
                    self.req_num(), json.dumps(ret, indent=4)))
        else:
            raw = await self._socket.recv()
            try:
                ret = self.json_decoder.decode_json_string(raw)
            except json.decoder.JSONDecodeError as e:
                msg = ('Failed to parse message. This often happens with ' +
                       'unknown symbols or other error conditions. Full ' +
                       'message text: ' + raw)
                raise UnparsableMessage(raw, e, msg)

            self.logger.debug(
                'Receive {}: Returning message from stream: {}'.format(
                    self.req_num(), json.dumps(ret, indent=4)))

        return ret

    async def _init_from_principals(self, principals):
        # Initialize accounts and streamer keys.
        # Assume a 1-to-1 mapping of streamer keys to accounts.
        accounts = principals['accounts']
        num_accounts = len(accounts)
        assert num_accounts > 0, 'zero accounts found'

        # If there's only one account, use it. Otherwise require an account ID.
        if num_accounts == 1:
            self._account = accounts[0]
        else:
            if self._account_id is None:
                raise ValueError(
                    'multiple accounts found and StreamClient was ' +
                    'initialized with unspecified account_id')
            for idx, account in enumerate(accounts):
                if int(account['accountId']) == self._account_id:
                    self._account = account

        if self._account is None:
            raise ValueError(
                'no account found with account_id {}'.format(
                    self._account_id))

        if self._account_id is None:
            self._account_id = self._account['accountId']

        # Record streamer subscription keys
        stream_keys = principals['streamerSubscriptionKeys']['keys']
        if len(stream_keys) > 1:
            self.logger.warn('Found {} stream keys, using the first one'.format(
                len(stream_keys)))
        self._stream_key = stream_keys[0]['key']

        # Initialize socket
        wss_url = 'wss://{}/ws'.format(
            principals['streamerInfo']['streamerSocketUrl'])
        if self._ssl_context:
            self._socket = await ws_client.connect(
                wss_url, ssl=self._ssl_context)
        else:
            self._socket = await ws_client.connect(wss_url)

        # Initialize miscellaneous parameters
        self._source = principals['streamerInfo']['appId']

    def _make_request(self, *, service, command, parameters):
        request_id = self._request_id
        self._request_id += 1

        request = {
            'service': service,
            'requestid': str(request_id),
            'command': command,
            'account': self._account_id,
            'source': self._source,
            'parameters': parameters
        }

        return request, request_id

    async def _await_response(self, request_id, service, command):
        deferred_messages = []

        # Context handler to ensure we always append the deferred messages,
        # regardless of how we exit the await loop below
        class WriteDeferredMessages:
            def __init__(self, this_client):
                self.this_client = this_client

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.this_client._overflow_items.extendleft(deferred_messages)

        with WriteDeferredMessages(self):
            while True:
                resp = await self._receive()

                if 'response' not in resp:
                    deferred_messages.append(resp)
                    continue

                # Validate request ID
                resp_request_id = int(resp['response'][0]['requestid'])
                if resp_request_id != request_id:
                    raise UnexpectedResponse(
                        resp, 'unexpected requestid: {}'.format(
                            resp_request_id))

                # Validate service
                resp_service = resp['response'][0]['service']
                if resp_service != service:
                    raise UnexpectedResponse(
                        resp, 'unexpected service: {}'.format(
                            resp_service))

                # Validate command
                resp_command = resp['response'][0]['command']
                if resp_command != command:
                    raise UnexpectedResponse(
                        resp, 'unexpected command: {}'.format(
                            resp_command))

                # Validate response code
                resp_code = resp['response'][0]['content']['code']
                if resp_code != 0:
                    raise UnexpectedResponseCode(
                        resp,
                        'unexpected response code: {}, msg is \'{}\''.format(
                            resp_code,
                            resp['response'][0]['content']['msg']))

                break

    async def _service_op(self, symbols, service, command, field_type,
                          *, fields=None):
        if fields is None:
            fields = field_type.all_fields()
        fields = sorted(self.convert_enum_iterable(fields, field_type))

        if str(field_type(0)).split('.')[1] == 'SYMBOL':
            for i in range(len(symbols)):
                symbols[i] = symbols[i].upper()

        request, request_id = self._make_request(
            service=service, command=command,
            parameters={
                'keys': ','.join(symbols),
                'fields': ','.join(str(f) for f in fields)})

        await self._send({'requests': [request]})
        await self._await_response(request_id, service, command)

    async def handle_message(self):
        msg = await self._receive()

        # response
        if 'response' in msg:
            raise UnexpectedResponse(msg)

        # data
        if 'data' in msg:
            for d in msg['data']:
                if d['service'] in self._handlers:
                    for handler in self._handlers[d['service']]:
                        labeled_d = handler.label_message(d)
                        h = handler(labeled_d)

                        # Check if h is an awaitable, if so schedule it
                        # This allows for both sync and async handlers
                        if inspect.isawaitable(h):
                            asyncio.ensure_future(h)

        # notify
        if 'notify' in msg:
            for d in msg['notify']:
                if 'heartbeat' in d:
                    pass
                else:
                    for handler in self._handlers[d['service']]:
                        h = handler(d)

                        # Check if h is an awaitable, if so schedule oit
                        # This allows for both sync and async handlers
                        if inspect.isawaitable(h):
                            asyncio.ensure_future(h)

    ##########################################################################
    # LOGIN

    async def login(self):
        '''
        `Official Documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640574>`__

        Performs initial stream setup:
         * Fetches streaming information from the HTTP client's
           :meth:`~tda.client.Client.get_user_principals` method
         * Initializes the socket
         * Builds and sends and authentication request
         * Waits for response indicating login success

        All stream operations are available after this method completes.
        '''

        # Fetch required data and initialize the client

        # TODO: Figure out which of these are actually needed
        r = self._client.get_user_principals(fields=[
            self._client.UserPrincipals.Fields.STREAMER_CONNECTION_INFO,
            self._client.UserPrincipals.Fields.STREAMER_SUBSCRIPTION_KEYS])

        # We don't actually know whether the client is synchronous or
        # asynchronous, so work around by awaiting the response if necessary
        if inspect.iscoroutine(r):
            r = await r
        assert r.status_code == httpx.codes.OK, r.raise_for_status()
        r = r.json()

        await self._init_from_principals(r)

        # Build and send the request object
        token_ts = datetime.datetime.strptime(
            r['streamerInfo']['tokenTimestamp'], '%Y-%m-%dT%H:%M:%S%z')
        token_ts = int(token_ts.timestamp()) * 1000

        credentials = {
            'userid': self._account_id,
            'token': r['streamerInfo']['token'],
            'company': self._account['company'],
            'segment': self._account['segment'],
            'cddomain': self._account['accountCdDomainId'],
            'usergroup': r['streamerInfo']['userGroup'],
            'accesslevel': r['streamerInfo']['accessLevel'],
            'authorized': 'Y',
            'timestamp': token_ts,
            'appid': r['streamerInfo']['appId'],
            'acl': r['streamerInfo']['acl']
        }

        request_parameters = {
            'credential': urllib.parse.urlencode(credentials),
            'token': r['streamerInfo']['token'],
            'version': '1.0'
        }

        request, request_id = self._make_request(
            service='ADMIN', command='LOGIN',
            parameters=request_parameters)

        await self._send({'requests': [request]})
        await self._await_response(request_id, 'ADMIN', 'LOGIN')

    ##########################################################################
    # QOS

    class QOSLevel(Enum):
        '''Quality of service levels'''

        #: 500ms between updates. Fastest available
        EXPRESS = '0'

        #: 750ms between updates
        REAL_TIME = '1'

        #: 1000ms between updates. Default value.
        FAST = '2'

        #: 1500ms between updates
        MODERATE = '3'

        #: 3000ms between updates
        SLOW = '4'

        #: 5000ms between updates
        DELAYED = '5'

    async def quality_of_service(self, qos_level):
        '''
        `Official Documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640578>`__

        Specifies the frequency with which updated data should be sent to the
        client. If not called, the frequency will default to every second.

        :param qos_level: Quality of service level to request. See
                          :class:`QOSLevel` for options.
        '''

        qos_level = self.convert_enum(qos_level, self.QOSLevel)

        request, request_id = self._make_request(
            service='ADMIN', command='QOS',
            parameters={'qoslevel': qos_level})

        await self._send({'requests': [request]})
        await self._await_response(request_id, 'ADMIN', 'QOS')

    ##########################################################################
    # ACCT_ACTIVITY

    class AccountActivityFields(_BaseFieldEnum):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640580>`__

        Data fields for equity account activity. Primarily an implementation detail
        and not used in client code. Provided here as documentation for key
        values stored returned in the stream messages.
        '''

        #: Subscription key. Represented in the stream as the
        #: ``key`` field.
        SUBSCRIPTION_KEY = 0

        #: Account # subscribed
        ACCOUNT = 1

        #: Refer to the `message type table in the official documentation
        #: <https://developer.tdameritrade.com/content/streaming-data
        #: #_Toc504640581>`__
        MESSAGE_TYPE = 2

        #: The core data for the message.  Either XML Message data describing
        #: the update, ``NULL`` in some cases, or plain text in case of
        #: ``ERROR``.
        MESSAGE_DATA = 3

    async def account_activity_sub(self):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640580>`__

        Subscribe to account activity for the account id associated with this
        streaming client. See :class:`AccountActivityFields` for more info.
        '''
        await self._service_op(
            [self._stream_key], 'ACCT_ACTIVITY', 'SUBS',
            self.AccountActivityFields)

    def add_account_activity_handler(self, handler):
        '''
        Adds a handler to the account activity subscription. See
        :ref:`registering_handlers` for details.
        '''
        self._handlers['ACCT_ACTIVITY'].append(_Handler(handler,
                                                        self.AccountActivityFields))

    ##########################################################################
    # CHART_EQUITY

    class ChartEquityFields(_BaseFieldEnum):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640589>`__

        Data fields for equity OHLCV data. Primarily an implementation detail
        and not used in client code. Provided here as documentation for key
        values stored returned in the stream messages.
        '''

        #: Ticker symbol in upper case. Represented in the stream as the
        #: ``key`` field.
        SYMBOL = 0

        #: Opening price for the minute
        OPEN_PRICE = 1

        #: Highest price for the minute
        HIGH_PRICE = 2

        #: Chart’s lowest price for the minute
        LOW_PRICE = 3

        #: Closing price for the minute
        CLOSE_PRICE = 4

        #: Total volume for the minute
        VOLUME = 5

        #: Identifies the candle minute. Explicitly labeled "not useful" in the
        #: official documentation.
        SEQUENCE = 6

        #: Milliseconds since Epoch
        CHART_TIME = 7

        #: Documented as not useful, included for completeness
        CHART_DAY = 8

    async def chart_equity_subs(self, symbols):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640587>`__

        Subscribe to equity charts. Behavior is undefined if called multiple
        times.

        :param symbols: Equity symbols to subscribe to.'''
        await self._service_op(
            symbols, 'CHART_EQUITY', 'SUBS', self.ChartEquityFields,
            fields=self.ChartEquityFields.all_fields())

    async def chart_equity_add(self, symbols):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640588>`__

        Add a symbol to the equity charts subscription. Behavior is undefined
        if called before :meth:`chart_equity_subs`.

        :param symbols: Equity symbols to add to the subscription.
        '''
        await self._service_op(
            symbols, 'CHART_EQUITY', 'ADD', self.ChartEquityFields,
            fields=self.ChartEquityFields.all_fields())

    def add_chart_equity_handler(self, handler):
        '''
        Adds a handler to the equity chart subscription. See
        :ref:`registering_handlers` for details.
        '''
        self._handlers['CHART_EQUITY'].append(_Handler(handler,
                                                       self.ChartEquityFields))

    ##########################################################################
    # CHART_FUTURES

    class ChartFuturesFields(_BaseFieldEnum):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640592>`__

        Data fields for equity OHLCV data. Primarily an implementation detail
        and not used in client code. Provided here as documentation for key
        values stored returned in the stream messages.
        '''

        #: Ticker symbol in upper case. Represented in the stream as the
        #: ``key`` field.
        SYMBOL = 0

        #: Milliseconds since Epoch
        CHART_TIME = 1

        #: Opening price for the minute
        OPEN_PRICE = 2

        #: Highest price for the minute
        HIGH_PRICE = 3

        #: Chart’s lowest price for the minute
        LOW_PRICE = 4

        #: Closing price for the minute
        CLOSE_PRICE = 5

        #: Total volume for the minute
        VOLUME = 6

    async def chart_futures_subs(self, symbols):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640587>`__

        Subscribe to futures charts. Behavior is undefined if called multiple
        times.

        :param symbols: Futures symbols to subscribe to.
        '''
        await self._service_op(
            symbols, 'CHART_FUTURES', 'SUBS', self.ChartFuturesFields,
            fields=self.ChartFuturesFields.all_fields())

    async def chart_futures_add(self, symbols):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640590>`__

        Add a symbol to the futures chart subscription. Behavior is undefined
        if called before :meth:`chart_futures_subs`.

        :param symbols: Futures symbols to add to the subscription.
        '''
        await self._service_op(
            symbols, 'CHART_FUTURES', 'ADD', self.ChartFuturesFields,
            fields=self.ChartFuturesFields.all_fields())

    def add_chart_futures_handler(self, handler):
        '''
        Adds a handler to the futures chart subscription. See
        :ref:`registering_handlers` for details.
        '''
        self._handlers['CHART_FUTURES'].append(_Handler(handler,
                                                        self.ChartFuturesFields))

    ##########################################################################
    # QUOTE

    class LevelOneEquityFields(_BaseFieldEnum):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640599>`__

        Fields for equity quotes.
        '''

        #: Ticker symbol in upper case. Represented in the stream as the
        #: ``key`` field.
        SYMBOL = 0

        #: Current Best Bid Price
        BID_PRICE = 1

        #: Current Best Ask Price
        ASK_PRICE = 2

        #: Price at which the last trade was matched
        LAST_PRICE = 3

        #: Number of shares for bid
        BID_SIZE = 4

        #: Number of shares for ask
        ASK_SIZE = 5

        #: Exchange with the best ask
        ASK_ID = 6

        #: Exchange with the best bid
        BID_ID = 7

        #: Aggregated shares traded throughout the day, including pre/post
        #: market hours. Note volume is set to zero at 7:28am ET.
        TOTAL_VOLUME = 8

        #: Number of shares traded with last trade, in 100's
        LAST_SIZE = 9

        #: Trade time of the last trade, in seconds since midnight EST
        TRADE_TIME = 10

        #: Trade time of the last quote, in seconds since midnight EST
        QUOTE_TIME = 11

        #: Day’s high trade price. Notes:
        #:
        #:  * According to industry standard, only regular session trades set
        #:    the High and Low.
        #:  * If a stock does not trade in the AM session, high and low will be
        #:    zero.
        #:  * High/low reset to 0 at 7:28am ET
        HIGH_PRICE = 12

        #: Day’s low trade price. Same notes as ``HIGH_PRICE``.
        LOW_PRICE = 13

        #: Indicates Up or Downtick (NASDAQ NMS & Small Cap). Updates whenever
        #: bid updates.
        BID_TICK = 14

        #: Previous day’s closing price. Notes:
        #:
        #:  * Closing prices are updated from the DB when Pre-Market tasks are
        #:    run by TD Ameritrade at 7:29AM ET.
        #:  * As long as the symbol is valid, this data is always present.
        #:  * This field is updated every time the closing prices are loaded
        #:    from DB
        CLOSE_PRICE = 15

        # TODO: Write a wrapper around this to make it easier to interpret.
        #: Primary "listing" Exchange.
        EXCHANGE_ID = 16

        #: Stock approved by the Federal Reserve and an investor's broker as
        #: being suitable for providing collateral for margin debt?
        MARGINABLE = 17

        #: Stock can be sold short?
        SHORTABLE = 18

        #: Deprecated, documented for completeness.
        ISLAND_BID_DEPRECATED = 19

        #: Deprecated, documented for completeness.
        ISLAND_ASK_DEPRECATED = 20

        #: Deprecated, documented for completeness.
        ISLAND_VOLUME_DEPRECATED = 21

        #: Day of the quote
        QUOTE_DAY = 22

        #: Day of the trade
        TRADE_DAY = 23

        #: Option Risk/Volatility Measurement. Notes:
        #:
        #:  * Volatility is reset to 0 when Pre-Market tasks are run at 7:28 AM
        #:    ET
        #:  * Once per day descriptions are loaded from the database when
        #:    Pre-Market tasks are run at 7:29:50 AM ET.
        VOLATILITY = 24

        #: A company, index or fund name
        DESCRIPTION = 25

        #: Exchange where last trade was executed
        LAST_ID = 26

        #: Valid decimal points. 4 digits for AMEX, NASDAQ, OTCBB, and PINKS,
        #: 2 for others.
        DIGITS = 27

        #: Day's Open Price. Notes:
        #:
        #:  * Open is set to ZERO when Pre-Market tasks are run at 7:28.
        #:  * If a stock doesn’t trade the whole day, then the open price is 0.
        #:  * In the AM session, Open is blank because the AM session trades do
        #:    not set the open.
        OPEN_PRICE = 28

        #: Current Last-Prev Close
        NET_CHANGE = 29

        #: Highest price traded in the past 12 months, or 52 weeks
        HIGH_52_WEEK = 30

        #: Lowest price traded in the past 12 months, or 52 weeks
        LOW_52_WEEK = 31

        #: Price to earnings ratio
        PE_RATIO = 32

        #: Dividen earnings Per Share
        DIVIDEND_AMOUNT = 33

        #: Dividend Yield
        DIVIDEND_YIELD = 34

        #: Deprecated, documented for completeness.
        ISLAND_BID_SIZE_DEPRECATED = 35

        #: Deprecated, documented for completeness.
        ISLAND_ASK_SIZE_DEPRECATED = 36

        #: Mutual Fund Net Asset Value
        NAV = 37

        #: Mutual fund price
        FUND_PRICE = 38

        #: Display name of exchange
        EXCHANGE_NAME = 39

        #: Dividend date
        DIVIDEND_DATE = 40

        #: Is last quote a regular quote
        IS_REGULAR_MARKET_QUOTE = 41

        #: Is last trade a regular trade
        IS_REGULAR_MARKET_TRADE = 42

        #: Last price, only used when ``IS_REGULAR_MARKET_TRADE`` is ``True``
        REGULAR_MARKET_LAST_PRICE = 43

        #: Last trade size, only used when ``IS_REGULAR_MARKET_TRADE`` is ``True``
        REGULAR_MARKET_LAST_SIZE = 44

        #: Last trade time, only used when ``IS_REGULAR_MARKET_TRADE`` is ``True``
        REGULAR_MARKET_TRADE_TIME = 45

        #: Last trade date, only used when ``IS_REGULAR_MARKET_TRADE`` is ``True``
        REGULAR_MARKET_TRADE_DAY = 46

        #: ``REGULAR_MARKET_LAST_PRICE`` minus ``CLOSE_PRICE``
        REGULAR_MARKET_NET_CHANGE = 47

        #: Indicates a symbols current trading status, Normal, Halted, Closed
        SECURITY_STATUS = 48

        #: Mark Price
        MARK = 49

        #: Last quote time in milliseconds since Epoch
        QUOTE_TIME_IN_LONG = 50

        #: Last trade time in milliseconds since Epoch
        TRADE_TIME_IN_LONG = 51

        #: Regular market trade time in milliseconds since Epoch
        REGULAR_MARKET_TRADE_TIME_IN_LONG = 52

    async def level_one_equity_subs(self, symbols, *, fields=None):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640599>`__

        Subscribe to level one equity quote data.

        :param symbols: Equity symbols to receive quotes for
        :param fields: Iterable of :class:`LevelOneEquityFields` representing
                       the fields to return in streaming entries. If unset, all
                       fields will be requested.
        '''
        if fields and self.LevelOneEquityFields.SYMBOL not in fields:
            fields.append(self.LevelOneEquityFields.SYMBOL)
        await self._service_op(
            symbols, 'QUOTE', 'SUBS', self.LevelOneEquityFields,
            fields=fields)

    def add_level_one_equity_handler(self, handler):
        '''
        Register a function to handle level one equity quotes as they are sent.
        See :ref:`registering_handlers` for details.
        '''
        self._handlers['QUOTE'].append(_Handler(handler,
                                                self.LevelOneEquityFields))

    ##########################################################################
    # OPTION

    class LevelOneOptionFields(_BaseFieldEnum):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640601>`__
        '''

        #: Ticker symbol in upper case. Represented in the stream as the
        #: ``key`` field.
        SYMBOL = 0

        #: A company, index or fund name
        DESCRIPTION = 1

        #: Current Best Bid Price
        BID_PRICE = 2

        #: Current Best Ask Price
        ASK_PRICE = 3

        #: Price at which the last trade was matched
        LAST_PRICE = 4

        #: Day’s high trade price. Notes:
        #:
        #:  * According to industry standard, only regular session trades set
        #:    the High and Low.
        #:  * If an option does not trade in the AM session, high and low will
        #:    be zero.
        #:  * High/low reset to 0 at 7:28am ET.
        HIGH_PRICE = 5

        #: Day’s low trade price. Same notes as ``HIGH_PRICE``.
        LOW_PRICE = 6

        #: Previous day’s closing price. Closing prices are updated from the
        #: DB when Pre-Market tasks are run at 7:29AM ET.
        CLOSE_PRICE = 7

        #: Aggregated shares traded throughout the day, including pre/post
        #: market hours. Reset to zero at 7:28am ET.
        TOTAL_VOLUME = 8

        #: Open interest
        OPEN_INTEREST = 9

        #: Option Risk/Volatility Measurement. Volatility is reset to 0 when
        #: Pre-Market tasks are run at 7:28 AM ET.
        VOLATILITY = 10

        #: Trade time of the last quote in seconds since midnight EST
        QUOTE_TIME = 11

        #: Trade time of the last quote in seconds since midnight EST
        TRADE_TIME = 12

        #: Money intrinsic value
        MONEY_INTRINSIC_VALUE = 13

        #: Day of the quote
        QUOTE_DAY = 14

        #: Day of the trade
        TRADE_DAY = 15

        #: Option expiration year
        EXPIRATION_YEAR = 16

        #: Option multiplier
        MULTIPLIER = 17

        #: Valid decimal points. 4 digits for AMEX, NASDAQ, OTCBB, and PINKS,
        #: 2 for others.
        DIGITS = 18

        #: Day's Open Price. Notes:
        #:
        #:  * Open is set to ZERO when Pre-Market tasks are run at 7:28.
        #:  * If a stock doesn’t trade the whole day, then the open price is 0.
        #:  * In the AM session, Open is blank because the AM session trades do
        #:    not set the open.
        OPEN_PRICE = 19

        #: Number of shares for bid
        BID_SIZE = 20

        #: Number of shares for ask
        ASK_SIZE = 21

        #: Number of shares traded with last trade, in 100's
        LAST_SIZE = 22

        #: Current Last-Prev Close
        NET_CHANGE = 23
        STRIKE_PRICE = 24
        CONTRACT_TYPE = 25
        UNDERLYING = 26
        EXPIRATION_MONTH = 27
        DELIVERABLES = 28
        TIME_VALUE = 29
        EXPIRATION_DAY = 30
        DAYS_TO_EXPIRATION = 31
        DELTA = 32
        GAMMA = 33
        THETA = 34
        VEGA = 35
        RHO = 36

        #: Indicates a symbols current trading status, Normal, Halted, Closed
        SECURITY_STATUS = 37
        THEORETICAL_OPTION_VALUE = 38
        UNDERLYING_PRICE = 39
        UV_EXPIRATION_TYPE = 40

        #: Mark Price
        MARK = 41

    async def level_one_option_subs(self, symbols, *, fields=None):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640602>`__

        Subscribe to level one option quote data.

        :param symbols: Option symbols to receive quotes for
        :param fields: Iterable of :class:`LevelOneOptionFields` representing
                       the fields to return in streaming entries. If unset, all
                       fields will be requested.
        '''
        if fields and self.LevelOneOptionFields.SYMBOL not in fields:
            fields.append(self.LevelOneOptionFields.SYMBOL)
        await self._service_op(
            symbols, 'OPTION', 'SUBS', self.LevelOneOptionFields,
            fields=fields)

    def add_level_one_option_handler(self, handler):
        '''
        Register a function to handle level one options quotes as they are sent.
        See :ref:`registering_handlers` for details.
        '''
        self._handlers['OPTION'].append(_Handler(handler,
                                                 self.LevelOneOptionFields))

    ##########################################################################
    # LEVELONE_FUTURES

    class LevelOneFuturesFields(_BaseFieldEnum):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640603>`__
        '''

        #: Ticker symbol in upper case. Represented in the stream as the
        #: ``key`` field.
        SYMBOL = 0

        #: Current Best Bid Price
        BID_PRICE = 1

        #: Current Best Ask Price
        ASK_PRICE = 2

        #: Price at which the last trade was matched
        LAST_PRICE = 3

        #: Number of shares for bid
        BID_SIZE = 4

        #: Number of shares for ask
        ASK_SIZE = 5

        #: Exchange with the best ask
        ASK_ID = 6

        #: Exchange with the best bid
        BID_ID = 7

        #: Aggregated shares traded throughout the day, including pre/post
        #: market hours
        TOTAL_VOLUME = 8

        #: Number of shares traded with last trade
        LAST_SIZE = 9

        #: Trade time of the last quote in milliseconds since epoch
        QUOTE_TIME = 10

        #: Trade time of the last trade in milliseconds since epoch
        TRADE_TIME = 11

        #: Day’s high trade price
        HIGH_PRICE = 12

        #: Day’s low trade price
        LOW_PRICE = 13

        #: Previous day’s closing price
        CLOSE_PRICE = 14

        #: Primary "listing" Exchange. Notes:
        #:  * I → ICE
        #:  * E → CME
        #:  * L → LIFFEUS
        EXCHANGE_ID = 15

        #: Description of the product
        DESCRIPTION = 16

        #: Exchange where last trade was executed
        LAST_ID = 17

        #: Day's Open Price
        OPEN_PRICE = 18

        #: Current Last-Prev Close
        NET_CHANGE = 19

        #: Current percent change
        FUTURE_PERCENT_CHANGE = 20

        #: Name of exchange
        EXCHANGE_NAME = 21

        #: Trading status of the symbol. Indicates a symbol's current trading
        #: status, Normal, Halted, Closed.
        SECURITY_STATUS = 22

        #: The total number of futures ontracts that are not closed or delivered
        #: on a particular day
        OPEN_INTEREST = 23

        #: Mark-to-Market value is calculated daily using current prices to
        #: determine profit/loss
        MARK = 24

        #: Minimum price movement
        TICK = 25

        #: Minimum amount that the price of the market can change
        TICK_AMOUNT = 26

        #: Futures product
        PRODUCT = 27

        #: Display in fraction or decimal format.
        FUTURE_PRICE_FORMAT = 28

        #: Trading hours. Notes:
        #:
        #:  * days: 0 = monday-friday, 1 = sunday.
        #:  * 7 = Saturday
        #:  * 0 = [-2000,1700] ==> open, close
        #:  * 1 = [-1530,-1630,-1700,1515] ==> open, close, open, close
        #:  * 0 = [-1800,1700,d,-1700,1900] ==> open, close, DST-flag, open, close
        #:  * If the DST-flag is present, the following hours are for DST days:
        #:    http://www.cmegroup.com/trading_hours/
        FUTURE_TRADING_HOURS = 29

        #: Flag to indicate if this future contract is tradable
        FUTURE_IS_TRADEABLE = 30

        #: Point value
        FUTURE_MULTIPLIER = 31

        #: Indicates if this contract is active
        FUTURE_IS_ACTIVE = 32

        #: Closing price
        FUTURE_SETTLEMENT_PRICE = 33

        #: Symbol of the active contract
        FUTURE_ACTIVE_SYMBOL = 34

        #: Expiration date of this contract in milliseconds since epoch
        FUTURE_EXPIRATION_DATE = 35

    async def level_one_futures_subs(self, symbols, *, fields=None):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640604>`__

        Subscribe to level one futures quote data.

        :param symbols: Futures symbols to receive quotes for
        :param fields: Iterable of :class:`LevelOneFuturesFields` representing
                       the fields to return in streaming entries. If unset, all
                       fields will be requested.
        '''
        if fields and self.LevelOneFuturesFields.SYMBOL not in fields:
            fields.append(self.LevelOneFuturesFields.SYMBOL)
        await self._service_op(
            symbols, 'LEVELONE_FUTURES', 'SUBS', self.LevelOneFuturesFields,
            fields=fields)

    def add_level_one_futures_handler(self, handler):
        '''
        Register a function to handle level one futures quotes as they are sent.
        See :ref:`registering_handlers` for details.
        '''
        self._handlers['LEVELONE_FUTURES'].append(
            _Handler(handler, self.LevelOneFuturesFields))

    ##########################################################################
    # LEVELONE_FOREX

    class LevelOneForexFields(_BaseFieldEnum):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640606>`__
        '''

        #: Ticker symbol in upper case. Represented in the stream as the
        #: ``key`` field.
        SYMBOL = 0

        #: Current Best Bid Price
        BID_PRICE = 1

        #: Current Best Ask Price
        ASK_PRICE = 2

        #: Price at which the last trade was matched
        LAST_PRICE = 3

        #: Number of shares for bid
        BID_SIZE = 4

        #: Number of shares for ask
        ASK_SIZE = 5

        #: Aggregated shares traded throughout the day, including pre/post
        #: market hours
        TOTAL_VOLUME = 6

        #: Number of shares traded with last trade
        LAST_SIZE = 7

        #: Trade time of the last quote in milliseconds since epoch
        QUOTE_TIME = 8

        #: Trade time of the last trade in milliseconds since epoch
        TRADE_TIME = 9

        #: Day’s high trade price
        HIGH_PRICE = 10

        #: Day’s low trade price
        LOW_PRICE = 11

        #: Previous day’s closing price
        CLOSE_PRICE = 12

        #: Primary "listing" Exchange
        EXCHANGE_ID = 13

        #: Description of the product
        DESCRIPTION = 14

        #: Day's Open Price
        OPEN_PRICE = 15

        #: Current Last-Prev Close
        NET_CHANGE = 16

        # Disabled because testing indicates the API returns some unparsable
        # characters
        # PERCENT_CHANGE = 17

        #: Name of exchange
        EXCHANGE_NAME = 18

        #: Valid decimal points
        DIGITS = 19

        #: Trading status of the symbol. Indicates a symbols current trading
        #: status, Normal, Halted, Closed.
        SECURITY_STATUS = 20

        #: Minimum price movement
        TICK = 21

        #: Minimum amount that the price of the market can change
        TICK_AMOUNT = 22

        #: Product name
        PRODUCT = 23

        # XXX: Documentation has TRADING_HOURS as 23, but testing suggests it's
        # actually 23. See here for details:
        # https://developer.tdameritrade.com/content/streaming-data#_Toc504640606
        #: Trading hours
        TRADING_HOURS = 24

        #: Flag to indicate if this forex is tradable
        IS_TRADABLE = 25

        MARKET_MAKER = 26

        #: Higest price traded in the past 12 months, or 52 weeks
        HIGH_52_WEEK = 27

        #: Lowest price traded in the past 12 months, or 52 weeks
        LOW_52_WEEK = 28

        #: Mark-to-Market value is calculated daily using current prices to
        #: determine profit/loss
        MARK = 29

    async def level_one_forex_subs(self, symbols, *, fields=None):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640606>`__

        Subscribe to level one forex quote data.

        :param symbols: Forex symbols to receive quotes for
        :param fields: Iterable of :class:`LevelOneForexFields` representing
                       the fields to return in streaming entries. If unset, all
                       fields will be requested.
        '''
        if fields and self.LevelOneForexFields.SYMBOL not in fields:
            fields.append(self.LevelOneForexFields.SYMBOL)
        await self._service_op(
            symbols, 'LEVELONE_FOREX', 'SUBS', self.LevelOneForexFields,
            fields=fields)

    def add_level_one_forex_handler(self, handler):
        '''
        Register a function to handle level one forex quotes as they are sent.
        See :ref:`registering_handlers` for details.
        '''
        self._handlers['LEVELONE_FOREX'].append(_Handler(handler,
                                                         self.LevelOneForexFields))

    ##########################################################################
    # LEVELONE_FUTURES_OPTIONS

    class LevelOneFuturesOptionsFields(_BaseFieldEnum):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640609>`__
        '''

        #: Ticker symbol in upper case. Represented in the stream as the
        #: ``key`` field.
        SYMBOL = 0

        #: Current Best Bid Price
        BID_PRICE = 1

        #: Current Best Ask Price
        ASK_PRICE = 2

        #: Price at which the last trade was matched
        LAST_PRICE = 3

        #: Number of shares for bid
        BID_SIZE = 4

        #: Number of shares for ask
        ASK_SIZE = 5

        #: Exchange with the best ask
        ASK_ID = 6

        #: Exchange with the best bid
        BID_ID = 7

        #: Aggregated shares traded throughout the day, including pre/post
        #: market hours
        TOTAL_VOLUME = 8

        #: Number of shares traded with last trade
        LAST_SIZE = 9

        #: Trade time of the last quote in milliseconds since epoch
        QUOTE_TIME = 10

        #: Trade time of the last trade in milliseconds since epoch
        TRADE_TIME = 11

        #: Day’s high trade price
        HIGH_PRICE = 12

        #: Day’s low trade price
        LOW_PRICE = 13

        #: Previous day’s closing price
        CLOSE_PRICE = 14

        #: Primary "listing" Exchange. Notes:
        #:  * I → ICE
        #:  * E → CME
        #:  * L → LIFFEUS
        EXCHANGE_ID = 15

        #: Description of the product
        DESCRIPTION = 16

        #: Exchange where last trade was executed
        LAST_ID = 17

        #: Day's Open Price
        OPEN_PRICE = 18

        #: Current Last-Prev Close
        NET_CHANGE = 19

        #: Current percent change
        FUTURE_PERCENT_CHANGE = 20

        #: Name of exchange
        EXCHANGE_NAME = 21

        #: Trading status of the symbol. Indicates a symbols current trading
        #: status, Normal, Halted, Closed.
        SECURITY_STATUS = 22

        #: The total number of futures ontracts that are not closed or delivered
        #: on a particular day
        OPEN_INTEREST = 23

        #: Mark-to-Market value is calculated daily using current prices to
        #: determine profit/loss
        MARK = 24

        #: Minimum price movement
        TICK = 25

        #: Minimum amount that the price of the market can change
        TICK_AMOUNT = 26

        #: Futures product
        PRODUCT = 27

        #: Display in fraction or decimal format
        FUTURE_PRICE_FORMAT = 28

        #: Trading hours
        FUTURE_TRADING_HOURS = 29

        #: Flag to indicate if this future contract is tradable
        FUTURE_IS_TRADEABLE = 30

        #: Point value
        FUTURE_MULTIPLIER = 31

        #: Indicates if this contract is active
        FUTURE_IS_ACTIVE = 32

        #: Closing price
        FUTURE_SETTLEMENT_PRICE = 33

        #: Symbol of the active contract
        FUTURE_ACTIVE_SYMBOL = 34

        #: Expiration date of this contract, in milliseconds since epoch
        FUTURE_EXPIRATION_DATE = 35

    async def level_one_futures_options_subs(self, symbols, *, fields=None):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640610>`__

        Subscribe to level one futures options quote data.

        :param symbols: Futures options symbols to receive quotes for
        :param fields: Iterable of :class:`LevelOneFuturesOptionsFields`
                       representing the fields to return in streaming entries.
                       If unset, all fields will be requested.
        '''
        if fields and self.LevelOneFuturesOptionsFields.SYMBOL not in fields:
            fields.append(self.LevelOneFuturesOptionsFields.SYMBOL)
        await self._service_op(
            symbols, 'LEVELONE_FUTURES_OPTIONS', 'SUBS',
            self.LevelOneFuturesOptionsFields, fields=fields)

    def add_level_one_futures_options_handler(self, handler):
        '''
        Register a function to handle level one futures options quotes as they
        are sent. See :ref:`registering_handlers` for details.
        '''
        self._handlers['LEVELONE_FUTURES_OPTIONS'].append(
            _Handler(handler, self.LevelOneFuturesOptionsFields))

    ##########################################################################
    # TIMESALE

    class TimesaleFields(_BaseFieldEnum):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640626>`__
        '''

        #: Ticker symbol in upper case. Represented in the stream as the
        #: ``key`` field.
        SYMBOL = 0

        #: Trade time of the last trade in milliseconds since epoch
        TRADE_TIME = 1

        #: Price at which the last trade was matched
        LAST_PRICE = 2

        #: Number of shares traded with last trade
        LAST_SIZE = 3

        #: Number of shares for bid
        LAST_SEQUENCE = 4

    async def timesale_equity_subs(self, symbols, *, fields=None):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640628>`__

        Subscribe to time of sale notifications for equities.

        :param symbols: Equity symbols to subscribe to
        '''
        if fields and self.TimesaleFields.SYMBOL not in fields:
            fields.append(self.TimesaleFields.SYMBOL)
        await self._service_op(
            symbols, 'TIMESALE_EQUITY', 'SUBS',
            self.TimesaleFields, fields=fields)

    def add_timesale_equity_handler(self, handler):
        '''
        Register a function to handle equity trade notifications as they happen
        See :ref:`registering_handlers` for details.
        '''
        self._handlers['TIMESALE_EQUITY'].append(_Handler(handler,
                                                          self.TimesaleFields))

    async def timesale_futures_subs(self, symbols, *, fields=None):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640628>`__

        Subscribe to time of sale notifications for futures.

        :param symbols: Futures symbols to subscribe to
        '''
        if fields and self.TimesaleFields.SYMBOL not in fields:
            fields.append(self.TimesaleFields.SYMBOL)
        await self._service_op(
            symbols, 'TIMESALE_FUTURES', 'SUBS',
            self.TimesaleFields, fields=fields)

    def add_timesale_futures_handler(self, handler):
        '''
        Register a function to handle futures trade notifications as they happen
        See :ref:`registering_handlers` for details.
        '''
        self._handlers['TIMESALE_FUTURES'].append(_Handler(handler,
                                                           self.TimesaleFields))

    async def timesale_options_subs(self, symbols, *, fields=None):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640628>`__

        Subscribe to time of sale notifications for options.

        :param symbols: Options symbols to subscribe to
        '''
        if fields and self.TimesaleFields.SYMBOL not in fields:
            fields.append(self.TimesaleFields.SYMBOL)
        await self._service_op(
            symbols, 'TIMESALE_OPTIONS', 'SUBS',
            self.TimesaleFields, fields=fields)

    def add_timesale_options_handler(self, handler):
        '''
        Register a function to handle options trade notifications as they happen
        See :ref:`registering_handlers` for details.
        '''
        self._handlers['TIMESALE_OPTIONS'].append(_Handler(handler,
                                                           self.TimesaleFields))

    ##########################################################################
    # Common book utilities

    class BookFields(_BaseFieldEnum):
        SYMBOL = 0
        BOOK_TIME = 1
        BIDS = 2
        ASKS = 3

    class BidFields(_BaseFieldEnum):
        BID_PRICE = 0
        TOTAL_VOLUME = 1
        NUM_BIDS = 2
        BIDS = 3

    class PerExchangeBidFields(_BaseFieldEnum):
        EXCHANGE = 0
        BID_VOLUME = 1
        SEQUENCE = 2

    class AskFields(_BaseFieldEnum):
        ASK_PRICE = 0
        TOTAL_VOLUME = 1
        NUM_ASKS = 2
        ASKS = 3

    class PerExchangeAskFields(_BaseFieldEnum):
        EXCHANGE = 0
        ASK_VOLUME = 1
        SEQUENCE = 2

    class _BookHandler(_Handler):
        def label_message(self, msg):
            # Relabel top-level fields
            new_msg = super().label_message(msg)

            # Relabel bids
            for content in new_msg['content']:
                if 'BIDS' in content:
                    for bid in content['BIDS']:
                        # Relabel top-level bids
                        StreamClient.BidFields.relabel_message(bid, bid)

                        # Relabel per-exchange bids
                        for e_bid in bid['BIDS']:
                            StreamClient.PerExchangeBidFields.relabel_message(
                                e_bid, e_bid)

            # Relabel asks
            for content in new_msg['content']:
                if 'ASKS' in content:
                    for ask in content['ASKS']:
                        # Relabel top-level asks
                        StreamClient.AskFields.relabel_message(ask, ask)

                        # Relabel per-exchange bids
                        for e_ask in ask['ASKS']:
                            StreamClient.PerExchangeAskFields.relabel_message(
                                e_ask, e_ask)

            return new_msg

    ##########################################################################
    # LISTED_BOOK

    async def listed_book_subs(self, symbols):
        '''
        Subscribe to the NYSE level two order book. Note this stream has no
        official documentation.
        '''
        await self._service_op(
            symbols, 'LISTED_BOOK', 'SUBS',
            self.BookFields, fields=self.BookFields.all_fields())

    def add_listed_book_handler(self, handler):
        '''
        Register a function to handle level two NYSE book data as it is updated
        See :ref:`registering_handlers` for details.
        '''
        self._handlers['LISTED_BOOK'].append(
            self._BookHandler(handler, self.BookFields))

    ##########################################################################
    # NASDAQ_BOOK

    async def nasdaq_book_subs(self, symbols):
        '''
        Subscribe to the NASDAQ level two order book. Note this stream has no
        official documentation.
        '''
        await self._service_op(symbols, 'NASDAQ_BOOK', 'SUBS',
                               self.BookFields,
                               fields=self.BookFields.all_fields())

    def add_nasdaq_book_handler(self, handler):
        '''
        Register a function to handle level two NASDAQ book data as it is
        updated See :ref:`registering_handlers` for details.
        '''
        self._handlers['NASDAQ_BOOK'].append(
            self._BookHandler(handler, self.BookFields))

    ##########################################################################
    # OPTIONS_BOOK

    async def options_book_subs(self, symbols):
        '''
        Subscribe to the level two order book for options. Note this stream has no
        official documentation, and it's not entirely clear what exchange it
        corresponds to. Use at your own risk.
        '''
        await self._service_op(symbols, 'OPTIONS_BOOK', 'SUBS',
                               self.BookFields,
                               fields=self.BookFields.all_fields())

    def add_options_book_handler(self, handler):
        '''
        Register a function to handle level two options book data as it is
        updated See :ref:`registering_handlers` for details.
        '''
        self._handlers['OPTIONS_BOOK'].append(
            self._BookHandler(handler, self.BookFields))

    ##########################################################################
    # NEWS_HEADLINE

    class NewsHeadlineFields(_BaseFieldEnum):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640626>`__
        '''

        #: Ticker symbol in upper case. Represented in the stream as the
        #: ``key`` field.
        SYMBOL = 0

        #: Specifies if there is any error
        ERROR_CODE = 1

        #: Headline’s datetime in milliseconds since epoch
        STORY_DATETIME = 2

        #: Unique ID for the headline
        HEADLINE_ID = 3
        STATUS = 4

        #: News headline
        HEADLINE = 5
        STORY_ID = 6
        COUNT_FOR_KEYWORD = 7
        KEYWORD_ARRAY = 8
        IS_HOT = 9
        STORY_SOURCE = 10

    async def news_headline_subs(self, symbols):
        '''
        `Official documentation <https://developer.tdameritrade.com/content/
        streaming-data#_Toc504640626>`__

        Subscribe to news headlines related to the given symbols.
        '''
        await self._service_op(symbols, 'NEWS_HEADLINE', 'SUBS',
                               self.NewsHeadlineFields,
                               fields=self.NewsHeadlineFields.all_fields())

    def add_news_headline_handler(self, handler):
        '''
        Register a function to handle news headlines as they are provided. See
        :ref:`registering_handlers` for details.
        '''
        self._handlers['NEWS_HEADLINE'].append(
            self._BookHandler(handler, self.NewsHeadlineFields))
