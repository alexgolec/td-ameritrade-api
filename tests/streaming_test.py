import tda
import urllib.parse
import json
import copy
from .utils import (account_principals, has_diff, MockResponse,
                    no_duplicates, AsyncMagicMock)
import asynctest
from unittest.mock import ANY, call, MagicMock, Mock
from tda import streaming

StreamClient = streaming.StreamClient


ACCOUNT_ID = 1000
TOKEN_TIMESTAMP = '2020-05-22T02:12:48+0000'


class StreamClientTest(asynctest.TestCase):

    def setUp(self):
        self.http_client = MagicMock()
        self.client = StreamClient(self.http_client)

        self.maxDiff = None

    def account(self, index):
        account = account_principals()['accounts'][0]
        account['accountId'] = str(ACCOUNT_ID + index)

        def parsable_as_int(s):
            try:
                int(s)
                return True
            except ValueError:
                return False
        for key, value in list(account.items()):
            if isinstance(value, str) and not parsable_as_int(value):
                account[key] = value + '-' + str(account['accountId'])
        return account

    def stream_key(self, index):
        return {'key': 'streamerSubscriptionKeys-keys-key' + str(index)}

    def request_from_socket_mock(self, socket):
        return json.loads(
            socket.send.call_args_list[0][0][0])['requests'][0]

    def success_response(self, request_id, service, command):
        return {
            'response': [
                {
                    'service': service,
                    'requestid': str(request_id),
                    'command': command,
                    'timestamp': 1590116673258,
                    'content': {
                        'code': 0,
                        'msg': 'success'
                    }
                }
            ]
        }

    def streaming_entry(self, service, command, content=None):
        d = {
            'data': [{
                'service': service,
                'command': command,
                'timestamp': 1590186642440
            }]
        }

        if content:
            d['data'][0]['content'] = content

        return d

    def assert_handler_called_once_with(self, handler, expected):
        handler.assert_called_once()
        self.assertEqual(len(handler.call_args_list[0]), 2)
        data = handler.call_args_list[0][0][0] # Mock from <- 3.7 has a bad api

        self.assertFalse(has_diff(data, expected))

    async def login_and_get_socket(self, ws_connect):
        principals = account_principals()

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, 200)
        socket = AsyncMagicMock()
        ws_connect.return_value = socket

        socket.recv.side_effect = [json.dumps(self.success_response(
            0, 'ADMIN', 'LOGIN'))]

        await self.client.login()

        socket.reset_mock()
        return socket

    ##########################################################################
    # Custom JSON Decoder


    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_default_parser_invalid_message(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = ['invalid json']

        # No custom parser
        msg = ('Failed to parse message. This often happens with ' +
               'unknown symbols or other error conditions. Full ' +
               'message text:')
        with self.assertRaisesRegex(tda.streaming.UnparsableMessage, msg):
            await self.client.level_one_equity_subs(['GOOG', 'MSFT'])


    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_custom_parser_invalid_message(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = ['invalid json']

        class CustomJsonDecoder(tda.contrib.util.StreamJsonDecoder):
            def decode_json_string(_, raw):
                self.assertEqual(raw, 'invalid json')
                return self.success_response(1, 'QUOTE', 'SUBS')

        self.client.set_json_decoder(CustomJsonDecoder())
        await self.client.level_one_equity_subs(['GOOG', 'MSFT'])


    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_custom_parser_wrong_type(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = ['invalid json']

        with self.assertRaises(ValueError):
            self.client.set_json_decoder('')


    ##########################################################################
    # Login

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_login_single_account_success(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['streamerSubscriptionKeys']['keys'].clear()
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(1))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, 200)
        socket = AsyncMagicMock()
        ws_connect.return_value = socket

        socket.recv.side_effect = [json.dumps(self.success_response(
            0, 'ADMIN', 'LOGIN'))]

        await self.client.login()

        socket.send.assert_awaited_once()
        request = self.request_from_socket_mock(socket)
        creds = urllib.parse.parse_qs(request['parameters']['credential'])

        self.assertEqual(creds['userid'], ['1001'])
        self.assertEqual(creds['token'], ['streamerInfo-token'])
        self.assertEqual(creds['company'], ['accounts-company-1001'])
        self.assertEqual(
            creds['cddomain'],
            ['accounts-accountCdDomainId-1001'])
        self.assertEqual(creds['usergroup'], ['streamerInfo-userGroup'])
        self.assertEqual(creds['accesslevel'], ['streamerInfo-accessLevel'])
        self.assertEqual(creds['authorized'], ['Y'])
        self.assertEqual(creds['timestamp'], ['1590113568000'])
        self.assertEqual(creds['appid'], ['streamerInfo-appId'])
        self.assertEqual(creds['acl'], ['streamerInfo-acl'])

        self.assertEqual(request['parameters']['token'], 'streamerInfo-token')
        self.assertEqual(request['parameters']['version'], '1.0')

        self.assertEqual(request['requestid'], '0')
        self.assertEqual(request['service'], 'ADMIN')
        self.assertEqual(request['command'], 'LOGIN')


    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_login_single_account_success_async(self, ws_connect):
        '''
        Same as test_login_single_account_success except the underlying client 
        is asynchronous and returns a coroutine for get_user_principals.
        '''
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['streamerSubscriptionKeys']['keys'].clear()
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(1))

        async def get_user_principals(*args, **kwargs):
            return MockResponse(principals, 200)

        self.http_client.get_user_principals = get_user_principals
        socket = AsyncMagicMock()
        ws_connect.return_value = socket

        socket.recv.side_effect = [json.dumps(self.success_response(
            0, 'ADMIN', 'LOGIN'))]

        await self.client.login()

        socket.send.assert_awaited_once()
        request = self.request_from_socket_mock(socket)
        creds = urllib.parse.parse_qs(request['parameters']['credential'])

        self.assertEqual(creds['userid'], ['1001'])
        self.assertEqual(creds['token'], ['streamerInfo-token'])
        self.assertEqual(creds['company'], ['accounts-company-1001'])
        self.assertEqual(
            creds['cddomain'],
            ['accounts-accountCdDomainId-1001'])
        self.assertEqual(creds['usergroup'], ['streamerInfo-userGroup'])
        self.assertEqual(creds['accesslevel'], ['streamerInfo-accessLevel'])
        self.assertEqual(creds['authorized'], ['Y'])
        self.assertEqual(creds['timestamp'], ['1590113568000'])
        self.assertEqual(creds['appid'], ['streamerInfo-appId'])
        self.assertEqual(creds['acl'], ['streamerInfo-acl'])

        self.assertEqual(request['parameters']['token'], 'streamerInfo-token')
        self.assertEqual(request['parameters']['version'], '1.0')

        self.assertEqual(request['requestid'], '0')
        self.assertEqual(request['service'], 'ADMIN')
        self.assertEqual(request['command'], 'LOGIN')

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_login_multiple_accounts_require_account_id(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['accounts'].append(self.account(2))
        principals['streamerSubscriptionKeys']['keys'].clear()
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(1))
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(2))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, 200)

        with self.assertRaisesRegex(ValueError,
                                    '.*initialized with unspecified account_id.*'):
            await self.client.login()
        ws_connect.assert_not_called()

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_login_multiple_accounts_with_account_id(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['accounts'].append(self.account(2))
        principals['streamerSubscriptionKeys']['keys'].clear()
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(1))
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(2))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, 200)
        socket = AsyncMagicMock()
        ws_connect.return_value = socket

        socket.recv.side_effect = [json.dumps(self.success_response(
            0, 'ADMIN', 'LOGIN'))]

        self.client = StreamClient(self.http_client, account_id=1002)
        await self.client.login()

        socket.send.assert_awaited_once()
        request = self.request_from_socket_mock(socket)
        creds = urllib.parse.parse_qs(request['parameters']['credential'])

        self.assertEqual(creds['userid'], ['1002'])
        self.assertEqual(creds['token'], ['streamerInfo-token'])
        self.assertEqual(creds['company'], ['accounts-company-1002'])
        self.assertEqual(
            creds['cddomain'],
            ['accounts-accountCdDomainId-1002'])
        self.assertEqual(creds['usergroup'], ['streamerInfo-userGroup'])
        self.assertEqual(creds['accesslevel'], ['streamerInfo-accessLevel'])
        self.assertEqual(creds['authorized'], ['Y'])
        self.assertEqual(creds['timestamp'], ['1590113568000'])
        self.assertEqual(creds['appid'], ['streamerInfo-appId'])
        self.assertEqual(creds['acl'], ['streamerInfo-acl'])

        self.assertEqual(request['parameters']['token'], 'streamerInfo-token')
        self.assertEqual(request['parameters']['version'], '1.0')

        self.assertEqual(request['requestid'], '0')
        self.assertEqual(request['service'], 'ADMIN')
        self.assertEqual(request['command'], 'LOGIN')

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_login_unrecognized_account_id(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['accounts'].append(self.account(2))
        principals['streamerSubscriptionKeys']['keys'].clear()
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(1))
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(2))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, 200)

        self.client = StreamClient(self.http_client, account_id=999999)

        with self.assertRaisesRegex(ValueError,
                                    '.*no account found with account_id 999999.*'):
            await self.client.login()
        ws_connect.assert_not_called()

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_login_bad_response(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['streamerSubscriptionKeys']['keys'].clear()
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(1))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, 200)
        socket = AsyncMagicMock()
        ws_connect.return_value = socket

        response = self.success_response(0, 'ADMIN', 'LOGIN')
        response['response'][0]['content']['code'] = 21
        response['response'][0]['content']['msg'] = 'failed for some reason'
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.login()

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_login_no_ssl_context(self, ws_connect):
        self.client = StreamClient(self.http_client)

        self.http_client.get_user_principals.return_value = MockResponse(
            account_principals(), 200)
        socket = AsyncMagicMock()
        ws_connect.return_value = socket

        socket.recv.side_effect = [json.dumps(self.success_response(
            0, 'ADMIN', 'LOGIN'))]

        await self.client.login()

        ws_connect.assert_awaited_once_with(ANY)


    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_login_ssl_context(self, ws_connect):
        self.client = StreamClient(self.http_client, ssl_context='ssl_context')

        self.http_client.get_user_principals.return_value = MockResponse(
            account_principals(), 200)
        socket = AsyncMagicMock()
        ws_connect.return_value = socket

        socket.recv.side_effect = [json.dumps(self.success_response(
            0, 'ADMIN', 'LOGIN'))]

        await self.client.login()

        ws_connect.assert_awaited_once_with(ANY, ssl='ssl_context')


    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_login_unexpected_request_id(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['streamerSubscriptionKeys']['keys'].clear()
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(1))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, 200)
        socket = AsyncMagicMock()
        ws_connect.return_value = socket

        response = self.success_response(0, 'ADMIN', 'LOGIN')
        response['response'][0]['requestid'] = 9999
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaisesRegex(tda.streaming.UnexpectedResponse,
                                    'unexpected requestid: 9999'):
            await self.client.login()

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_login_unexpected_service(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['streamerSubscriptionKeys']['keys'].clear()
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(1))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, 200)
        socket = AsyncMagicMock()
        ws_connect.return_value = socket

        response = self.success_response(0, 'NOT_ADMIN', 'LOGIN')
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaisesRegex(tda.streaming.UnexpectedResponse,
                                    'unexpected service: NOT_ADMIN'):
            await self.client.login()

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_login_unexpected_command(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['streamerSubscriptionKeys']['keys'].clear()
        principals['streamerSubscriptionKeys']['keys'].append(
            self.stream_key(1))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, 200)
        socket = AsyncMagicMock()
        ws_connect.return_value = socket

        response = self.success_response(0, 'ADMIN', 'NOT_LOGIN')
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaisesRegex(tda.streaming.UnexpectedResponse,
                                    'unexpected command: NOT_LOGIN'):
            await self.client.login()

    ##########################################################################
    # QOS

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_qos_success(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'ADMIN', 'QOS'))]

        await self.client.quality_of_service(StreamClient.QOSLevel.EXPRESS)
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'ADMIN',
            'command': 'QOS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'qoslevel': '0'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_qos_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'ADMIN', 'QOS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.quality_of_service(StreamClient.QOSLevel.EXPRESS)
        socket.recv.assert_awaited_once()

    ##########################################################################
    # ACCT_ACTIVITY

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_account_activity_subs_success(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'ACCT_ACTIVITY', 'SUBS'))]

        await self.client.account_activity_sub()
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'ACCT_ACTIVITY',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'streamerSubscriptionKeys-keys-key',
                'fields': '0,1,2,3'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_account_activity_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'ACCT_ACTIVITY', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.account_activity_sub()

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_account_activity_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [
                {
                    'service': 'ACCT_ACTIVITY',
                    'timestamp': 1591754497594,
                    'command': 'SUBS',
                    'content': [
                        {
                            'seq': 1,
                            'key': 'streamerSubscriptionKeys-keys-key',
                            '1': '1001',
                            '2': 'OrderEntryRequest',
                            '3': ''
                        }
                    ]
                }
            ]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'ACCT_ACTIVITY', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.account_activity_sub()

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_account_activity_handler(handler)
        self.client.add_account_activity_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'ACCT_ACTIVITY',
            'timestamp': 1591754497594,
            'command': 'SUBS',
            'content': [
                {
                    'seq': 1,
                    'key': 'streamerSubscriptionKeys-keys-key',
                    'ACCOUNT': '1001',
                    'MESSAGE_TYPE': 'OrderEntryRequest',
                    'MESSAGE_DATA': ''
                }
            ]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    ##########################################################################
    # CHART_EQUITY

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_equity_subs_and_add_success(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'CHART_EQUITY', 'SUBS'))]

        await self.client.chart_equity_subs(['GOOG', 'MSFT'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'CHART_EQUITY',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': '0,1,2,3,4,5,6,7,8'
            }
        })

        socket.reset_mock()

        socket.recv.side_effect = [json.dumps(self.success_response(
            2, 'CHART_EQUITY', 'ADD'))]

        await self.client.chart_equity_add(['INTC'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'CHART_EQUITY',
            'command': 'ADD',
            'requestid': '2',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'INTC',
                'fields': '0,1,2,3,4,5,6,7,8'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_equity_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'CHART_EQUITY', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.chart_equity_subs(['GOOG', 'MSFT'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_equity_add_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response_subs = self.success_response(1, 'CHART_EQUITY', 'SUBS')

        response_add = self.success_response(2, 'CHART_EQUITY', 'ADD')
        response_add['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [
            json.dumps(response_subs),
            json.dumps(response_add)]

        await self.client.chart_equity_subs(['GOOG', 'MSFT'])

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.chart_equity_add(['INTC'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_equity_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [
                {
                    'service': 'CHART_EQUITY',
                    'timestamp': 1590597641293,
                    'command': 'SUBS',
                    'content': [
                        {
                            'seq': 985,
                            'key': 'MSFT',
                            '1': 179.445,
                            '2': 179.57,
                            '3': 179.4299,
                            '4': 179.52,
                            '5': 53742.0,
                            '6': 339,
                            '7': 1590597540000,
                            '8': 18409
                        },
                        {
                            'seq': 654,
                            'key': 'GOOG',
                            '1': 1408.8,
                            '2': 1408.8,
                            '3': 1408.1479,
                            '4': 1408.1479,
                            '5': 500.0,
                            '6': 339,
                            '7': 1590597540000,
                            '8': 18409
                        }
                    ]
                }
            ]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_EQUITY', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.chart_equity_subs(['GOOG', 'MSFT'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_chart_equity_handler(handler)
        self.client.add_chart_equity_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'CHART_EQUITY',
            'timestamp': 1590597641293,
            'command': 'SUBS',
            'content': [
                {
                    'seq': 985,
                    'key': 'MSFT',
                    'OPEN_PRICE': 179.445,
                    'HIGH_PRICE': 179.57,
                    'LOW_PRICE': 179.4299,
                    'CLOSE_PRICE': 179.52,
                    'VOLUME': 53742.0,
                    'SEQUENCE': 339,
                    'CHART_TIME': 1590597540000,
                    'CHART_DAY': 18409
                },
                {
                    'seq': 654,
                    'key': 'GOOG',
                    'OPEN_PRICE': 1408.8,
                    'HIGH_PRICE': 1408.8,
                    'LOW_PRICE': 1408.1479,
                    'CLOSE_PRICE': 1408.1479,
                    'VOLUME': 500.0,
                    'SEQUENCE': 339,
                    'CHART_TIME': 1590597540000,
                    'CHART_DAY': 18409
                }
            ]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_equity_subs_lowercase_symbols_success(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'CHART_EQUITY', 'SUBS'))]

        await self.client.chart_equity_subs(['goog', 'nflX'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'CHART_EQUITY',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,NFLX',
                'fields': '0,1,2,3,4,5,6,7,8'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_equity_add_lowercase_symbols_success(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)
        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'CHART_EQUITY', 'ADD'))]

        await self.client.chart_equity_add(['nvda'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'CHART_EQUITY',
            'command': 'ADD',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'NVDA',
                'fields': '0,1,2,3,4,5,6,7,8'
            }
        })

    ##########################################################################
    # CHART_FUTURES

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_futures_subs_and_add_success(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'CHART_FUTURES', 'SUBS'))]

        await self.client.chart_futures_subs(['/ES', '/CL'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'CHART_FUTURES',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': '0,1,2,3,4,5,6'
            }
        })

        socket.reset_mock()

        socket.recv.side_effect = [json.dumps(self.success_response(
            2, 'CHART_FUTURES', 'ADD'))]

        await self.client.chart_futures_add(['/ZC'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'CHART_FUTURES',
            'command': 'ADD',
            'requestid': '2',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ZC',
                'fields': '0,1,2,3,4,5,6'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_futures_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'CHART_FUTURES', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.chart_futures_subs(['/ES', '/CL'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_futures_add_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response_subs = self.success_response(1, 'CHART_FUTURES', 'SUBS')

        response_add = self.success_response(2, 'CHART_FUTURES', 'ADD')
        response_add['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [
            json.dumps(response_subs),
            json.dumps(response_add)]

        await self.client.chart_futures_subs(['/ES', '/CL'])

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.chart_futures_add(['/ZC'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_futures_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [
                {
                    'service': 'CHART_FUTURES',
                    'timestamp': 1590597913941,
                    'command': 'SUBS',
                    'content': [
                        {
                            'seq': 0,
                            'key': '/ES',
                            '1': 1590597840000,
                            '2': 2996.25,
                            '3': 2997.25,
                            '4': 2995.25,
                            '5': 2997.25,
                            '6': 1501.0
                        },
                        {
                            'seq': 0,
                            'key': '/CL',
                            '1': 1590597840000,
                            '2': 33.34,
                            '3': 33.35,
                            '4': 33.32,
                            '5': 33.35,
                            '6': 186.0
                        }
                    ]
                }
            ]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_FUTURES', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.chart_futures_subs(['/ES', '/CL'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_chart_futures_handler(handler)
        self.client.add_chart_futures_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'CHART_FUTURES',
            'timestamp': 1590597913941,
            'command': 'SUBS',
            'content': [{
                'seq': 0,
                'key': '/ES',
                'CHART_TIME': 1590597840000,
                'OPEN_PRICE': 2996.25,
                'HIGH_PRICE': 2997.25,
                'LOW_PRICE': 2995.25,
                'CLOSE_PRICE': 2997.25,
                'VOLUME': 1501.0
            }, {
                'seq': 0,
                'key': '/CL',
                'CHART_TIME': 1590597840000,
                'OPEN_PRICE': 33.34,
                'HIGH_PRICE': 33.35,
                'LOW_PRICE': 33.32,
                'CLOSE_PRICE': 33.35,
                'VOLUME': 186.0
            }]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_futures_subs_lowercase_symbols_success(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'CHART_FUTURES', 'SUBS'))]

        await self.client.chart_futures_subs(['/es', '/cl'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'CHART_FUTURES',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': '0,1,2,3,4,5,6'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_chart_futures_add_lowercase_symbols_success(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'CHART_FUTURES', 'ADD'))]

        await self.client.chart_futures_add(['/zc'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'CHART_FUTURES',
            'command': 'ADD',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ZC',
                'fields': '0,1,2,3,4,5,6'
            }
        })
    

    ##########################################################################
    # QUOTE

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_equity_subs_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'QUOTE', 'SUBS'))]

        await self.client.level_one_equity_subs(['GOOG', 'MSFT'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'QUOTE',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,' +
                           '20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,' +
                           '36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,' +
                           '52')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_equity_subs_success_some_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'QUOTE', 'SUBS'))]

        await self.client.level_one_equity_subs(['GOOG', 'MSFT'], fields=[
            StreamClient.LevelOneEquityFields.SYMBOL,
            StreamClient.LevelOneEquityFields.BID_PRICE,
            StreamClient.LevelOneEquityFields.ASK_PRICE,
            StreamClient.LevelOneEquityFields.QUOTE_TIME,
        ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'QUOTE',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': '0,1,2,11'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_equity_subs_success_some_fields_no_symbol(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'QUOTE', 'SUBS'))]

        await self.client.level_one_equity_subs(['GOOG', 'MSFT'], fields=[
            StreamClient.LevelOneEquityFields.BID_PRICE,
            StreamClient.LevelOneEquityFields.ASK_PRICE,
            StreamClient.LevelOneEquityFields.QUOTE_TIME,
        ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'QUOTE',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': '0,1,2,11'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_equity_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'QUOTE', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.level_one_equity_subs(['GOOG', 'MSFT'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_quote_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [{
                'service': 'QUOTE',
                'command': 'SUBS',
                'timestamp': 1590186642440,
                'content': [{
                    'key': 'GOOG',
                    'delayed': False,
                    'assetMainType': 'EQUITY',
                    'cusip': '02079K107',
                    '1': 1404.92,
                    '2': 1412.99,
                    '3': 1411.89,
                    '4': 1,
                    '5': 2,
                    '6': 'P',
                    '7': 'K',
                    '8': 1309408,
                    '9': 2,
                    '10': 71966,
                    '11': 71970,
                    '12': 1412.76,
                    '13': 1391.83,
                    '14': ' ',
                    '15': 1410.42,
                    '16': 'q',
                    '17': True,
                    '18': True,
                    '19': 1412.991,
                    '20': 1411.891,
                    '21': 1309409,
                    '22': 18404,
                    '23': 18404,
                    '24': 0.0389,
                    '25': 'Alphabet Inc. - Class C Capital Stock',
                    '26': 'P',
                    '27': 4,
                    '28': 1396.71,
                    '29': 1.47,
                    '30': 1532.106,
                    '31': 1013.536,
                    '32': 28.07,
                    '33': 6.52,
                    '34': 5.51,
                    '35': 122.0,
                    '36': 123.0,
                    '37': 123123.0,
                    '38': 123214.0,
                    '39': 'NASD',
                    '40': ' ',
                    '41': True,
                    '42': True,
                    '43': 1410.42,
                    '44': 699,
                    '45': 57600,
                    '46': 18404,
                    '47': 1.48,
                    '48': 'Normal',
                    '49': 1410.42,
                    '50': 1590191970734,
                    '51': 1590191966446,
                    '52': 1590177600617
                }, {
                    'key': 'MSFT',
                    'delayed': False,
                    'assetMainType': 'EQUITY',
                    'cusip': '594918104',
                    '1': 183.65,
                    '2': 183.7,
                    '3': 183.65,
                    '4': 3,
                    '5': 10,
                    '6': 'P',
                    '7': 'P',
                    '8': 20826898,
                    '9': 200,
                    '10': 71988,
                    '11': 71988,
                    '12': 184.46,
                    '13': 182.54,
                    '14': ' ',
                    '15': 183.51,
                    '16': 'q',
                    '17': True,
                    '18': True,
                    '19': 182.65,
                    '20': 182.7,
                    '21': 20826899,
                    '22': 18404,
                    '23': 18404,
                    '24': 0.0126,
                    '25': 'Microsoft Corporation - Common Stock',
                    '26': 'K',
                    '27': 4,
                    '28': 183.19,
                    '29': 0.14,
                    '30': 190.7,
                    '31': 119.01,
                    '32': 32.3555,
                    '33': 2.04,
                    '34': 1.11,
                    '35': 122.0,
                    '36': 123.0,
                    '37': 123123.0,
                    '38': 123214.0,
                    '39': 'NASD',
                    '40': '2020-05-20 00:00:00.000',
                    '41': True,
                    '42': True,
                    '43': 183.51,
                    '44': 16890,
                    '45': 57600,
                    '46': 18404,
                    '48': 'Normal',
                    '47': 1.49,
                    '49': 183.51,
                    '50': 1590191988960,
                    '51': 1590191988957,
                    '52': 1590177600516
                }]
            }]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'QUOTE', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.level_one_equity_subs(['GOOG', 'MSFT'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_level_one_equity_handler(handler)
        self.client.add_level_one_equity_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'QUOTE',
            'command': 'SUBS',
            'timestamp': 1590186642440,
            'content': [{
                'key': 'GOOG',
                'delayed': False,
                'assetMainType': 'EQUITY',
                'cusip': '02079K107',
                'BID_PRICE': 1404.92,
                'ASK_PRICE': 1412.99,
                'LAST_PRICE': 1411.89,
                'BID_SIZE': 1,
                'ASK_SIZE': 2,
                'ASK_ID': 'P',
                'BID_ID': 'K',
                'TOTAL_VOLUME': 1309408,
                'LAST_SIZE': 2,
                'TRADE_TIME': 71966,
                'QUOTE_TIME': 71970,
                'HIGH_PRICE': 1412.76,
                'LOW_PRICE': 1391.83,
                'BID_TICK': ' ',
                'CLOSE_PRICE': 1410.42,
                'EXCHANGE_ID': 'q',
                'MARGINABLE': True,
                'SHORTABLE': True,
                'ISLAND_BID_DEPRECATED': 1412.991,
                'ISLAND_ASK_DEPRECATED': 1411.891,
                'ISLAND_VOLUME_DEPRECATED': 1309409,
                'QUOTE_DAY': 18404,
                'TRADE_DAY': 18404,
                'VOLATILITY': 0.0389,
                'DESCRIPTION': 'Alphabet Inc. - Class C Capital Stock',
                'LAST_ID': 'P',
                'DIGITS': 4,
                'OPEN_PRICE': 1396.71,
                'NET_CHANGE': 1.47,
                'HIGH_52_WEEK': 1532.106,
                'LOW_52_WEEK': 1013.536,
                'PE_RATIO': 28.07,
                'DIVIDEND_AMOUNT': 6.52,
                'DIVIDEND_YIELD': 5.51,
                'ISLAND_BID_SIZE_DEPRECATED': 122.0,
                'ISLAND_ASK_SIZE_DEPRECATED': 123.0,
                'NAV': 123123.0,
                'FUND_PRICE': 123214.0,
                'EXCHANGE_NAME': 'NASD',
                'DIVIDEND_DATE': ' ',
                'IS_REGULAR_MARKET_QUOTE': True,
                'IS_REGULAR_MARKET_TRADE': True,
                'REGULAR_MARKET_LAST_PRICE': 1410.42,
                'REGULAR_MARKET_LAST_SIZE': 699,
                'REGULAR_MARKET_TRADE_TIME': 57600,
                'REGULAR_MARKET_TRADE_DAY': 18404,
                'REGULAR_MARKET_NET_CHANGE': 1.48,
                'SECURITY_STATUS': 'Normal',
                'MARK': 1410.42,
                'QUOTE_TIME_IN_LONG': 1590191970734,
                'TRADE_TIME_IN_LONG': 1590191966446,
                'REGULAR_MARKET_TRADE_TIME_IN_LONG': 1590177600617
            }, {
                'key': 'MSFT',
                'delayed': False,
                'assetMainType': 'EQUITY',
                'cusip': '594918104',
                'BID_PRICE': 183.65,
                'ASK_PRICE': 183.7,
                'LAST_PRICE': 183.65,
                'BID_SIZE': 3,
                'ASK_SIZE': 10,
                'ASK_ID': 'P',
                'BID_ID': 'P',
                'TOTAL_VOLUME': 20826898,
                'LAST_SIZE': 200,
                'TRADE_TIME': 71988,
                'QUOTE_TIME': 71988,
                'HIGH_PRICE': 184.46,
                'LOW_PRICE': 182.54,
                'BID_TICK': ' ',
                'CLOSE_PRICE': 183.51,
                'EXCHANGE_ID': 'q',
                'MARGINABLE': True,
                'SHORTABLE': True,
                'ISLAND_BID_DEPRECATED': 182.65,
                'ISLAND_ASK_DEPRECATED': 182.7,
                'ISLAND_VOLUME_DEPRECATED': 20826899,
                'QUOTE_DAY': 18404,
                'TRADE_DAY': 18404,
                'VOLATILITY': 0.0126,
                'DESCRIPTION': 'Microsoft Corporation - Common Stock',
                'LAST_ID': 'K',
                'DIGITS': 4,
                'OPEN_PRICE': 183.19,
                'NET_CHANGE': 0.14,
                'HIGH_52_WEEK': 190.7,
                'LOW_52_WEEK': 119.01,
                'PE_RATIO': 32.3555,
                'DIVIDEND_AMOUNT': 2.04,
                'DIVIDEND_YIELD': 1.11,
                'ISLAND_BID_SIZE_DEPRECATED': 122.0,
                'ISLAND_ASK_SIZE_DEPRECATED': 123.0,
                'NAV': 123123.0,
                'FUND_PRICE': 123214.0,
                'EXCHANGE_NAME': 'NASD',
                'DIVIDEND_DATE': '2020-05-20 00:00:00.000',
                'IS_REGULAR_MARKET_QUOTE': True,
                'IS_REGULAR_MARKET_TRADE': True,
                'REGULAR_MARKET_LAST_PRICE': 183.51,
                'REGULAR_MARKET_LAST_SIZE': 16890,
                'REGULAR_MARKET_TRADE_TIME': 57600,
                'REGULAR_MARKET_TRADE_DAY': 18404,
                'SECURITY_STATUS': 'Normal',
                'REGULAR_MARKET_NET_CHANGE': 1.49,
                'MARK': 183.51,
                'QUOTE_TIME_IN_LONG': 1590191988960,
                'TRADE_TIME_IN_LONG': 1590191988957,
                'REGULAR_MARKET_TRADE_TIME_IN_LONG': 1590177600516
            }]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_equity_subs_lowercase_symbols_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'QUOTE', 'SUBS'))]

        await self.client.level_one_equity_subs(['goog', 'msft'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'QUOTE',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,' +
                           '20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,' +
                           '36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,' +
                           '52')
            }
        })

    ##########################################################################
    # OPTION

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_option_subs_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'OPTION', 'SUBS'))]

        await self.client.level_one_option_subs(
            ['GOOG_052920C620', 'MSFT_052920C145'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'OPTION',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG_052920C620,MSFT_052920C145',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,' +
                           '20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,' +
                           '36,37,38,39,40,41')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_option_subs_success_some_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'OPTION', 'SUBS'))]

        await self.client.level_one_option_subs(
            ['GOOG_052920C620', 'MSFT_052920C145'], fields=[
                StreamClient.LevelOneOptionFields.SYMBOL,
                StreamClient.LevelOneOptionFields.BID_PRICE,
                StreamClient.LevelOneOptionFields.ASK_PRICE,
                StreamClient.LevelOneOptionFields.VOLATILITY,
            ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'OPTION',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG_052920C620,MSFT_052920C145',
                'fields': '0,2,3,10'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_option_subs_success_some_fields_no_symbol(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'OPTION', 'SUBS'))]

        await self.client.level_one_option_subs(
            ['GOOG_052920C620', 'MSFT_052920C145'], fields=[
                StreamClient.LevelOneOptionFields.BID_PRICE,
                StreamClient.LevelOneOptionFields.ASK_PRICE,
                StreamClient.LevelOneOptionFields.VOLATILITY,
            ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'OPTION',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG_052920C620,MSFT_052920C145',
                'fields': '0,2,3,10'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_option_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'OPTION', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.level_one_option_subs(
                ['GOOG_052920C620', 'MSFT_052920C145'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_option_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [{
                'service': 'OPTION',
                'timestamp': 1590244265891,
                'command': 'SUBS',
                'content': [{
                    'key': 'MSFT_052920C145',
                    'delayed': False,
                    'assetMainType': 'OPTION',
                    'cusip': '0MSFT.ET00145000',
                    '1': 'MSFT May 29 2020 145 Call (Weekly)',
                    '2': 38.05,
                    '3': 39.05,
                    '4': 38.85,
                    '5': 38.85,
                    '6': 38.85,
                    '7': 38.581,
                    '8': 2,
                    '9': 7,
                    '10': 5,
                    '11': 57599,
                    '12': 53017,
                    '13': 38.51,
                    '14': 18404,
                    '15': 18404,
                    '16': 2020,
                    '17': 100,
                    '18': 2,
                    '19': 38.85,
                    '20': 6,
                    '21': 116,
                    '22': 1,
                    '23': 0.3185,
                    '24': 145,
                    '25': 'C',
                    '26': 'MSFT',
                    '27': 5,
                    '29': 0.34,
                    '30': 29,
                    '31': 6,
                    '32': 1,
                    '33': 0,
                    '34': 0,
                    '35': 0.1882,
                    '37': 'Normal',
                    '38': 38.675,
                    '39': 183.51,
                    '40': 'S',
                    '41': 38.55
                }, {
                    'key': 'GOOG_052920C620',
                    'delayed': False,
                    'assetMainType': 'OPTION',
                    'cusip': '0GOOG.ET00620000',
                    '1': 'GOOG May 29 2020 620 Call (Weekly)',
                    '2': 785.2,
                    '3': 794,
                    '7': 790.42,
                    '10': 238.2373,
                    '11': 57594,
                    '12': 68400,
                    '13': 790.42,
                    '14': 18404,
                    '16': 2020,
                    '17': 100,
                    '18': 2,
                    '20': 1,
                    '21': 6,
                    '24': 620,
                    '25': 'C',
                    '26': 'GOOG',
                    '27': 5,
                    '29': -0.82,
                    '30': 29,
                    '31': 6,
                    '32': 0.996,
                    '33': 0,
                    '34': -0.3931,
                    '35': 0.023,
                    '36': 0.1176,
                    '37': 'Normal',
                    '38': 789.6,
                    '39': 1410.42,
                    '40': 'S',
                    '41': 789.6
                }]
            }
            ]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'OPTION', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.level_one_option_subs(
            ['GOOG_052920C620', 'MSFT_052920C145'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_level_one_option_handler(handler)
        self.client.add_level_one_option_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'OPTION',
            'timestamp': 1590244265891,
            'command': 'SUBS',
            'content': [{
                'key': 'MSFT_052920C145',
                'delayed': False,
                'assetMainType': 'OPTION',
                'cusip': '0MSFT.ET00145000',
                'DESCRIPTION': 'MSFT May 29 2020 145 Call (Weekly)',
                'BID_PRICE': 38.05,
                'ASK_PRICE': 39.05,
                'LAST_PRICE': 38.85,
                'HIGH_PRICE': 38.85,
                'LOW_PRICE': 38.85,
                'CLOSE_PRICE': 38.581,
                'TOTAL_VOLUME': 2,
                'OPEN_INTEREST': 7,
                'VOLATILITY': 5,
                'QUOTE_TIME': 57599,
                'TRADE_TIME': 53017,
                'MONEY_INTRINSIC_VALUE': 38.51,
                'QUOTE_DAY': 18404,
                'TRADE_DAY': 18404,
                'EXPIRATION_YEAR': 2020,
                'MULTIPLIER': 100,
                'DIGITS': 2,
                'OPEN_PRICE': 38.85,
                'BID_SIZE': 6,
                'ASK_SIZE': 116,
                'LAST_SIZE': 1,
                'NET_CHANGE': 0.3185,
                'STRIKE_PRICE': 145,
                'CONTRACT_TYPE': 'C',
                'UNDERLYING': 'MSFT',
                'EXPIRATION_MONTH': 5,
                'TIME_VALUE': 0.34,
                'EXPIRATION_DAY': 29,
                'DAYS_TO_EXPIRATION': 6,
                'DELTA': 1,
                'GAMMA': 0,
                'THETA': 0,
                'VEGA': 0.1882,
                'SECURITY_STATUS': 'Normal',
                'THEORETICAL_OPTION_VALUE': 38.675,
                'UNDERLYING_PRICE': 183.51,
                'UV_EXPIRATION_TYPE': 'S',
                'MARK': 38.55
            }, {
                'key': 'GOOG_052920C620',
                'delayed': False,
                'assetMainType': 'OPTION',
                'cusip': '0GOOG.ET00620000',
                'DESCRIPTION': 'GOOG May 29 2020 620 Call (Weekly)',
                'BID_PRICE': 785.2,
                'ASK_PRICE': 794,
                'CLOSE_PRICE': 790.42,
                'VOLATILITY': 238.2373,
                'QUOTE_TIME': 57594,
                'TRADE_TIME': 68400,
                'MONEY_INTRINSIC_VALUE': 790.42,
                'QUOTE_DAY': 18404,
                'EXPIRATION_YEAR': 2020,
                'MULTIPLIER': 100,
                'DIGITS': 2,
                'BID_SIZE': 1,
                'ASK_SIZE': 6,
                'STRIKE_PRICE': 620,
                'CONTRACT_TYPE': 'C',
                'UNDERLYING': 'GOOG',
                'EXPIRATION_MONTH': 5,
                'TIME_VALUE': -0.82,
                'EXPIRATION_DAY': 29,
                'DAYS_TO_EXPIRATION': 6,
                'DELTA': 0.996,
                'GAMMA': 0,
                'THETA': -0.3931,
                'VEGA': 0.023,
                'RHO': 0.1176,
                'SECURITY_STATUS': 'Normal',
                'THEORETICAL_OPTION_VALUE': 789.6,
                'UNDERLYING_PRICE': 1410.42,
                'UV_EXPIRATION_TYPE': 'S',
                'MARK': 789.6
            }]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_option_subs_lowercase_symbols_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'OPTION', 'SUBS'))]

        await self.client.level_one_option_subs(
            ['nflx_052920C620', 'fb_052920C145'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'OPTION',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'NFLX_052920C620,FB_052920C145',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,' +
                           '20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,' +
                           '36,37,38,39,40,41')
            }
        })

    ##########################################################################
    # LEVELONE_FUTURES

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_subs_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FUTURES', 'SUBS'))]

        await self.client.level_one_futures_subs(['/ES', '/CL'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FUTURES',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,' +
                           '20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_subs_success_some_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FUTURES', 'SUBS'))]

        await self.client.level_one_futures_subs(['/ES', '/CL'], fields=[
            StreamClient.LevelOneFuturesFields.SYMBOL,
            StreamClient.LevelOneFuturesFields.BID_PRICE,
            StreamClient.LevelOneFuturesFields.ASK_PRICE,
            StreamClient.LevelOneFuturesFields.FUTURE_PRICE_FORMAT,
        ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FUTURES',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': '0,1,2,28'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_subs_success_some_fields_no_symbol(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FUTURES', 'SUBS'))]

        await self.client.level_one_futures_subs(['/ES', '/CL'], fields=[
            StreamClient.LevelOneFuturesFields.BID_PRICE,
            StreamClient.LevelOneFuturesFields.ASK_PRICE,
            StreamClient.LevelOneFuturesFields.FUTURE_PRICE_FORMAT,
        ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FUTURES',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': '0,1,2,28'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'LEVELONE_FUTURES', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.level_one_futures_subs(['/ES', '/CL'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [{
                'service': 'LEVELONE_FUTURES',
                'timestamp': 1590598762176,
                'command': 'SUBS',
                'content': [{
                    'key': '/ES',
                    'delayed': False,
                    '1': 2998.75,
                    '2': 2999,
                    '3': 2998.75,
                    '4': 15,
                    '5': 47,
                    '6': '?',
                    '7': '?',
                    '8': 1489587,
                    '9': 6,
                    '10': 1590598761934,
                    '11': 1590598761921,
                    '12': 3035,
                    '13': 2965.5,
                    '14': 2994.5,
                    '15': 'E',
                    '16': 'E-mini S&P 500 Index Futures,Jun-2020,ETH',
                    '17': '?',
                    '18': 2994,
                    '19': 4.25,
                    '20': 0.0014,
                    '21': 'XCME',
                    '22': 'Unknown',
                    '23': 3121588,
                    '24': 2999.25,
                    '25': 0.25,
                    '26': 12.5,
                    '27': '/ES',
                    '28': 'D,D',
                    '29': ('GLBX(de=1640;0=-1700151515301600;' +
                           '1=r-17001515r15301600d-15551640;' +
                           '7=d-16401555)'),
                    '30': True,
                    '31': 50,
                    '32': True,
                    '33': 2994.5,
                    '34': '/ESM20',
                    '35': 1592539200000
                }, {
                    'key': '/CL',
                    'delayed': False,
                    '1': 33.33,
                    '2': 33.34,
                    '3': 33.34,
                    '4': 13,
                    '5': 3,
                    '6': '?',
                    '7': '?',
                    '8': 325014,
                    '9': 2,
                    '10': 1590598761786,
                    '11': 1590598761603,
                    '12': 34.32,
                    '13': 32.18,
                    '14': 34.35,
                    '15': 'E',
                    '16': 'Light Sweet Crude Oil Futures,Jul-2020,ETH',
                    '17': '?',
                    '18': 34.14,
                    '19': -1.01,
                    '20': -0.0294,
                    '21': 'XNYM',
                    '22': 'Unknown',
                    '23': 270931,
                    '24': 33.35,
                    '25': 0.01,
                    '26': 10,
                    '27': '/CL',
                    '28': 'D,D',
                    '29': ('GLBX(de=1640;0=-17001600;' +
                           '1=-17001600d-15551640;7=d-16401555)'),
                    '30': True,
                    '31': 1000,
                    '32': True,
                    '33': 34.35,
                    '34': '/CLN20',
                    '35': 1592798400000
                }]
            }]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'LEVELONE_FUTURES', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.level_one_futures_subs(['/ES', '/CL'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_level_one_futures_handler(handler)
        self.client.add_level_one_futures_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'LEVELONE_FUTURES',
            'timestamp': 1590598762176,
            'command': 'SUBS',
            'content': [{
                'key': '/ES',
                'delayed': False,
                'BID_PRICE': 2998.75,
                'ASK_PRICE': 2999,
                'LAST_PRICE': 2998.75,
                'BID_SIZE': 15,
                'ASK_SIZE': 47,
                'ASK_ID': '?',
                'BID_ID': '?',
                'TOTAL_VOLUME': 1489587,
                'LAST_SIZE': 6,
                'QUOTE_TIME': 1590598761934,
                'TRADE_TIME': 1590598761921,
                'HIGH_PRICE': 3035,
                'LOW_PRICE': 2965.5,
                'CLOSE_PRICE': 2994.5,
                'EXCHANGE_ID': 'E',
                'DESCRIPTION': 'E-mini S&P 500 Index Futures,Jun-2020,ETH',
                'LAST_ID': '?',
                'OPEN_PRICE': 2994,
                'NET_CHANGE': 4.25,
                'FUTURE_PERCENT_CHANGE': 0.0014,
                'EXCHANGE_NAME': 'XCME',
                'SECURITY_STATUS': 'Unknown',
                'OPEN_INTEREST': 3121588,
                'MARK': 2999.25,
                'TICK': 0.25,
                'TICK_AMOUNT': 12.5,
                'PRODUCT': '/ES',
                'FUTURE_PRICE_FORMAT': 'D,D',
                'FUTURE_TRADING_HOURS': (
                    'GLBX(de=1640;0=-1700151515301600;' +
                    '1=r-17001515r15301600d-15551640;' +
                    '7=d-16401555)'),
                'FUTURE_IS_TRADEABLE': True,
                'FUTURE_MULTIPLIER': 50,
                'FUTURE_IS_ACTIVE': True,
                'FUTURE_SETTLEMENT_PRICE': 2994.5,
                'FUTURE_ACTIVE_SYMBOL': '/ESM20',
                'FUTURE_EXPIRATION_DATE': 1592539200000
            }, {
                'key': '/CL',
                'delayed': False,
                'BID_PRICE': 33.33,
                'ASK_PRICE': 33.34,
                'LAST_PRICE': 33.34,
                'BID_SIZE': 13,
                'ASK_SIZE': 3,
                'ASK_ID': '?',
                'BID_ID': '?',
                'TOTAL_VOLUME': 325014,
                'LAST_SIZE': 2,
                'QUOTE_TIME': 1590598761786,
                'TRADE_TIME': 1590598761603,
                'HIGH_PRICE': 34.32,
                'LOW_PRICE': 32.18,
                'CLOSE_PRICE': 34.35,
                'EXCHANGE_ID': 'E',
                'DESCRIPTION': 'Light Sweet Crude Oil Futures,Jul-2020,ETH',
                'LAST_ID': '?',
                'OPEN_PRICE': 34.14,
                'NET_CHANGE': -1.01,
                'FUTURE_PERCENT_CHANGE': -0.0294,
                'EXCHANGE_NAME': 'XNYM',
                'SECURITY_STATUS': 'Unknown',
                'OPEN_INTEREST': 270931,
                'MARK': 33.35,
                'TICK': 0.01,
                'TICK_AMOUNT': 10,
                'PRODUCT': '/CL',
                'FUTURE_PRICE_FORMAT': 'D,D',
                'FUTURE_TRADING_HOURS': (
                    'GLBX(de=1640;0=-17001600;' +
                    '1=-17001600d-15551640;7=d-16401555)'),
                'FUTURE_IS_TRADEABLE': True,
                'FUTURE_MULTIPLIER': 1000,
                'FUTURE_IS_ACTIVE': True,
                'FUTURE_SETTLEMENT_PRICE': 34.35,
                'FUTURE_ACTIVE_SYMBOL': '/CLN20',
                'FUTURE_EXPIRATION_DATE': 1592798400000
            }]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_subs_lowercase_symbols_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FUTURES', 'SUBS'))]

        await self.client.level_one_futures_subs(['/es', '/cl'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FUTURES',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,' +
                           '20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35')
            }
        })

    ##########################################################################
    # LEVELONE_FOREX

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_forex_subs_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FOREX', 'SUBS'))]

        await self.client.level_one_forex_subs(['EUR/USD', 'EUR/GBP'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FOREX',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'EUR/USD,EUR/GBP',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,' +
                           '20,21,22,23,24,25,26,27,28,29')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_forex_subs_success_some_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FOREX', 'SUBS'))]

        await self.client.level_one_forex_subs(['EUR/USD', 'EUR/GBP'], fields=[
            StreamClient.LevelOneForexFields.SYMBOL,
            StreamClient.LevelOneForexFields.HIGH_PRICE,
            StreamClient.LevelOneForexFields.LOW_PRICE,
            StreamClient.LevelOneForexFields.MARKET_MAKER,
        ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FOREX',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'EUR/USD,EUR/GBP',
                'fields': '0,10,11,26'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_forex_subs_success_some_fields_no_symbol(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FOREX', 'SUBS'))]

        await self.client.level_one_forex_subs(['EUR/USD', 'EUR/GBP'], fields=[
            StreamClient.LevelOneForexFields.HIGH_PRICE,
            StreamClient.LevelOneForexFields.LOW_PRICE,
            StreamClient.LevelOneForexFields.MARKET_MAKER,
        ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FOREX',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'EUR/USD,EUR/GBP',
                'fields': '0,10,11,26'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_forex_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'LEVELONE_FOREX', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.level_one_forex_subs(['EUR/USD', 'EUR/GBP'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_forex_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [{
                'service': 'LEVELONE_FOREX',
                'timestamp': 1590599267920,
                'command': 'SUBS',
                'content': [{
                    'key': 'EUR/GBP',
                    'delayed': False,
                    'assetMainType': 'FOREX',
                    '1': 0.8967,
                    '2': 0.8969,
                    '3': 0.8968,
                    '4': 1000000,
                    '5': 1000000,
                    '6': 19000000,
                    '7': 370000,
                    '8': 1590599267658,
                    '9': 1590599267658,
                    '10': 0.8994,
                    '11': 0.8896,
                    '12': 0.894,
                    '13': 'T',
                    '14': 'Euro/GBPound Spot',
                    '15': 0.8901,
                    '16': 0.0028,
                    '18': 'GFT',
                    '19': 2,
                    '20': 'Unknown',
                    '21': 'UNUSED',
                    '22': 'UNUSED',
                    '23': 'UNUSED',
                    '24': 'UNUSED',
                    '25': 'UNUSED',
                    '26': 'UNUSED',
                    '27': 0.8994,
                    '28': 0.8896,
                    '29': 0.8968
                }, {
                    'key': 'EUR/USD',
                    'delayed': False,
                    'assetMainType': 'FOREX',
                    '1': 1.0976,
                    '2': 1.0978,
                    '3': 1.0977,
                    '4': 1000000,
                    '5': 2800000,
                    '6': 633170000,
                    '7': 10000,
                    '8': 1590599267658,
                    '9': 1590599267658,
                    '10': 1.1031,
                    '11': 1.0936,
                    '12': 1.0893,
                    '13': 'T',
                    '14': 'Euro/USDollar Spot',
                    '15': 1.0982,
                    '16': 0.0084,
                    '18': 'GFT',
                    '19': 2,
                    '20': 'Unknown',
                    '21': 'UNUSED',
                    '22': 'UNUSED',
                    '23': 'UNUSED',
                    '24': 'UNUSED',
                    '25': 'UNUSED',
                    '26': 'UNUSED',
                    '27': 1.1031,
                    '28': 1.0936,
                    '29': 1.0977
                }]
            }]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'LEVELONE_FOREX', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.level_one_forex_subs(['EUR/USD', 'EUR/GBP'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_level_one_forex_handler(handler)
        self.client.add_level_one_forex_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'LEVELONE_FOREX',
            'timestamp': 1590599267920,
            'command': 'SUBS',
            'content': [{
                'key': 'EUR/GBP',
                'delayed': False,
                'assetMainType': 'FOREX',
                'BID_PRICE': 0.8967,
                'ASK_PRICE': 0.8969,
                'LAST_PRICE': 0.8968,
                'BID_SIZE': 1000000,
                'ASK_SIZE': 1000000,
                'TOTAL_VOLUME': 19000000,
                'LAST_SIZE': 370000,
                'QUOTE_TIME': 1590599267658,
                'TRADE_TIME': 1590599267658,
                'HIGH_PRICE': 0.8994,
                'LOW_PRICE': 0.8896,
                'CLOSE_PRICE': 0.894,
                'EXCHANGE_ID': 'T',
                'DESCRIPTION': 'Euro/GBPound Spot',
                'OPEN_PRICE': 0.8901,
                'NET_CHANGE': 0.0028,
                'EXCHANGE_NAME': 'GFT',
                'DIGITS': 2,
                'SECURITY_STATUS': 'Unknown',
                'TICK': 'UNUSED',
                'TICK_AMOUNT': 'UNUSED',
                'PRODUCT': 'UNUSED',
                'TRADING_HOURS': 'UNUSED',
                'IS_TRADABLE': 'UNUSED',
                'MARKET_MAKER': 'UNUSED',
                'HIGH_52_WEEK': 0.8994,
                'LOW_52_WEEK': 0.8896,
                'MARK': 0.8968
            }, {
                'key': 'EUR/USD',
                'delayed': False,
                'assetMainType': 'FOREX',
                'BID_PRICE': 1.0976,
                'ASK_PRICE': 1.0978,
                'LAST_PRICE': 1.0977,
                'BID_SIZE': 1000000,
                'ASK_SIZE': 2800000,
                'TOTAL_VOLUME': 633170000,
                'LAST_SIZE': 10000,
                'QUOTE_TIME': 1590599267658,
                'TRADE_TIME': 1590599267658,
                'HIGH_PRICE': 1.1031,
                'LOW_PRICE': 1.0936,
                'CLOSE_PRICE': 1.0893,
                'EXCHANGE_ID': 'T',
                'DESCRIPTION': 'Euro/USDollar Spot',
                'OPEN_PRICE': 1.0982,
                'NET_CHANGE': 0.0084,
                'EXCHANGE_NAME': 'GFT',
                'DIGITS': 2,
                'SECURITY_STATUS': 'Unknown',
                'TICK': 'UNUSED',
                'TICK_AMOUNT': 'UNUSED',
                'PRODUCT': 'UNUSED',
                'TRADING_HOURS': 'UNUSED',
                'IS_TRADABLE': 'UNUSED',
                'MARKET_MAKER': 'UNUSED',
                'HIGH_52_WEEK': 1.1031,
                'LOW_52_WEEK': 1.0936,
                'MARK': 1.0977
            }]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_forex_subs_lowercase_symbols_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FOREX', 'SUBS'))]

        await self.client.level_one_forex_subs(['eur/usd', 'eur/gBP'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FOREX',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'EUR/USD,EUR/GBP',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,' +
                           '20,21,22,23,24,25,26,27,28,29')
            }
        })

    ##########################################################################
    # LEVELONE_FUTURES_OPTIONS

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_options_subs_success_all_fields(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FUTURES_OPTIONS', 'SUBS'))]

        await self.client.level_one_futures_options_subs(
            ['NQU20_C6500', 'NQU20_P6500'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FUTURES_OPTIONS',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'NQU20_C6500,NQU20_P6500',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,' +
                           '19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_options_subs_success_some_fields(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FUTURES_OPTIONS', 'SUBS'))]

        await self.client.level_one_futures_options_subs(
            ['NQU20_C6500', 'NQU20_P6500'], fields=[
                StreamClient.LevelOneFuturesOptionsFields.SYMBOL,
                StreamClient.LevelOneFuturesOptionsFields.BID_SIZE,
                StreamClient.LevelOneFuturesOptionsFields.ASK_SIZE,
                StreamClient.LevelOneFuturesOptionsFields.FUTURE_PRICE_FORMAT,
            ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FUTURES_OPTIONS',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'NQU20_C6500,NQU20_P6500',
                'fields': '0,4,5,28'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_options_subs_success_some_fields_no_symol(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FUTURES_OPTIONS', 'SUBS'))]

        await self.client.level_one_futures_options_subs(
            ['NQU20_C6500', 'NQU20_P6500'], fields=[
                StreamClient.LevelOneFuturesOptionsFields.BID_SIZE,
                StreamClient.LevelOneFuturesOptionsFields.ASK_SIZE,
                StreamClient.LevelOneFuturesOptionsFields.FUTURE_PRICE_FORMAT,
            ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FUTURES_OPTIONS',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'NQU20_C6500,NQU20_P6500',
                'fields': '0,4,5,28'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_options_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'LEVELONE_FUTURES_OPTIONS', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.level_one_futures_options_subs(
                ['NQU20_C6500', 'NQU20_P6500'])

    @no_duplicates
    # TODO: Replace this with real messages
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_options_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [{
                'service': 'LEVELONE_FUTURES_OPTIONS',
                'timestamp': 1590245129396,
                'command': 'SUBS',
                'content': [{
                    'key': 'NQU20_C6500',
                    'delayed': False,
                    'assetMainType': 'FUTURES_OPTION',
                    '1': 2956,
                    '2': 2956.5,
                    '3': 2956.4,
                    '4': 3,
                    '5': 2,
                    '6': 'E',
                    '7': 'T',
                    '8': 1293,
                    '9': 6,
                    '10': 1590181200064,
                    '11': 1590181199726,
                    '12': 2956.6,
                    '13': 2956.3,
                    '14': 2956.25,
                    '15': '?',
                    '16': 'NASDAQ Call',
                    '17': '?',
                    '18': 2956.0,
                    '19': 0.1,
                    '20': 1.2,
                    '21': 'EXCH',
                    '22': 'Unknown',
                    '23': 19,
                    '24': 2955.9,
                    '25': 0.1,
                    '26': 100,
                    '27': 'NQU',
                    '28': '0.01',
                    '29': ('GLBX(de=1640;0=-1700151515301596;' +
                           '1=r-17001515r15301600d-15551640;' +
                           '7=d-16401555)'),
                    '30': True,
                    '31': 100,
                    '32': True,
                    '33': 17.9,
                    '33': 'NQU',
                    '34': '2020-03-01'
                }, {
                    'key': 'NQU20_C6500',
                    'delayed': False,
                    'assetMainType': 'FUTURES_OPTION',
                    '1': 2957,
                    '2': 2958.5,
                    '3': 2957.4,
                    '4': 4,
                    '5': 3,
                    '6': 'Q',
                    '7': 'V',
                    '8': 1294,
                    '9': 7,
                    '10': 1590181200065,
                    '11': 1590181199727,
                    '12': 2956.7,
                    '13': 2956.4,
                    '14': 2956.26,
                    '15': '?',
                    '16': 'NASDAQ Put',
                    '17': '?',
                    '18': 2956.1,
                    '19': 0.2,
                    '20': 1.3,
                    '21': 'EXCH',
                    '22': 'Unknown',
                    '23': 20,
                    '24': 2956.9,
                    '25': 0.2,
                    '26': 101,
                    '27': 'NQU',
                    '28': '0.02',
                    '29': ('GLBX(de=1641;0=-1700151515301596;' +
                           '1=r-17001515r15301600d-15551640;' +
                           '7=d-16401555)'),
                    '30': True,
                    '31': 101,
                    '32': True,
                    '33': 17.10,
                    '33': 'NQU',
                    '34': '2021-03-01'
                }]
            }]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(
                1, 'LEVELONE_FUTURES_OPTIONS', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.level_one_futures_options_subs(
            ['NQU20_C6500', 'NQU20_P6500'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_level_one_futures_options_handler(handler)
        self.client.add_level_one_futures_options_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'LEVELONE_FUTURES_OPTIONS',
            'timestamp': 1590245129396,
            'command': 'SUBS',
            'content': [{
                'key': 'NQU20_C6500',
                'delayed': False,
                'assetMainType': 'FUTURES_OPTION',
                'BID_PRICE': 2956,
                'ASK_PRICE': 2956.5,
                'LAST_PRICE': 2956.4,
                'BID_SIZE': 3,
                'ASK_SIZE': 2,
                'ASK_ID': 'E',
                'BID_ID': 'T',
                'TOTAL_VOLUME': 1293,
                'LAST_SIZE': 6,
                'QUOTE_TIME': 1590181200064,
                'TRADE_TIME': 1590181199726,
                'HIGH_PRICE': 2956.6,
                'LOW_PRICE': 2956.3,
                'CLOSE_PRICE': 2956.25,
                'EXCHANGE_ID': '?',
                'DESCRIPTION': 'NASDAQ Call',
                'LAST_ID': '?',
                'OPEN_PRICE': 2956.0,
                'NET_CHANGE': 0.1,
                'FUTURE_PERCENT_CHANGE': 1.2,
                'EXCHANGE_NAME': 'EXCH',
                'SECURITY_STATUS': 'Unknown',
                'OPEN_INTEREST': 19,
                'MARK': 2955.9,
                'TICK': 0.1,
                'TICK_AMOUNT': 100,
                'PRODUCT': 'NQU',
                'FUTURE_PRICE_FORMAT': '0.01',
                'FUTURE_TRADING_HOURS': ('GLBX(de=1640;0=-1700151515301596;' +
                                         '1=r-17001515r15301600d-15551640;' +
                                         '7=d-16401555)'),
                'FUTURE_IS_TRADEABLE': True,
                'FUTURE_MULTIPLIER': 100,
                'FUTURE_IS_ACTIVE': True,
                'FUTURE_SETTLEMENT_PRICE': 17.9,
                'FUTURE_SETTLEMENT_PRICE': 'NQU',
                'FUTURE_ACTIVE_SYMBOL': '2020-03-01'
            }, {
                'key': 'NQU20_C6500',
                'delayed': False,
                'assetMainType': 'FUTURES_OPTION',
                'BID_PRICE': 2957,
                'ASK_PRICE': 2958.5,
                'LAST_PRICE': 2957.4,
                'BID_SIZE': 4,
                'ASK_SIZE': 3,
                'ASK_ID': 'Q',
                'BID_ID': 'V',
                'TOTAL_VOLUME': 1294,
                'LAST_SIZE': 7,
                'QUOTE_TIME': 1590181200065,
                'TRADE_TIME': 1590181199727,
                'HIGH_PRICE': 2956.7,
                'LOW_PRICE': 2956.4,
                'CLOSE_PRICE': 2956.26,
                'EXCHANGE_ID': '?',
                'DESCRIPTION': 'NASDAQ Put',
                'LAST_ID': '?',
                'OPEN_PRICE': 2956.1,
                'NET_CHANGE': 0.2,
                'FUTURE_PERCENT_CHANGE': 1.3,
                'EXCHANGE_NAME': 'EXCH',
                'SECURITY_STATUS': 'Unknown',
                'OPEN_INTEREST': 20,
                'MARK': 2956.9,
                'TICK': 0.2,
                'TICK_AMOUNT': 101,
                'PRODUCT': 'NQU',
                'FUTURE_PRICE_FORMAT': '0.02',
                'FUTURE_TRADING_HOURS': ('GLBX(de=1641;0=-1700151515301596;' +
                                         '1=r-17001515r15301600d-15551640;' +
                                         '7=d-16401555)'),
                'FUTURE_IS_TRADEABLE': True,
                'FUTURE_MULTIPLIER': 101,
                'FUTURE_IS_ACTIVE': True,
                'FUTURE_SETTLEMENT_PRICE': 17.10,
                'FUTURE_SETTLEMENT_PRICE': 'NQU',
                'FUTURE_ACTIVE_SYMBOL': '2021-03-01'
            }]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_level_one_futures_options_lowercase_symbols_subs_success_all_fields(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LEVELONE_FUTURES_OPTIONS', 'SUBS'))]

        await self.client.level_one_futures_options_subs(
            ['nqu20_C6500', 'Nqu20_P6500'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LEVELONE_FUTURES_OPTIONS',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'NQU20_C6500,NQU20_P6500',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,' +
                           '19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35')
            }
        })

    ##########################################################################
    # TIMESALE_EQUITY

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_equity_subs_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_EQUITY', 'SUBS'))]

        await self.client.timesale_equity_subs(['GOOG', 'MSFT'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_EQUITY',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': ('0,1,2,3,4')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_equity_subs_success_some_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_EQUITY', 'SUBS'))]

        await self.client.timesale_equity_subs(['GOOG', 'MSFT'], fields=[
            StreamClient.TimesaleFields.SYMBOL,
            StreamClient.TimesaleFields.TRADE_TIME,
            StreamClient.TimesaleFields.LAST_SIZE,
        ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_EQUITY',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': '0,1,3'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_equity_subs_success_some_fields_no_symbol(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_EQUITY', 'SUBS'))]

        await self.client.timesale_equity_subs(['GOOG', 'MSFT'], fields=[
            StreamClient.TimesaleFields.TRADE_TIME,
            StreamClient.TimesaleFields.LAST_SIZE,
        ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_EQUITY',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': '0,1,3'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_equity_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'TIMESALE_EQUITY', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.timesale_equity_subs(['GOOG', 'MSFT'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_equity_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [{
                'service': 'TIMESALE_EQUITY',
                'timestamp': 1590599684016,
                'command': 'SUBS',
                'content': [{
                    'seq': 43,
                    'key': 'MSFT',
                    '1': 1590599683785,
                    '2': 179.64,
                    '3': 100.0,
                    '4': 111626
                }, {
                    'seq': 0,
                    'key': 'GOOG',
                    '1': 1590599678467,
                    '2': 1406.91,
                    '3': 100.0,
                    '4': 8620
                }]
            }]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'TIMESALE_EQUITY', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.timesale_equity_subs(['GOOG', 'MSFT'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_timesale_equity_handler(handler)
        self.client.add_timesale_equity_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'TIMESALE_EQUITY',
            'timestamp': 1590599684016,
            'command': 'SUBS',
            'content': [{
                'seq': 43,
                'key': 'MSFT',
                'TRADE_TIME': 1590599683785,
                'LAST_PRICE': 179.64,
                'LAST_SIZE': 100.0,
                'LAST_SEQUENCE': 111626
            }, {
                'seq': 0,
                'key': 'GOOG',
                'TRADE_TIME': 1590599678467,
                'LAST_PRICE': 1406.91,
                'LAST_SIZE': 100.0,
                'LAST_SEQUENCE': 8620
            }]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    ##########################################################################
    # TIMESALE_FUTURES

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_futures_subs_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_FUTURES', 'SUBS'))]

        await self.client.timesale_futures_subs(['/ES', '/CL'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_FUTURES',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': ('0,1,2,3,4')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_futures_subs_success_some_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_FUTURES', 'SUBS'))]

        await self.client.timesale_futures_subs(['/ES', '/CL'], fields=[
            StreamClient.TimesaleFields.SYMBOL,
            StreamClient.TimesaleFields.TRADE_TIME,
            StreamClient.TimesaleFields.LAST_SIZE,
        ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_FUTURES',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': '0,1,3'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_futures_subs_success_some_fields_no_symbol(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_FUTURES', 'SUBS'))]

        await self.client.timesale_futures_subs(['/ES', '/CL'], fields=[
            StreamClient.TimesaleFields.TRADE_TIME,
            StreamClient.TimesaleFields.LAST_SIZE,
        ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_FUTURES',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': '0,1,3'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_futures_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'TIMESALE_FUTURES', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.timesale_futures_subs(['/ES', '/CL'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_futures_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [{
                'service': 'TIMESALE_FUTURES',
                'timestamp': 1590600568685,
                'command': 'SUBS',
                'content': [{
                    'seq': 0,
                    'key': '/ES',
                    '1': 1590600568524,
                    '2': 2998.0,
                    '3': 1.0,
                    '4': 9236856
                }, {
                    'seq': 0,
                    'key': '/CL',
                    '1': 1590600568328,
                    '2': 33.08,
                    '3': 1.0,
                    '4': 68989244
                }]
            }]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'TIMESALE_FUTURES', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.timesale_futures_subs(['/ES', '/CL'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_timesale_futures_handler(handler)
        self.client.add_timesale_futures_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'TIMESALE_FUTURES',
            'timestamp': 1590600568685,
            'command': 'SUBS',
            'content': [{
                'seq': 0,
                'key': '/ES',
                'TRADE_TIME': 1590600568524,
                'LAST_PRICE': 2998.0,
                'LAST_SIZE': 1.0,
                'LAST_SEQUENCE': 9236856
            }, {
                'seq': 0,
                'key': '/CL',
                'TRADE_TIME': 1590600568328,
                'LAST_PRICE': 33.08,
                'LAST_SIZE': 1.0,
                'LAST_SEQUENCE': 68989244
            }]
        }
        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_equity_subs_lowercase_symbols_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_EQUITY', 'SUBS'))]

        await self.client.timesale_equity_subs(['goog', 'fb'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_EQUITY',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,FB',
                'fields': ('0,1,2,3,4')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_futures_subs_lowercase_symbols_success_some_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_FUTURES', 'SUBS'))]

        await self.client.timesale_futures_subs(['/es', '/cl'], fields=[
            StreamClient.TimesaleFields.SYMBOL,
            StreamClient.TimesaleFields.TRADE_TIME,
            StreamClient.TimesaleFields.LAST_SIZE,
        ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_FUTURES',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': '0,1,3'
            }
        })

    ##########################################################################
    # TIMESALE_OPTIONS

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_options_subs_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_OPTIONS', 'SUBS'))]

        await self.client.timesale_options_subs(['/ES', '/CL'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_OPTIONS',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': ('0,1,2,3,4')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_options_subs_success_some_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_OPTIONS', 'SUBS'))]

        await self.client.timesale_options_subs(
            ['GOOG_052920C620', 'MSFT_052920C145'], fields=[
                StreamClient.TimesaleFields.SYMBOL,
                StreamClient.TimesaleFields.TRADE_TIME,
                StreamClient.TimesaleFields.LAST_SIZE,
            ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_OPTIONS',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG_052920C620,MSFT_052920C145',
                'fields': '0,1,3'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_options_subs_success_some_fields_no_symbol(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_OPTIONS', 'SUBS'))]

        await self.client.timesale_options_subs(
            ['GOOG_052920C620', 'MSFT_052920C145'], fields=[
                StreamClient.TimesaleFields.TRADE_TIME,
                StreamClient.TimesaleFields.LAST_SIZE,
            ])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_OPTIONS',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG_052920C620,MSFT_052920C145',
                'fields': '0,1,3'
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_options_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'TIMESALE_OPTIONS', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.timesale_options_subs(
                ['GOOG_052920C620', 'MSFT_052920C145'])

    @no_duplicates
    # TODO: Replace this with real messages
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_options_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [{
                'service': 'TIMESALE_OPTIONS',
                'timestamp': 1590245129396,
                'command': 'SUBS',
                'content': [{
                    'key': 'GOOG_052920C620',
                    'delayed': False,
                    '1': 1590181199726,
                    '2': 1000,
                    '3': 100,
                    '4': 9990
                }, {
                    'key': 'MSFT_052920C145',
                    'delayed': False,
                    '1': 1590181199727,
                    '2': 1100,
                    '3': 110,
                    '4': 9991
                }]
            }]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'TIMESALE_OPTIONS', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.timesale_options_subs(
            ['GOOG_052920C620', 'MSFT_052920C145'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_timesale_options_handler(handler)
        self.client.add_timesale_options_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'TIMESALE_OPTIONS',
            'timestamp': 1590245129396,
            'command': 'SUBS',
            'content': [{
                'key': 'GOOG_052920C620',
                'delayed': False,
                'TRADE_TIME': 1590181199726,
                'LAST_PRICE': 1000,
                'LAST_SIZE': 100,
                'LAST_SEQUENCE': 9990
            }, {
                'key': 'MSFT_052920C145',
                'delayed': False,
                'TRADE_TIME': 1590181199727,
                'LAST_PRICE': 1100,
                'LAST_SIZE': 110,
                'LAST_SEQUENCE': 9991
            }]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_timesale_options_subs_lowercase_symbols_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'TIMESALE_OPTIONS', 'SUBS'))]

        await self.client.timesale_options_subs(['/es', '/cl'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'TIMESALE_OPTIONS',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': '/ES,/CL',
                'fields': ('0,1,2,3,4')
            }
        })

    ##########################################################################
    # LISTED_BOOK

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_listed_book_subs_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LISTED_BOOK', 'SUBS'))]

        await self.client.listed_book_subs(['GOOG', 'MSFT'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LISTED_BOOK',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': ('0,1,2,3')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_listed_book_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'LISTED_BOOK', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.listed_book_subs(['GOOG', 'MSFT'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_listed_book_subs_lowercase_symbols_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'LISTED_BOOK', 'SUBS'))]

        await self.client.listed_book_subs(['aapl', 'msft'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'LISTED_BOOK',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'AAPL,MSFT',
                'fields': ('0,1,2,3')
            }
        })

    ##########################################################################
    # NASDAQ_BOOK

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_nasdaq_book_subs_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'NASDAQ_BOOK', 'SUBS'))]

        await self.client.nasdaq_book_subs(['GOOG', 'MSFT'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'NASDAQ_BOOK',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': ('0,1,2,3')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_nasdaq_book_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'NASDAQ_BOOK', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.nasdaq_book_subs(['GOOG', 'MSFT'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_nasdaq_book_subs_lowercase_symbols_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'NASDAQ_BOOK', 'SUBS'))]

        await self.client.nasdaq_book_subs(['aapl', 'jnj'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'NASDAQ_BOOK',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'AAPL,JNJ',
                'fields': ('0,1,2,3')
            }
        })

    ##########################################################################
    # OPTIONS_BOOK

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_options_book_subs_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'OPTIONS_BOOK', 'SUBS'))]

        await self.client.options_book_subs(
            ['GOOG_052920C620', 'MSFT_052920C145'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'OPTIONS_BOOK',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG_052920C620,MSFT_052920C145',
                'fields': ('0,1,2,3')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_options_book_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'OPTIONS_BOOK', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.options_book_subs(
                ['GOOG_052920C620', 'MSFT_052920C145'])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_options_book_subs_lowercase_symbols_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'OPTIONS_BOOK', 'SUBS'))]

        await self.client.options_book_subs(
            ['goog_052920C620', 'msft_052920C145'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'OPTIONS_BOOK',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG_052920C620,MSFT_052920C145',
                'fields': ('0,1,2,3')
            }
        })

    ##########################################################################
    # Common book handler functionality

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_listed_book_handler(self, ws_connect):
        async def subs():
            await self.client.listed_book_subs(['GOOG', 'MSFT'])

        def register_handler():
            handler = Mock()
            async_handler = asynctest.CoroutineMock()
            self.client.add_listed_book_handler(handler)
            self.client.add_listed_book_handler(async_handler)
            return handler, async_handler

        return await self.__test_book_handler(
            ws_connect, 'LISTED_BOOK', subs, register_handler)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_nasdaq_book_handler(self, ws_connect):
        async def subs():
            await self.client.nasdaq_book_subs(['GOOG', 'MSFT'])

        def register_handler():
            handler = Mock()
            async_handler = asynctest.CoroutineMock()
            self.client.add_nasdaq_book_handler(handler)
            self.client.add_nasdaq_book_handler(async_handler)
            return handler, async_handler

        return await self.__test_book_handler(
            ws_connect, 'NASDAQ_BOOK', subs, register_handler)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_options_book_handler(self, ws_connect):
        async def subs():
            await self.client.options_book_subs(['GOOG', 'MSFT'])

        def register_handler():
            handler = Mock()
            async_handler = asynctest.CoroutineMock()
            self.client.add_options_book_handler(handler)
            self.client.add_options_book_handler(async_handler)
            return handler, async_handler

        return await self.__test_book_handler(
            ws_connect, 'OPTIONS_BOOK', subs, register_handler)

    @no_duplicates
    async def __test_book_handler(
            self, ws_connect, service, subs, register_handler):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [
                {
                    'service': service,
                    'timestamp': 1590532470149,
                    'command': 'SUBS',
                    'content': [
                        {
                            'key': 'MSFT',
                            '1': 1590532442608,
                            '2': [
                                {
                                    '0': 181.77,
                                    '1': 100,
                                    '2': 1,
                                    '3': [
                                        {
                                            '0': 'edgx',
                                            '1': 100,
                                            '2': 63150257
                                        }
                                    ]
                                },
                                {
                                    '0': 181.75,
                                    '1': 545,
                                    '2': 2,
                                    '3': [
                                        {
                                            '0': 'NSDQ',
                                            '1': 345,
                                            '2': 62685730
                                        },
                                        {
                                            '0': 'arcx',
                                            '1': 200,
                                            '2': 63242588
                                        }
                                    ]
                                },
                                {
                                    '0': 157.0,
                                    '1': 100,
                                    '2': 1,
                                    '3': [
                                        {
                                            '0': 'batx',
                                            '1': 100,
                                            '2': 63082708
                                        }
                                    ]
                                }
                            ],
                            '3': [
                                {
                                    '0': 181.95,
                                    '1': 100,
                                    '2': 1,
                                    '3': [
                                        {
                                            '0': 'arcx',
                                            '1': 100,
                                            '2': 63006734
                                        }
                                    ]
                                },
                                {
                                    '0': 181.98,
                                    '1': 48,
                                    '2': 1,
                                    '3': [
                                        {
                                            '0': 'NSDQ',
                                            '1': 48,
                                            '2': 62327464
                                        }
                                    ]
                                },
                                {
                                    '0': 182.3,
                                    '1': 100,
                                    '2': 1,
                                    '3': [
                                        {
                                            '0': 'edgx',
                                            '1': 100,
                                            '2': 63192542
                                        }
                                    ]
                                },
                                {
                                    '0': 186.8,
                                    '1': 700,
                                    '2': 1,
                                    '3': [
                                        {
                                            '0': 'batx',
                                            '1': 700,
                                            '2': 60412822
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            'key': 'GOOG',
                            '1': 1590532323728,
                            '2': [
                                {
                                    '0': 1418.0,
                                    '1': 1,
                                    '2': 1,
                                    '3': [
                                        {
                                            '0': 'NSDQ',
                                            '1': 1,
                                            '2': 54335011
                                        }
                                    ]
                                },
                                {
                                    '0': 1417.26,
                                    '1': 100,
                                    '2': 1,
                                    '3': [
                                        {
                                            '0': 'batx',
                                            '1': 100,
                                            '2': 62782324
                                        }
                                    ]
                                },
                                {
                                    '0': 1417.25,
                                    '1': 100,
                                    '2': 1,
                                    '3': [
                                        {
                                            '0': 'arcx',
                                            '1': 100,
                                            '2': 62767878
                                        }
                                    ]
                                },
                                {
                                    '0': 1400.88,
                                    '1': 100,
                                    '2': 1,
                                    '3': [
                                        {
                                            '0': 'edgx',
                                            '1': 100,
                                            '2': 54000952
                                        }
                                    ]
                                }
                            ],
                            '3': [
                                {
                                    '0': 1421.0,
                                    '1': 300,
                                    '2': 2,
                                    '3': [
                                        {
                                            '0': 'edgx',
                                            '1': 200,
                                            '2': 56723908
                                        },
                                        {
                                            '0': 'arcx',
                                            '1': 100,
                                            '2': 62709059
                                        }
                                    ]
                                },
                                {
                                    '0': 1421.73,
                                    '1': 10,
                                    '2': 1,
                                    '3': [
                                        {
                                            '0': 'NSDQ',
                                            '1': 10,
                                            '2': 62737731
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, service, 'SUBS')),
            json.dumps(stream_item)]
        await subs()

        handler, async_handler = register_handler()
        await self.client.handle_message()

        expected_item = {
            'service': service,
            'timestamp': 1590532470149,
            'command': 'SUBS',
            'content': [
                        {
                            'key': 'MSFT',
                            'BOOK_TIME': 1590532442608,
                            'BIDS': [
                                {
                                    'BID_PRICE': 181.77,
                                    'TOTAL_VOLUME': 100,
                                    'NUM_BIDS': 1,
                                    'BIDS': [
                                        {
                                            'EXCHANGE': 'edgx',
                                            'BID_VOLUME': 100,
                                            'SEQUENCE': 63150257
                                        }
                                    ]
                                },
                                {
                                    'BID_PRICE': 181.75,
                                    'TOTAL_VOLUME': 545,
                                    'NUM_BIDS': 2,
                                    'BIDS': [
                                        {
                                            'EXCHANGE': 'NSDQ',
                                            'BID_VOLUME': 345,
                                            'SEQUENCE': 62685730
                                        },
                                        {
                                            'EXCHANGE': 'arcx',
                                            'BID_VOLUME': 200,
                                            'SEQUENCE': 63242588
                                        }
                                    ]
                                },
                                {
                                    'BID_PRICE': 157.0,
                                    'TOTAL_VOLUME': 100,
                                    'NUM_BIDS': 1,
                                    'BIDS': [
                                        {
                                            'EXCHANGE': 'batx',
                                            'BID_VOLUME': 100,
                                            'SEQUENCE': 63082708
                                        }
                                    ]
                                }
                            ],
                            'ASKS': [
                                {
                                    'ASK_PRICE': 181.95,
                                    'TOTAL_VOLUME': 100,
                                    'NUM_ASKS': 1,
                                    'ASKS': [
                                        {
                                            'EXCHANGE': 'arcx',
                                            'ASK_VOLUME': 100,
                                            'SEQUENCE': 63006734
                                        }
                                    ]
                                },
                                {
                                    'ASK_PRICE': 181.98,
                                    'TOTAL_VOLUME': 48,
                                    'NUM_ASKS': 1,
                                    'ASKS': [
                                        {
                                            'EXCHANGE': 'NSDQ',
                                            'ASK_VOLUME': 48,
                                            'SEQUENCE': 62327464
                                        }
                                    ]
                                },
                                {
                                    'ASK_PRICE': 182.3,
                                    'TOTAL_VOLUME': 100,
                                    'NUM_ASKS': 1,
                                    'ASKS': [
                                        {
                                            'EXCHANGE': 'edgx',
                                            'ASK_VOLUME': 100,
                                            'SEQUENCE': 63192542
                                        }
                                    ]
                                },
                                {
                                    'ASK_PRICE': 186.8,
                                    'TOTAL_VOLUME': 700,
                                    'NUM_ASKS': 1,
                                    'ASKS': [
                                        {
                                            'EXCHANGE': 'batx',
                                            'ASK_VOLUME': 700,
                                            'SEQUENCE': 60412822
                                        }
                                    ]
                                }
                            ]
                        },
                {
                            'key': 'GOOG',
                            'BOOK_TIME': 1590532323728,
                            'BIDS': [
                                {
                                    'BID_PRICE': 1418.0,
                                    'TOTAL_VOLUME': 1,
                                    'NUM_BIDS': 1,
                                    'BIDS': [
                                        {
                                            'EXCHANGE': 'NSDQ',
                                            'BID_VOLUME': 1,
                                            'SEQUENCE': 54335011
                                        }
                                    ]
                                },
                                {
                                    'BID_PRICE': 1417.26,
                                    'TOTAL_VOLUME': 100,
                                    'NUM_BIDS': 1,
                                    'BIDS': [
                                        {
                                            'EXCHANGE': 'batx',
                                            'BID_VOLUME': 100,
                                            'SEQUENCE': 62782324
                                        }
                                    ]
                                },
                                {
                                    'BID_PRICE': 1417.25,
                                    'TOTAL_VOLUME': 100,
                                    'NUM_BIDS': 1,
                                    'BIDS': [
                                        {
                                            'EXCHANGE': 'arcx',
                                            'BID_VOLUME': 100,
                                            'SEQUENCE': 62767878
                                        }
                                    ]
                                },
                                {
                                    'BID_PRICE': 1400.88,
                                    'TOTAL_VOLUME': 100,
                                    'NUM_BIDS': 1,
                                    'BIDS': [
                                        {
                                            'EXCHANGE': 'edgx',
                                            'BID_VOLUME': 100,
                                            'SEQUENCE': 54000952
                                        }
                                    ]
                                }
                            ],
                            'ASKS': [
                                {
                                    'ASK_PRICE': 1421.0,
                                    'TOTAL_VOLUME': 300,
                                    'NUM_ASKS': 2,
                                    'ASKS': [
                                        {
                                            'EXCHANGE': 'edgx',
                                            'ASK_VOLUME': 200,
                                            'SEQUENCE': 56723908
                                        },
                                        {
                                            'EXCHANGE': 'arcx',
                                            'ASK_VOLUME': 100,
                                            'SEQUENCE': 62709059
                                        }
                                    ]
                                },
                                {
                                    'ASK_PRICE': 1421.73,
                                    'TOTAL_VOLUME': 10,
                                    'NUM_ASKS': 1,
                                    'ASKS': [
                                        {
                                            'EXCHANGE': 'NSDQ',
                                            'ASK_VOLUME': 10,
                                            'SEQUENCE': 62737731
                                        }
                                    ]
                                }
                            ]
                        }
            ]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    ##########################################################################
    # NEWS_HEADLINE

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_news_headline_subs_success(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'NEWS_HEADLINE', 'SUBS'))]

        await self.client.news_headline_subs(['GOOG', 'MSFT'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'NEWS_HEADLINE',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10')
            }
        })

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_news_headline_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'NEWS_HEADLINE', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.news_headline_subs(['GOOG', 'MSFT'])

    @no_duplicates
    # TODO: Replace this with real messages.
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_news_headline_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [{
                'service': 'NEWS_HEADLINE',
                'timestamp': 1590245129396,
                'command': 'SUBS',
                'content': [{
                    'key': 'GOOG',
                    'delayed': False,
                    '1': 0,
                    '2': 1590181199727,
                    '3': '0S21111333342',
                    '4': 'Active',
                    '5': 'Google Does Something',
                    '6': '0S1113435443',
                    '7': '1',
                    '8': 'GOOG',
                    '9': False,
                    '10': 'Bloomberg',
                }, {
                    'key': 'MSFT',
                    'delayed': False,
                    '1': 0,
                    '2': 1590181199728,
                    '3': '0S21111333343',
                    '4': 'Active',
                    '5': 'Microsoft Does Something',
                    '6': '0S1113435444',
                    '7': '2',
                    '8': 'MSFT',
                    '9': False,
                    '10': 'WSJ',
                }]
            }]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'NEWS_HEADLINE', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.news_headline_subs(['GOOG', 'MSFT'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_news_headline_handler(handler)
        self.client.add_news_headline_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'NEWS_HEADLINE',
            'timestamp': 1590245129396,
            'command': 'SUBS',
            'content': [{
                'key': 'GOOG',
                'delayed': False,
                'ERROR_CODE': 0,
                'STORY_DATETIME': 1590181199727,
                'HEADLINE_ID': '0S21111333342',
                'STATUS': 'Active',
                'HEADLINE': 'Google Does Something',
                'STORY_ID': '0S1113435443',
                'COUNT_FOR_KEYWORD': '1',
                'KEYWORD_ARRAY': 'GOOG',
                'IS_HOT': False,
                'STORY_SOURCE': 'Bloomberg',
            }, {
                'key': 'MSFT',
                'delayed': False,
                'ERROR_CODE': 0,
                'STORY_DATETIME': 1590181199728,
                'HEADLINE_ID': '0S21111333343',
                'STATUS': 'Active',
                'HEADLINE': 'Microsoft Does Something',
                'STORY_ID': '0S1113435444',
                'COUNT_FOR_KEYWORD': '2',
                'KEYWORD_ARRAY': 'MSFT',
                'IS_HOT': False,
                'STORY_SOURCE': 'WSJ',
            }]
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_news_headline_not_authorized_notification(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            "notify": [
                {
                    "service": "NEWS_HEADLINE",
                    "timestamp": 1591500923797,
                    "content": {
                        "code": 17,
                        "msg": "Not authorized for all quotes."
                    }
                }
            ]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'NEWS_HEADLINE', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.news_headline_subs(['GOOG', 'MSFT'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_news_headline_handler(handler)
        self.client.add_news_headline_handler(async_handler)
        await self.client.handle_message()

        expected_item = {
            "service": "NEWS_HEADLINE",
            "timestamp": 1591500923797,
            "content": {
                "code": 17,
                "msg": "Not authorized for all quotes."
            }
        }

        self.assert_handler_called_once_with(handler, expected_item)
        self.assert_handler_called_once_with(async_handler, expected_item)

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_news_headline_subs_lowercase_symbols_success(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'NEWS_HEADLINE', 'SUBS'))]

        await self.client.news_headline_subs(['goog', 'msft'])
        socket.recv.assert_awaited_once()
        request = self.request_from_socket_mock(socket)

        self.assertEqual(request, {
            'account': '1001',
            'service': 'NEWS_HEADLINE',
            'command': 'SUBS',
            'requestid': '1',
            'source': 'streamerInfo-appId',
            'parameters': {
                'keys': 'GOOG,MSFT',
                'fields': ('0,1,2,3,4,5,6,7,8,9,10')
            }
        })

    ###########################################################################
    # Handler edge cases
    #
    # Note: We use CHART_EQUITY as a test case, which leaks the implementation
    # detail that the handler disasynctest.patching is implemented by a common component.
    # If this were to ever change, these tests will have to be revisited.

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_messages_received_while_awaiting_response(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = self.streaming_entry('CHART_EQUITY', 'SUBS')

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_EQUITY', 'SUBS')),
            json.dumps(stream_item),
            json.dumps(self.success_response(2, 'CHART_EQUITY', 'ADD'))]

        await self.client.chart_equity_subs(['GOOG,MSFT'])
        await self.client.chart_equity_add(['INTC'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_chart_equity_handler(handler)
        self.client.add_chart_equity_handler(async_handler)
        await self.client.handle_message()
        handler.assert_called_once_with(stream_item['data'][0])
        async_handler.assert_called_once_with(stream_item['data'][0])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_messages_received_while_awaiting_failed_response_bad_code(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = self.streaming_entry('CHART_EQUITY', 'SUBS')

        failed_add_response = self.success_response(2, 'CHART_EQUITY', 'ADD')
        failed_add_response['response'][0]['content']['code'] = 21

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_EQUITY', 'SUBS')),
            json.dumps(stream_item),
            json.dumps(failed_add_response)]

        await self.client.chart_equity_subs(['GOOG,MSFT'])
        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.chart_equity_add(['INTC'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_chart_equity_handler(handler)
        self.client.add_chart_equity_handler(async_handler)
        await self.client.handle_message()
        handler.assert_called_once_with(stream_item['data'][0])
        async_handler.assert_called_once_with(stream_item['data'][0])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_messages_received_while_receiving_unexpected_response(
            self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = self.streaming_entry('CHART_EQUITY', 'SUBS')

        failed_add_response = self.success_response(999, 'CHART_EQUITY', 'ADD')
        failed_add_response['response'][0]['content']['code'] = 21

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_EQUITY', 'SUBS')),
            json.dumps(stream_item),
            json.dumps(failed_add_response)]

        await self.client.chart_equity_subs(['GOOG,MSFT'])
        with self.assertRaises(tda.streaming.UnexpectedResponse):
            await self.client.chart_equity_add(['INTC'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_chart_equity_handler(handler)
        self.client.add_chart_equity_handler(async_handler)
        await self.client.handle_message()
        handler.assert_called_once_with(stream_item['data'][0])
        async_handler.assert_called_once_with(stream_item['data'][0])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_notify_heartbeat_messages_ignored(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_EQUITY', 'SUBS')),
            json.dumps({'notify': [{'heartbeat': '1591499624412'}]})]

        await self.client.chart_equity_subs(['GOOG,MSFT'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_chart_equity_handler(handler)
        self.client.add_chart_equity_handler(async_handler)
        await self.client.handle_message()
        handler.assert_not_called()
        async_handler.assert_not_called()

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_handle_message_unexpected_response(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_EQUITY', 'SUBS')),
            json.dumps(self.success_response(2, 'CHART_EQUITY', 'SUBS'))]

        await self.client.chart_equity_subs(['GOOG,MSFT'])

        with self.assertRaises(tda.streaming.UnexpectedResponse):
            await self.client.handle_message()

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_handle_message_unparsable_message(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_EQUITY', 'SUBS')),
            '{"data":[{"service":"LEVELONE_FUTURES", ' +
            '"timestamp":1590248118165,"command":"SUBS",' +
            '"content":[{"key":"/GOOG","delayed":false,' +
            '"1":�,"2":�,"3":�,"6":"?","7":"?","12":�,"13":�,' +
            '"14":�,"15":"?","16":"Symbol not found","17":"?",' +
            '"18":�,"21":"unavailable","22":"Unknown","24":�,'
            '"28":"D,D","33":�}]}]}']

        await self.client.chart_equity_subs(['GOOG,MSFT'])

        with self.assertRaises(tda.streaming.UnparsableMessage):
            await self.client.handle_message()

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_handle_message_multiple_handlers(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item_1 = self.streaming_entry('CHART_EQUITY', 'SUBS')

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_EQUITY', 'SUBS')),
            json.dumps(stream_item_1)]

        await self.client.chart_equity_subs(['GOOG,MSFT'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_chart_equity_handler(handler)
        self.client.add_chart_equity_handler(async_handler)

        await self.client.handle_message()
        handler.assert_called_once_with(stream_item_1['data'][0])
        async_handler.assert_called_once_with(stream_item_1['data'][0])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_multiple_data_per_message(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = self.streaming_entry(
            'CHART_EQUITY', 'SUBS', [{'msg': 1}])
        stream_item['data'].append(self.streaming_entry(
            'CHART_EQUITY', 'SUBS', [{'msg': 2}])['data'][0])

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_EQUITY', 'SUBS')),
            json.dumps(stream_item)]

        await self.client.chart_equity_subs(['GOOG,MSFT'])

        handler = Mock()
        async_handler = asynctest.CoroutineMock()
        self.client.add_chart_equity_handler(handler)
        self.client.add_chart_equity_handler(async_handler)

        await self.client.handle_message()
        handler.assert_has_calls(
            [call(stream_item['data'][0]), call(stream_item['data'][1])])
        async_handler.assert_has_calls(
            [call(stream_item['data'][0]), call(stream_item['data'][1])])

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_handle_message_without_login(self, ws_connect):
        with self.assertRaisesRegex(ValueError, '.*Socket not open.*'):
            await self.client.handle_message()

    @no_duplicates
    @asynctest.patch('tda.streaming.ws_client.connect', new_callable=asynctest.CoroutineMock)
    async def test_subscribe_without_login(self, ws_connect):
        with self.assertRaisesRegex(ValueError, '.*Socket not open.*'):
            await self.client.chart_equity_subs(['GOOG,MSFT'])
