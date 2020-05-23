from tests.test_utils import MockResponse, account_principals
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch
from tda.streaming import StreamClient

import aiounittest
import asyncio
import copy
import json
import urllib.parse
import tda

ACCOUNT_ID = 1000
TOKEN_TIMESTAMP = '2020-05-22T02:12:48+0000'


class StreamClientTest(aiounittest.AsyncTestCase):

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

    def request_from_socket_mock(self, socket):
        return json.loads(
            socket.send.call_args_list[0].args[0])['requests'][0]

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

    def streaming_entry(self, service, command):
        return {
            'data': [{
                'service': service,
                'command': command,
                'timestamp': 1590186642440
            }]
        }

    async def login_and_get_socket(self, ws_connect):
        principals = account_principals()

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, True)
        socket = AsyncMock()
        ws_connect.return_value = socket

        socket.recv.side_effect = [json.dumps(self.success_response(
            0, 'ADMIN', 'LOGIN'))]

        await self.client.login()

        socket.reset_mock()
        return socket

    ##########################################################################
    # Login

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_login_single_account_success(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, True)
        socket = AsyncMock()
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

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_login_multiple_accounts_require_account_id(self, ws_connect):
        # Unfortunately, AsyncTestCase does not offer an async equivalent to
        # unittest.mock.patch
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['accounts'].append(self.account(2))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, True)

        with self.assertRaisesRegex(ValueError,
                                    '.*initialized with unspecified account_id.*'):
            await self.client.login()
        ws_connect.assert_not_called()

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_login_multiple_accounts_with_account_id(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['accounts'].append(self.account(2))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, True)
        socket = AsyncMock()
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

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_login_unrecognized_account_id(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))
        principals['accounts'].append(self.account(2))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, True)

        self.client = StreamClient(self.http_client, account_id=999999)

        with self.assertRaisesRegex(ValueError,
                                    '.*no account found with account_id 999999.*'):
            await self.client.login()
        ws_connect.assert_not_called()

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_login_bad_response(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, True)
        socket = AsyncMock()
        ws_connect.return_value = socket

        response = self.success_response(0, 'ADMIN', 'LOGIN')
        response['response'][0]['content']['code'] = 21
        response['response'][0]['content']['msg'] = 'failed for some reason'
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.login()

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_login_unexpected_request_id(self, ws_connect):
        principals = account_principals()
        principals['accounts'].clear()
        principals['accounts'].append(self.account(1))

        self.http_client.get_user_principals.return_value = MockResponse(
            principals, True)
        socket = AsyncMock()
        ws_connect.return_value = socket

        response = self.success_response(0, 'ADMIN', 'LOGIN')
        response['response'][0]['requestid'] = 9999
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaisesRegex(tda.streaming.UnexpectedResponse,
                                    'unexpected requestid: 9999'):
            await self.client.login()

    ##########################################################################
    # QOS

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
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

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_qos_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'ADMIN', 'QOS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.quality_of_service(StreamClient.QOSLevel.EXPRESS)
        socket.recv.assert_awaited_once()

    ##########################################################################
    # CHART_EQUITY

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
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

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_chart_equity_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'CHART_EQUITY', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.chart_equity_subs(['GOOG', 'MSFT'])

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
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

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_chart_equity_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [
                {
                    'service': 'CHART_EQUITY',
                    'command': 'SUBS',
                    'timestamp': 1590186642440,
                    'content': [
                        {
                            'key': 'MSFT',
                            '1': 200,
                            '2': 300,
                            '3': 100,
                            '4': 200,
                            '5': 123456789,
                            '6': 901,
                            '7': 1590187260000,
                            '8': 18404,
                        },
                        {
                            'key': 'GOOG',
                            '1': 2000,
                            '2': 3000,
                            '3': 1000,
                            '4': 2000,
                            '5': 1234567890,
                            '6': 9010,
                            '7': 1590187260000,
                            '8': 18404,
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
        self.client.add_chart_equity_handler(handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'CHART_EQUITY',
            'command': 'SUBS',
            'timestamp': 1590186642440,
            'content': [
                        {
                            'key': 'MSFT',
                            'OPEN_PRICE': 200,
                            'HIGH_PRICE': 300,
                            'LOW_PRICE': 100,
                            'CLOSE_PRICE': 200,
                            'VOLUME': 123456789,
                            'SEQUENCE': 901,
                            'CHART_TIME': 1590187260000,
                            'CHART_DAY': 18404,
                        },
                {
                            'key': 'GOOG',
                            'OPEN_PRICE': 2000,
                            'HIGH_PRICE': 3000,
                            'LOW_PRICE': 1000,
                            'CLOSE_PRICE': 2000,
                            'VOLUME': 1234567890,
                            'SEQUENCE': 9010,
                            'CHART_TIME': 1590187260000,
                            'CHART_DAY': 18404,
                        }
            ]
        }

        handler.assert_called_once_with(expected_item)

    ##########################################################################
    # CHART_FUTURES

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
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
                'fields': '0,1,2,3,4,5,6,7,8'
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
                'fields': '0,1,2,3,4,5,6,7,8'
            }
        })

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_chart_futures_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'CHART_FUTURES', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.chart_futures_subs(['/ES', '/CL'])

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
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

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_chart_futures_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [
                {
                    'service': 'CHART_FUTURES',
                    'command': 'SUBS',
                    'timestamp': 1590186642440,
                    'content': [
                        {
                            'key': '/ES',
                            '1': 200,
                            '2': 300,
                            '3': 100,
                            '4': 200,
                            '5': 123456789,
                            '6': 901,
                            '7': 1590187260000,
                            '8': 18404,
                        },
                        {
                            'key': '/CL',
                            '1': 2000,
                            '2': 3000,
                            '3': 1000,
                            '4': 2000,
                            '5': 1234567890,
                            '6': 9010,
                            '7': 1590187260000,
                            '8': 18404,
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
        self.client.add_chart_futures_handler(handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'CHART_FUTURES',
            'command': 'SUBS',
            'timestamp': 1590186642440,
            'content': [
                        {
                            'key': '/ES',
                            'OPEN_PRICE': 200,
                            'HIGH_PRICE': 300,
                            'LOW_PRICE': 100,
                            'CLOSE_PRICE': 200,
                            'VOLUME': 123456789,
                            'SEQUENCE': 901,
                            'CHART_TIME': 1590187260000,
                            'CHART_DAY': 18404,
                        },
                {
                            'key': '/CL',
                            'OPEN_PRICE': 2000,
                            'HIGH_PRICE': 3000,
                            'LOW_PRICE': 1000,
                            'CLOSE_PRICE': 2000,
                            'VOLUME': 1234567890,
                            'SEQUENCE': 9010,
                            'CHART_TIME': 1590187260000,
                            'CHART_DAY': 18404,
                        }
            ]
        }

        handler.assert_called_once_with(expected_item)

    ##########################################################################
    # QUOTE

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_level_one_quote_subs_success_all_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'QUOTE', 'SUBS'))]

        await self.client.level_one_quote_subs(['GOOG', 'MSFT'])
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

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_level_one_quote_subs_success_some_fields(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        socket.recv.side_effect = [json.dumps(self.success_response(
            1, 'QUOTE', 'SUBS'))]

        await self.client.level_one_quote_subs(['GOOG', 'MSFT'], fields=[
            StreamClient.LevelOneQuoteFields.SYMBOL,
            StreamClient.LevelOneQuoteFields.BID_PRICE,
            StreamClient.LevelOneQuoteFields.ASK_PRICE,
            StreamClient.LevelOneQuoteFields.QUOTE_TIME,
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

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_level_one_quote_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'QUOTE', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.level_one_quote_subs(['GOOG', 'MSFT'])

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_level_one_quote_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            'data': [
                {
                    'service': 'QUOTE',
                    'command': 'SUBS',
                    'timestamp': 1590186642440,
                    'content': [
                        {
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
                        },
                        {
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
                        }
                    ]
                }
            ]
        }

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'QUOTE', 'SUBS')),
            json.dumps(stream_item)]
        await self.client.level_one_quote_subs(['GOOG', 'MSFT'])

        handler = Mock()
        self.client.add_level_one_quote_handler(handler)
        await self.client.handle_message()

        expected_item = {
            'service': 'QUOTE',
            'command': 'SUBS',
            'timestamp': 1590186642440,
            'content': [
                {
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
                },
                {
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
                }
            ]
        }

        handler.assert_called_once_with(expected_item)

    ##########################################################################
    # OPTION

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
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

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
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

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_level_one_option_subs_failure(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        response = self.success_response(1, 'OPTION', 'SUBS')
        response['response'][0]['content']['code'] = 21
        socket.recv.side_effect = [json.dumps(response)]

        with self.assertRaises(tda.streaming.UnexpectedResponseCode):
            await self.client.level_one_option_subs(
                ['GOOG_052920C620', 'MSFT_052920C145'])

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_level_one_option_handler(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = {
            "data": [
                {
                    "service": "OPTION",
                    "timestamp": 1590244265891,
                    "command": "SUBS",
                    "content": [{
                        "key": "MSFT_052920C145",
                        "delayed": False,
                        "assetMainType": "OPTION",
                        "cusip": "0MSFT.ET00145000",
                        "1": "MSFT May 29 2020 145 Call (Weekly)",
                        "2": 38.05,
                        "3": 39.05,
                        "4": 38.85,
                        "5": 38.85,
                        "6": 38.85,
                        "7": 38.581,
                        "8": 2,
                        "9": 7,
                        "10": 5,
                        "11": 57599,
                        "12": 53017,
                        "13": 38.51,
                        "14": 18404,
                        "15": 18404,
                        "16": 2020,
                        "17": 100,
                        "18": 2,
                        "19": 38.85,
                        "20": 6,
                        "21": 116,
                        "22": 1,
                        "23": 0.3185,
                        "24": 145,
                        "25": "C",
                        "26": "MSFT",
                        "27": 5,
                        "29": 0.34,
                        "30": 29,
                        "31": 6,
                        "32": 1,
                        "33": 0,
                        "34": 0,
                        "35": 0.1882,
                        "37": "Normal",
                        "38": 38.675,
                        "39": 183.51,
                        "40": "S",
                        "41": 38.55
                    }, {
                        "key": "GOOG_052920C620",
                        "delayed": False,
                        "assetMainType": "OPTION",
                        "cusip": "0GOOG.ET00620000",
                        "1": "GOOG May 29 2020 620 Call (Weekly)",
                        "2": 785.2,
                        "3": 794,
                        "7": 790.42,
                        "10": 238.2373,
                        "11": 57594,
                        "12": 68400,
                        "13": 790.42,
                        "14": 18404,
                        "16": 2020,
                        "17": 100,
                        "18": 2,
                        "20": 1,
                        "21": 6,
                        "24": 620,
                        "25": "C",
                        "26": "GOOG",
                        "27": 5,
                        "29": -0.82,
                        "30": 29,
                        "31": 6,
                        "32": 0.996,
                        "33": 0,
                        "34": -0.3931,
                        "35": 0.023,
                        "36": 0.1176,
                        "37": "Normal",
                        "38": 789.6,
                        "39": 1410.42,
                        "40": "S",
                        "41": 789.6
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
        self.client.add_level_one_option_handler(handler)
        await self.client.handle_message()

        expected_item = {
            "service": "OPTION",
            "timestamp": 1590244265891,
            "command": "SUBS",
            "content": [{
                "key": "MSFT_052920C145",
                "delayed": False,
                "assetMainType": "OPTION",
                "cusip": "0MSFT.ET00145000",
                "DESCRIPTION": "MSFT May 29 2020 145 Call (Weekly)",
                "BID_PRICE": 38.05,
                "ASK_PRICE": 39.05,
                "LAST_PRICE": 38.85,
                "HIGH_PRICE": 38.85,
                "LOW_PRICE": 38.85,
                "CLOSE_PRICE": 38.581,
                "TOTAL_VOLUME": 2,
                "OPEN_INTEREST": 7,
                "VOLATILITY": 5,
                "QUOTE_TIME": 57599,
                "TRADE_TIME": 53017,
                "MONEY_INTRINSIC_VALUE": 38.51,
                "QUOTE_DAY": 18404,
                "TRADE_DAY": 18404,
                "EXPIRATION_YEAR": 2020,
                "MULTIPLIER": 100,
                "DIGITS": 2,
                "OPEN_PRICE": 38.85,
                "BID_SIZE": 6,
                "ASK_SIZE": 116,
                "LAST_SIZE": 1,
                "NET_CHANGE": 0.3185,
                "STRIKE_PRICE": 145,
                "CONTRACT_TYPE": "C",
                "UNDERLYING": "MSFT",
                "EXPIRATION_MONTH": 5,
                "TIME_VALUE": 0.34,
                "EXPIRATION_DAY": 29,
                "DAYS_TO_EXPIRATION": 6,
                "DELTA": 1,
                "GAMMA": 0,
                "THETA": 0,
                "VEGA": 0.1882,
                "SECURITY_STATUS": "Normal",
                "THEORETICAL_OPTION_VALUE": 38.675,
                "UNDERLYING_PRICE": 183.51,
                "UV_EXPIRATION_TYPE": "S",
                "MARK": 38.55
            }, {
                "key": "GOOG_052920C620",
                "delayed": False,
                "assetMainType": "OPTION",
                "cusip": "0GOOG.ET00620000",
                "DESCRIPTION": "GOOG May 29 2020 620 Call (Weekly)",
                "BID_PRICE": 785.2,
                "ASK_PRICE": 794,
                "CLOSE_PRICE": 790.42,
                "VOLATILITY": 238.2373,
                "QUOTE_TIME": 57594,
                "TRADE_TIME": 68400,
                "MONEY_INTRINSIC_VALUE": 790.42,
                "QUOTE_DAY": 18404,
                "EXPIRATION_YEAR": 2020,
                "MULTIPLIER": 100,
                "DIGITS": 2,
                "BID_SIZE": 1,
                "ASK_SIZE": 6,
                "STRIKE_PRICE": 620,
                "CONTRACT_TYPE": "C",
                "UNDERLYING": "GOOG",
                "EXPIRATION_MONTH": 5,
                "TIME_VALUE": -0.82,
                "EXPIRATION_DAY": 29,
                "DAYS_TO_EXPIRATION": 6,
                "DELTA": 0.996,
                "GAMMA": 0,
                "THETA": -0.3931,
                "VEGA": 0.023,
                "RHO": 0.1176,
                "SECURITY_STATUS": "Normal",
                "THEORETICAL_OPTION_VALUE": 789.6,
                "UNDERLYING_PRICE": 1410.42,
                "UV_EXPIRATION_TYPE": "S",
                "MARK": 789.6
            }]
        }

        handler.assert_called_once_with(expected_item)

    ###########################################################################
    # Handler edge cases
    #
    # Note: We use CHART_EQUITY as a test case, which leaks the implementation
    # detail that the handler dispatching is implemented by a common component.
    # If this were to ever change, these tests will have to be revisited.

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
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
        self.client.add_chart_equity_handler(handler)
        await self.client.handle_message()
        handler.assert_called_once_with(stream_item['data'][0])

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
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
        self.client.add_chart_equity_handler(handler)
        await self.client.handle_message()
        handler.assert_called_once_with(stream_item['data'][0])

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
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
        self.client.add_chart_equity_handler(handler)
        await self.client.handle_message()
        handler.assert_called_once_with(stream_item['data'][0])

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_notify_messages_ignored(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = self.streaming_entry('CHART_EQUITY', 'SUBS')

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_EQUITY', 'SUBS')),
            json.dumps({'notify': {'doesnt': 'matter'}})]

        await self.client.chart_equity_subs(['GOOG,MSFT'])

        handler = Mock()
        self.client.add_chart_equity_handler(handler)
        await self.client.handle_message()
        handler.assert_not_called()

    @patch('tda.streaming.websockets.client.connect', autospec=AsyncMock())
    async def test_handle_message_unexpected_response(self, ws_connect):
        socket = await self.login_and_get_socket(ws_connect)

        stream_item = self.streaming_entry('CHART_EQUITY', 'SUBS')

        socket.recv.side_effect = [
            json.dumps(self.success_response(1, 'CHART_EQUITY', 'SUBS')),
            json.dumps(self.success_response(2, 'CHART_EQUITY', 'SUBS'))]

        await self.client.chart_equity_subs(['GOOG,MSFT'])

        with self.assertRaises(tda.streaming.UnexpectedResponse):
            await self.client.handle_message()