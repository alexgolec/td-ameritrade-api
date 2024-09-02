import unittest

from tda.orders.common import *
from tda.orders.equities import *
from .utils import has_diff, no_duplicates

import imp
from unittest.mock import patch


class EquityOrderBuilderLegacy(unittest.TestCase):

    def test_import_EquityOrderBuilder(self):
        import sys
        assert sys.version_info[0] == 3

        if sys.version_info[1] >= 7:
            with self.assertRaisesRegex(
                    ImportError, 'EquityOrderBuilder has been deleted'):
                from tda.orders import EquityOrderBuilder

    def test_import_EquityOrderBuilder_pre_3_7(self):
        import sys
        assert sys.version_info[0] == 3

        if sys.version_info[1] < 7:
            from tda import orders
            imp.reload(orders)

            from tda.orders import EquityOrderBuilder

            with self.assertRaisesRegex(NotImplementedError,
                    'EquityOrderBuilder has been deleted'):
                EquityOrderBuilder('args')

    def test_other_import(self):
        with self.assertRaisesRegex(ImportError, 'bogus'):
            from tda.orders import bogus

    def test_attribute_access(self):
        with self.assertRaisesRegex(AttributeError, 'bogus'):
            import tda
            print(tda.orders.bogus)


class BuilderTemplates(unittest.TestCase):

    def test_equity_buy_market(self):
        self.assertFalse(has_diff({
            'orderType': 'MARKET',
            'session': 'NORMAL',
            'duration': 'DAY',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': 'BUY',
                'quantity': 10,
                'instrument': {
                    'symbol': 'GOOG',
                    'assetType': 'EQUITY',
                }
            }]
        }, equity_buy_market('GOOG', 10).build()))

    def test_equity_buy_limit(self):
        self.assertFalse(has_diff({
            'orderType': 'LIMIT',
            'session': 'NORMAL',
            'duration': 'DAY',
            'price': '199.99',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': 'BUY',
                'quantity': 10,
                'instrument': {
                    'symbol': 'GOOG',
                    'assetType': 'EQUITY',
                }
            }]
        }, equity_buy_limit('GOOG', 10, 199.99).build()))

    def test_equity_sell_market(self):
        self.assertFalse(has_diff({
            'orderType': 'MARKET',
            'session': 'NORMAL',
            'duration': 'DAY',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': 'SELL',
                'quantity': 10,
                'instrument': {
                    'symbol': 'GOOG',
                    'assetType': 'EQUITY',
                }
            }]
        }, equity_sell_market('GOOG', 10).build()))

    def test_equity_sell_limit(self):
        self.assertFalse(has_diff({
            'orderType': 'LIMIT',
            'session': 'NORMAL',
            'duration': 'DAY',
            'price': '199.99',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': 'SELL',
                'quantity': 10,
                'instrument': {
                    'symbol': 'GOOG',
                    'assetType': 'EQUITY',
                }
            }]
        }, equity_sell_limit('GOOG', 10, 199.99).build()))

    def test_equity_sell_short_market(self):
        self.assertFalse(has_diff({
            'orderType': 'MARKET',
            'session': 'NORMAL',
            'duration': 'DAY',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': 'SELL_SHORT',
                'quantity': 10,
                'instrument': {
                    'symbol': 'GOOG',
                    'assetType': 'EQUITY',
                }
            }]
        }, equity_sell_short_market('GOOG', 10).build()))

    def test_equity_sell_short_limit(self):
        self.assertFalse(has_diff({
            'orderType': 'LIMIT',
            'session': 'NORMAL',
            'duration': 'DAY',
            'price': '199.99',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': 'SELL_SHORT',
                'quantity': 10,
                'instrument': {
                    'symbol': 'GOOG',
                    'assetType': 'EQUITY',
                }
            }]
        }, equity_sell_short_limit('GOOG', 10, 199.99).build()))

    def test_equity_buy_to_cover_market(self):
        self.assertFalse(has_diff({
            'orderType': 'MARKET',
            'session': 'NORMAL',
            'duration': 'DAY',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': 'BUY_TO_COVER',
                'quantity': 10,
                'instrument': {
                    'symbol': 'GOOG',
                    'assetType': 'EQUITY',
                }
            }]
        }, equity_buy_to_cover_market('GOOG', 10).build()))

    def test_equity_buy_to_cover_limit(self):
        self.assertFalse(has_diff({
            'orderType': 'LIMIT',
            'session': 'NORMAL',
            'duration': 'DAY',
            'price': '199.99',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': 'BUY_TO_COVER',
                'quantity': 10,
                'instrument': {
                    'symbol': 'GOOG',
                    'assetType': 'EQUITY',
                }
            }]
        }, equity_buy_to_cover_limit('GOOG', 10, 199.99).build()))

    def test_equity_sell_stop_market(self):
        self.assertFalse(has_diff({
            'orderType': 'STOP',
            'quantity': 10,
            'session': 'NORMAL',
            'duration': 'DAY',
            'stopPrice': '199.99',
            'stopType': 'MARK',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': 'SELL',
                'quantity': 10,
                'instrument': {
                    'symbol': 'GOOG',
                    'assetType': 'EQUITY',
                }
            }]
        }, equity_sell_stop_market('GOOG', 10, 199.99).build()))

    def test_equity_sell_stop_limit(self):
        self.assertFalse(has_diff({
            'orderType': 'STOP_LIMIT',
            'quantity': 10,
            'session': 'NORMAL',
            'duration': 'DAY',
            'price': '88.88',
            'stopPrice': '199.99',
            'stopType': 'MARK',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': 'SELL',
                'quantity': 10,
                'instrument': {
                    'symbol': 'GOOG',
                    'assetType': 'EQUITY',
                }
            }]
        }, equity_sell_stop_limit('GOOG', 10, 88.88, 199.99).build()))

    def test_equity_sell_trailing_stop_market(self):
        self.assertFalse(has_diff({
            'orderType': 'TRAILING_STOP',
            'quantity': 10,
            'session': 'NORMAL',
            'duration': 'DAY',
            'stopType': 'MARK',
            'stopPriceOffset': 5,
            'stopPriceLinkBasis': 'MARK',
            'stopPriceLinkType': 'PERCENT',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': 'SELL',
                'quantity': 10,
                'instrument': {
                    'symbol': 'GOOG',
                    'assetType': 'EQUITY',
                }
            }]
        }, equity_sell_trailing_stop_market('GOOG', 10, 5).build()))
