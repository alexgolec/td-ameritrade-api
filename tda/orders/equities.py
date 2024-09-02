from enum import Enum

from tda.orders.common import (
        Duration,
        StopType,
        Session,
        StopType,
        StopPriceLinkBasis,
        StopPriceLinkType,
)


##########################################################################
# Buy orders


def equity_buy_market(symbol, quantity):
    '''
    Returns a pre-filled :class:`~tda.orders.generic.OrderBuilder` for an equity
    buy market order.
    '''
    from tda.orders.common import Duration, EquityInstruction
    from tda.orders.common import OrderStrategyType, OrderType, Session
    from tda.orders.generic import OrderBuilder

    return (OrderBuilder()
            .set_order_type(OrderType.MARKET)
            .set_session(Session.NORMAL)
            .set_duration(Duration.DAY)
            .set_order_strategy_type(OrderStrategyType.SINGLE)
            .add_equity_leg(EquityInstruction.BUY, symbol, quantity))


def equity_buy_limit(symbol, quantity, price):
    '''
    Returns a pre-filled :class:`~tda.orders.generic.OrderBuilder` for an equity
    buy limit order.
    '''
    from tda.orders.common import Duration, EquityInstruction
    from tda.orders.common import OrderStrategyType, OrderType, Session
    from tda.orders.generic import OrderBuilder

    return (OrderBuilder()
            .set_order_type(OrderType.LIMIT)
            .set_price(price)
            .set_session(Session.NORMAL)
            .set_duration(Duration.DAY)
            .set_order_strategy_type(OrderStrategyType.SINGLE)
            .add_equity_leg(EquityInstruction.BUY, symbol, quantity))

##########################################################################
# Sell orders


def equity_sell_market(symbol, quantity):
    '''
    Returns a pre-filled :class:`~tda.orders.generic.OrderBuilder` for an equity
    sell market order.
    '''
    from tda.orders.common import Duration, EquityInstruction
    from tda.orders.common import OrderStrategyType, OrderType, Session
    from tda.orders.generic import OrderBuilder

    return (OrderBuilder()
            .set_order_type(OrderType.MARKET)
            .set_session(Session.NORMAL)
            .set_duration(Duration.DAY)
            .set_order_strategy_type(OrderStrategyType.SINGLE)
            .add_equity_leg(EquityInstruction.SELL, symbol, quantity))


def equity_sell_limit(symbol, quantity, price):
    '''
    Returns a pre-filled :class:`~tda.orders.generic.OrderBuilder` for an equity
    sell limit order.
    '''
    from tda.orders.common import Duration, EquityInstruction
    from tda.orders.common import OrderStrategyType, OrderType, Session
    from tda.orders.generic import OrderBuilder

    return (OrderBuilder()
            .set_order_type(OrderType.LIMIT)
            .set_price(price)
            .set_session(Session.NORMAL)
            .set_duration(Duration.DAY)
            .set_order_strategy_type(OrderStrategyType.SINGLE)
            .add_equity_leg(EquityInstruction.SELL, symbol, quantity))

##########################################################################
# Short sell orders


def equity_sell_short_market(symbol, quantity):
    '''
    Returns a pre-filled :class:`~tda.orders.generic.OrderBuilder` for an equity
    short sell market order.
    '''
    from tda.orders.common import Duration, EquityInstruction
    from tda.orders.common import OrderStrategyType, OrderType, Session
    from tda.orders.generic import OrderBuilder

    return (OrderBuilder()
            .set_order_type(OrderType.MARKET)
            .set_session(Session.NORMAL)
            .set_duration(Duration.DAY)
            .set_order_strategy_type(OrderStrategyType.SINGLE)
            .add_equity_leg(EquityInstruction.SELL_SHORT, symbol, quantity))


def equity_sell_short_limit(symbol, quantity, price):
    '''
    Returns a pre-filled :class:`~tda.orders.generic.OrderBuilder` for an equity
    short sell limit order.
    '''
    from tda.orders.common import Duration, EquityInstruction
    from tda.orders.common import OrderStrategyType, OrderType, Session
    from tda.orders.generic import OrderBuilder

    return (OrderBuilder()
            .set_order_type(OrderType.LIMIT)
            .set_price(price)
            .set_session(Session.NORMAL)
            .set_duration(Duration.DAY)
            .set_order_strategy_type(OrderStrategyType.SINGLE)
            .add_equity_leg(EquityInstruction.SELL_SHORT, symbol, quantity))

##########################################################################
# Buy to cover orders


def equity_buy_to_cover_market(symbol, quantity):
    '''
    Returns a pre-filled :class:`~tda.orders.generic.OrderBuilder` for an equity
    buy-to-cover market order.
    '''
    from tda.orders.common import Duration, EquityInstruction
    from tda.orders.common import OrderStrategyType, OrderType, Session
    from tda.orders.generic import OrderBuilder

    return (OrderBuilder()
            .set_order_type(OrderType.MARKET)
            .set_session(Session.NORMAL)
            .set_duration(Duration.DAY)
            .set_order_strategy_type(OrderStrategyType.SINGLE)
            .add_equity_leg(EquityInstruction.BUY_TO_COVER, symbol, quantity))


def equity_buy_to_cover_limit(symbol, quantity, price):
    '''
    Returns a pre-filled :class:`~tda.orders.generic.OrderBuilder` for an equity
    buy-to-cover limit order.
    '''
    from tda.orders.common import Duration, EquityInstruction
    from tda.orders.common import OrderStrategyType, OrderType, Session
    from tda.orders.generic import OrderBuilder

    return (OrderBuilder()
            .set_order_type(OrderType.LIMIT)
            .set_price(price)
            .set_session(Session.NORMAL)
            .set_duration(Duration.DAY)
            .set_order_strategy_type(OrderStrategyType.SINGLE)
            .add_equity_leg(EquityInstruction.BUY_TO_COVER, symbol, quantity))


##########################################################################
# Stop orders


def equity_sell_stop_market(
        symbol, quantity, stop_price, stop_type=StopType.MARK):
    """
    Returns a pre-filled :class:`~tda.orders.generic.OrderBuilder` for an equity
    sell stop order.
    """
    from tda.orders.common import Duration, EquityInstruction
    from tda.orders.common import OrderStrategyType, OrderType, Session
    from tda.orders.generic import OrderBuilder

    return (OrderBuilder()
            .set_order_type(OrderType.STOP)
            .set_quantity(quantity)
            .set_session(Session.NORMAL)
            .set_stop_price(stop_price)
            .set_stop_type(stop_type)
            .set_duration(Duration.DAY)
            .set_order_strategy_type(OrderStrategyType.SINGLE)
            .add_equity_leg(EquityInstruction.SELL, symbol, quantity))


def equity_sell_stop_limit(symbol, quantity, limit_price, stop_price, 
                           stop_type=StopType.MARK):
    """
    Returns a pre-filled :class:`~tda.orders.generic.OrderBuilder` for an equity
    sell stop limit order.
    """

    from tda.orders.common import Duration, EquityInstruction
    from tda.orders.common import OrderStrategyType, OrderType, Session
    from tda.orders.generic import OrderBuilder

    return (OrderBuilder()
            .set_order_type(OrderType.STOP_LIMIT)
            .set_quantity(quantity)
            .set_session(Session.NORMAL)
            .set_price(limit_price)
            .set_stop_price(stop_price)
            .set_stop_type(stop_type)
            .set_duration(Duration.DAY)
            .set_order_strategy_type(OrderStrategyType.SINGLE)
            .add_equity_leg(EquityInstruction.SELL, symbol, quantity))


def equity_sell_trailing_stop_market(
        symbol, quantity, trail_offset,
        trail_offset_type=StopPriceLinkType.PERCENT, stop_type=StopType.MARK,
        stop_price_link_basis=StopPriceLinkBasis.MARK):
    """
    Returns a pre-filled :class:`~tda.orders.generic.OrderBuilder` for an equity
    sell trailing stop order.
    """

    from tda.orders.common import Duration, EquityInstruction
    from tda.orders.common import OrderStrategyType, OrderType, Session
    from tda.orders.generic import OrderBuilder

    return (OrderBuilder()
            .set_order_type(OrderType.TRAILING_STOP)
            .set_quantity(quantity)
            .set_session(Session.NORMAL)
            .set_duration(Duration.DAY)
            .set_stop_type(stop_type)
            .set_stop_price_offset(trail_offset)
            .set_stop_price_link_basis(stop_price_link_basis)
            .set_stop_price_link_type(trail_offset_type)
            .set_order_strategy_type(OrderStrategyType.SINGLE)
            .add_equity_leg(EquityInstruction.SELL, symbol, quantity))
