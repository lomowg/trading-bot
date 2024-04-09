import asyncio
import logging
import pandas as pd
from uuid import uuid4

from tinkoff.invest.utils import now
from app.strategies.base import BaseStrategy
from app.client import client
from app.settings import settings
from typing import List, Optional
from tinkoff.invest import CandleInterval, HistoricCandle, AioRequestError, Instrument
from app.stats.handler import StatsHandler
from app.strategies.models import StrategyName
from datetime import timedelta
from app.strategies.BollingerBands.models import BollingerBandsConfig
from app.utils.portfolio import get_position, get_order
from app.utils.quotation import quotation_to_float
from tinkoff.invest.grpc.instruments_pb2 import INSTRUMENT_ID_TYPE_FIGI
from tinkoff.invest.grpc.orders_pb2 import (
    ORDER_DIRECTION_SELL,
    ORDER_DIRECTION_BUY,
    ORDER_TYPE_MARKET,
)
from app.strategies.BollingerBands.models import Corridor
from app.utils.quantity import is_quantity_valid

logger = logging.getLogger(__name__)


class BollingerBands(BaseStrategy):
    def __init__(self, figi: str, **kwargs):
        self.account_id = settings.account_id
        self.figi = figi
        self.instrument_info: Optional[Instrument] = None
        self.corridor: Optional[Corridor] = None
        self.config: BollingerBandsConfig = BollingerBandsConfig(**kwargs)
        self.stats_handler = StatsHandler(StrategyName.BOLLINGER_BANDS, client)
        self.total_price = 0
        self.total_quantity = 0

    async def get_historical_data(self) -> List[HistoricCandle]:
        """
        Gets historical data for the instrument. Returns list of candles.
        Requests all the 1-min candles from days_back_to_consider days back to now.

        :return: list of HistoricCandle
        """
        candles = []
        logger.debug(
            f"Start getting historical data for 90 "
            f"days back from now. figi={self.figi}"
        )
        async for candle in client.get_all_candles(
                figi=self.figi,
                from_=now() - timedelta(days=90),
                to=now(),
                interval=CandleInterval.CANDLE_INTERVAL_1_MIN,
        ):
            candles.append(candle)
        logger.debug(f"Found {len(candles)} candles. figi={self.figi}")
        return candles

    async def get_position_quantity(self) -> int:
        """
        Get quantity of the instrument in the position.
        :return: int - quantity
        """
        positions = (await client.get_portfolio(account_id=self.account_id)).positions
        position = get_position(positions, self.figi)
        if position is None:
            return 0
        return int(quotation_to_float(position.quantity))

    async def get_last_price(self) -> float:
        """
        Get last price of the instrument.
        :return: float - last price
        """
        last_prices_response = await client.get_last_prices(figi=[self.figi])
        last_prices = last_prices_response.last_prices
        return quotation_to_float(last_prices.pop().price)

    async def ensure_market_open(self):
        """
        Ensure that the market is open. Holds the loop until the instrument is available.
        :return: when instrument is available for trading
        """
        trading_status = await client.get_trading_status(figi=self.figi)
        while not (
                trading_status.market_order_available_flag and trading_status.api_trade_available_flag
        ):
            logger.debug(f"Waiting for the market to open. figi={self.figi}")
            await asyncio.sleep(60)
            trading_status = await client.get_trading_status(figi=self.figi)

    async def prepare_data(self):
        self.instrument_info = (
            await client.get_instrument(id_type=INSTRUMENT_ID_TYPE_FIGI, id=self.figi)
        ).instrument

    async def update_bollinger_bands(self) -> None:
        """
        Gets historical data and calculates new Bollinger Bands. Stores them in the class.
        """
        candles = await self.get_historical_data()
        if len(candles) == 0:
            return

        values = [quotation_to_float(candle.close) for candle in candles]

        prices_series = pd.Series(values)

        rolling_mean = prices_series.rolling(window=self.config.length).mean()
        rolling_std = prices_series.rolling(window=self.config.length).std()

        bottom_band = rolling_mean - self.config.num_std * rolling_std
        top_band = rolling_mean + self.config.num_std * rolling_std

        self.corridor = Corridor(bottom=bottom_band.iloc[-1], top=top_band.iloc[-1])

        logger.debug(
            f"Bollinger Bands: {self.corridor}.\n"
            f"Length={self.config.length} num_std={self.config.num_std} check_interval={self.config.check_interval} quantity_limit={self.config.quantity_limit} figi={self.figi}\n"
            f"Mean: {rolling_mean.iloc[-1]}\n"
        )

    async def handle_bollinger_band_crossing_top(self, last_price: float) -> None:
        """
        This method is called when last price is higher than the upper Bollinger Band.
        Check how many shares we already have and sell them.

        :param last_price: last price of the instrument
        """
        position_quantity = await self.get_position_quantity()
        if position_quantity > 0:
            logger.info(
                f"Selling {position_quantity} shares. Last price={last_price} figi={self.figi}"
            )
            try:
                quantity = position_quantity / self.instrument_info.lot
                if not is_quantity_valid(quantity):
                    raise ValueError(f"Invalid quantity for posting an order. quantity={quantity}")
                posted_order = await client.post_order(
                    order_id=str(uuid4()),
                    figi=self.figi,
                    direction=ORDER_DIRECTION_SELL,
                    quantity=int(quantity),
                    order_type=ORDER_TYPE_MARKET,
                    account_id=self.account_id,
                )
            except Exception as e:
                logger.error(f"Failed to post sell order. figi={self.figi}. {e}")
                return
            await asyncio.create_task(
                self.stats_handler.handle_new_order(
                    order_id=posted_order.order_id, account_id=self.account_id
                )
            )
            self.total_price = 0
            self.total_quantity = 0

    async def handle_bollinger_band_crossing_bottom(self, last_price: float) -> None:
        """
        This method is called when last price is lower than the lower Bollinger Band.
        Check how many shares we already have and buy more until the quantity_limit is reached.

        :param last_price: last price of the instrument
        """
        position_quantity = await self.get_position_quantity()
        if position_quantity <= self.config.quantity_limit:
            quantity_to_buy = self.config.quantity_limit - position_quantity
            logger.info(
                f"Buying {quantity_to_buy} shares. Last price={last_price} figi={self.figi}"
            )
            try:
                quantity = quantity_to_buy / self.instrument_info.lot
                if not is_quantity_valid(quantity):
                    raise ValueError(f"Invalid quantity for posting an order. quantity={quantity}")
                posted_order = await client.post_order(
                    order_id=str(uuid4()),
                    figi=self.figi,
                    direction=ORDER_DIRECTION_BUY,
                    quantity=int(quantity),
                    order_type=ORDER_TYPE_MARKET,
                    account_id=self.account_id,
                )
            except Exception as e:
                logger.error(f"Failed to post buy order. figi={self.figi}. {e}")
                return
            await asyncio.create_task(
                self.stats_handler.handle_new_order(
                    order_id=posted_order.order_id, account_id=self.account_id
                )
            )
            self.total_price = last_price*quantity_to_buy
            self.total_quantity = quantity_to_buy

    async def validate_stop_loss(self, last_price: float) -> None:
        """
        Check if stop loss is reached. If yes, then sells all the shares.
        :param last_price: Last price of the instrument.
        """
        positions = (await client.get_portfolio(account_id=self.account_id)).positions
        position = get_position(positions, self.figi)
        if position is None or quotation_to_float(position.quantity) == 0:
            return
        position_price = quotation_to_float(position.average_position_price)
        if last_price <= position_price - position_price * self.config.stop_loss_percent:
            logger.info(f"Stop loss triggered. Last price={last_price} figi={self.figi}")
            try:
                quantity = int(quotation_to_float(position.quantity)) / self.instrument_info.lot
                if not is_quantity_valid(quantity):
                    raise ValueError(f"Invalid quantity for posting an order. quantity={quantity}")
                posted_order = await client.post_order(
                    order_id=str(uuid4()),
                    figi=self.figi,
                    direction=ORDER_DIRECTION_SELL,
                    quantity=int(quantity),
                    order_type=ORDER_TYPE_MARKET,
                    account_id=self.account_id,
                )
            except Exception as e:
                logger.error(f"Failed to post sell order. figi={self.figi}. {e}")
                return
            await asyncio.create_task(
                self.stats_handler.handle_new_order(
                    order_id=posted_order.order_id, account_id=self.account_id
                )
            )
            self.total_price = 0
            self.total_quantity = 0
        return

    async def main_cycle(self):
        await self.prepare_data()
        logger.info(
            f"Starting Bollinger Bands strategy for figi {self.figi} "
            f"({self.instrument_info.name} {self.instrument_info.currency}) lot size is {self.instrument_info.lot}. "
            f"Configuration is: {self.config}"
        )
        while True:
            try:
                await self.ensure_market_open()
                await self.update_bollinger_bands()

                orders = await client.get_orders(account_id=self.account_id)
                if get_order(orders=orders.orders, figi=self.figi):
                    logger.info(f"There are orders in progress. Waiting. figi={self.figi}")
                    continue

                last_price = await self.get_last_price()
                to_buy = self.corridor.bottom
                to_sell = self.corridor.top
                profit = to_sell*self.total_quantity - self.total_price - self.total_price*0.003 - to_sell*self.total_quantity*0.003 - (to_sell*self.total_quantity - self.total_price) * 0.13
                price_to_sell = self.total_price*1.007
                logger.debug(f"\nLast price: {last_price}, figi={self.figi}\n"
                             f"Top band: {to_sell} ({round((to_sell - last_price) / to_sell * 100, 2)}%)\n"
                             f"Bottom band: {to_buy} ({round((last_price - to_buy) / last_price * 100, 2)}%)\n"
                             f"Potential profit: {profit}\n"
                             f"Total price: {self.total_price}\n"
                             f"Price to sell: {price_to_sell} ({round(0 if price_to_sell == 0 else (price_to_sell - last_price) / price_to_sell * 100, 2)}%)")

                await self.validate_stop_loss(last_price)

                if last_price >= self.corridor.top and profit > 0:
                    logger.debug(
                        f"Last price {last_price} is higher than top Bollinger Band "
                        f"{self.corridor.top}. figi={self.figi}"
                    )
                    await self.handle_bollinger_band_crossing_top(last_price=last_price)
                elif last_price <= self.corridor.bottom:
                    logger.debug(
                        f"Last price {last_price} is lower than bottom Bollinger Band "
                        f"{self.corridor.bottom}. figi={self.figi}"
                    )
                    await self.handle_bollinger_band_crossing_bottom(last_price=last_price)
            except AioRequestError as are:
                logger.error(f"Client error {are}")

            await asyncio.sleep(self.config.check_interval)

    async def start(self):
        if self.account_id is None:
            try:
                self.account_id = (await client.get_accounts()).accounts.pop().id
            except AioRequestError as are:
                logger.error(f"Error taking account id. Stopping strategy. {are}")
                return
        await self.main_cycle()
