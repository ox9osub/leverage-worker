"""
예시 전략 모듈

간단한 이동평균 기반 전략 예시
"""

from typing import Any, Dict, Optional

from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@register_strategy("example_strategy")
class ExampleStrategy(BaseStrategy):
    """
    예시 전략: 단순 이동평균 크로스

    파라미터:
        short_period: 단기 이동평균 기간 (기본 5)
        long_period: 장기 이동평균 기간 (기본 20)
        stop_loss_pct: 손절 비율 (기본 0.03 = 3%)
        take_profit_pct: 익절 비율 (기본 0.05 = 5%)
        position_size: 매수 수량 (기본 1)
    """

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        # 기본 파라미터
        self._short_period = self.get_param("short_period", 5)
        self._long_period = self.get_param("long_period", 20)
        self._stop_loss_pct = self.get_param("stop_loss_pct", 0.03)
        self._take_profit_pct = self.get_param("take_profit_pct", 0.05)
        self._position_size = self.get_param("position_size", 1)

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """
        시그널 생성

        진입 조건:
            - 미보유 상태에서 단기 MA가 장기 MA를 상향 돌파

        청산 조건:
            - 손절: 수익률이 -stop_loss_pct 이하
            - 익절: 수익률이 +take_profit_pct 이상
            - 추세 반전: 단기 MA가 장기 MA를 하향 돌파
        """
        stock_code = context.stock_code

        # 이동평균 계산
        short_ma = context.get_sma(self._short_period)
        long_ma = context.get_sma(self._long_period)

        if short_ma is None or long_ma is None:
            logger.debug(f"[{stock_code}] Not enough data for MA calculation")
            return TradingSignal.hold(stock_code, "Insufficient data")

        # 현재 포지션 보유 시 청산 조건 확인
        if context.has_position:
            profit_rate = context.profit_rate / 100  # % → 소수

            # 손절
            if profit_rate <= -self._stop_loss_pct:
                return TradingSignal.sell(
                    stock_code=stock_code,
                    quantity=context.position_quantity,
                    reason=f"Stop loss triggered: {profit_rate:.2%}",
                    confidence=1.0,
                )

            # 익절
            if profit_rate >= self._take_profit_pct:
                return TradingSignal.sell(
                    stock_code=stock_code,
                    quantity=context.position_quantity,
                    reason=f"Take profit triggered: {profit_rate:.2%}",
                    confidence=1.0,
                )

            # 추세 반전 (단기 < 장기)
            if short_ma < long_ma:
                return TradingSignal.sell(
                    stock_code=stock_code,
                    quantity=context.position_quantity,
                    reason=f"Trend reversal: short_ma={short_ma:.0f} < long_ma={long_ma:.0f}",
                    confidence=0.8,
                )

            return TradingSignal.hold(stock_code, "Holding position")

        # 미보유 시 진입 조건 확인
        else:
            # 골든 크로스 (단기 > 장기)
            if short_ma > long_ma:
                # 이전 상태 확인 (크로스 발생 여부)
                if len(context.price_history) >= self._long_period + 1:
                    prev_short_ma = self._calculate_prev_sma(context, self._short_period)
                    prev_long_ma = self._calculate_prev_sma(context, self._long_period)

                    if prev_short_ma and prev_long_ma and prev_short_ma <= prev_long_ma:
                        return TradingSignal.buy(
                            stock_code=stock_code,
                            quantity=self._position_size,
                            reason=f"Golden cross: short_ma={short_ma:.0f} > long_ma={long_ma:.0f}",
                            confidence=0.9,
                        )

            return TradingSignal.hold(stock_code, "Waiting for entry signal")

    def _calculate_prev_sma(self, context: StrategyContext, period: int) -> Optional[float]:
        """이전 시점의 SMA 계산 (1분봉 전)"""
        if len(context.price_history) < period + 1:
            return None

        # 마지막 1개를 제외한 N개의 평균
        prices = [p.close_price for p in context.price_history[-(period + 1):-1]]
        if len(prices) < period:
            return None

        return sum(prices) / len(prices)

    def on_entry(self, context: StrategyContext, signal: TradingSignal) -> None:
        logger.info(
            f"[{self.name}] Entry signal: {context.stock_code} "
            f"@ {context.current_price} - {signal.reason}"
        )

    def on_exit(self, context: StrategyContext, signal: TradingSignal) -> None:
        logger.info(
            f"[{self.name}] Exit signal: {context.stock_code} "
            f"@ {context.current_price} - {signal.reason}"
        )


@register_strategy("simple_momentum")
class SimpleMomentumStrategy(BaseStrategy):
    """
    단순 모멘텀 전략

    파라미터:
        lookback: 비교 기간 (기본 10분)
        threshold_pct: 상승 임계값 (기본 0.01 = 1%)
        stop_loss_pct: 손절 비율 (기본 0.02 = 2%)
        take_profit_pct: 익절 비율 (기본 0.03 = 3%)
        position_size: 매수 수량 (기본 1)
    """

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        self._lookback = self.get_param("lookback", 10)
        self._threshold_pct = self.get_param("threshold_pct", 0.01)
        self._stop_loss_pct = self.get_param("stop_loss_pct", 0.02)
        self._take_profit_pct = self.get_param("take_profit_pct", 0.03)
        self._position_size = self.get_param("position_size", 1)

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """
        시그널 생성

        진입 조건:
            - 현재가가 lookback분 전 대비 threshold_pct 이상 상승

        청산 조건:
            - 손절/익절
        """
        stock_code = context.stock_code

        if len(context.price_history) < self._lookback:
            return TradingSignal.hold(stock_code, "Insufficient data")

        # 포지션 보유 시
        if context.has_position:
            profit_rate = context.profit_rate / 100

            if profit_rate <= -self._stop_loss_pct:
                return TradingSignal.sell(
                    stock_code=stock_code,
                    quantity=context.position_quantity,
                    reason=f"Stop loss: {profit_rate:.2%}",
                )

            if profit_rate >= self._take_profit_pct:
                return TradingSignal.sell(
                    stock_code=stock_code,
                    quantity=context.position_quantity,
                    reason=f"Take profit: {profit_rate:.2%}",
                )

            return TradingSignal.hold(stock_code, "Holding")

        # 미보유 시: 모멘텀 확인
        past_price = context.price_history[-self._lookback].close_price
        current_price = context.current_price

        if past_price > 0:
            change_rate = (current_price - past_price) / past_price

            if change_rate >= self._threshold_pct:
                return TradingSignal.buy(
                    stock_code=stock_code,
                    quantity=self._position_size,
                    reason=f"Momentum up: {change_rate:.2%}",
                )

        return TradingSignal.hold(stock_code, "No momentum")
