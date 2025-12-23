"""
피보나치 5일 전략 (E_Fib5_Lucky7_TP4)

피보나치 수열(5일)과 행운의 숫자(7)를 결합한 비전통적 전략.
5일 상승 + 거래량 증가 시 진입하는 단순하지만 효과적인 전략.

대상: KODEX 레버리지 (122630)

진입 조건:
    1. 현재 종가 > 5봉 전 종가 (5봉 상승)
    2. 현재 거래량 > 전봉 거래량 (거래량 증가)

청산 조건:
    - 익절: +4.0%
    - 손절: -2.0%
    - 시간 청산: 5봉 (피보나치 5일)

백테스트 결과:
    - 총 수익률: 92.7%
    - 승률: 63.2%
    - MDD: 4.7%
"""

from typing import Any, Dict, Optional

from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@register_strategy("fibonacci_lucky")
class FibonacciLuckyStrategy(BaseStrategy):
    """
    피보나치 5일 전략 (E_Fib5_Lucky7_TP4)

    파라미터:
        fib_period: 피보나치 비교 기간 (기본 5)
        take_profit_pct: 익절 비율 (기본 0.04 = 4%)
        stop_loss_pct: 손절 비율 (기본 0.02 = 2%)
        max_holding_period: 최대 보유 기간 (기본 5, 피보나치 수)
        position_size: 매수 수량 (기본 1)
    """

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        self._fib_period = self.get_param("fib_period", 5)
        self._take_profit_pct = self.get_param("take_profit_pct", 0.04)
        self._stop_loss_pct = self.get_param("stop_loss_pct", 0.02)
        self._max_holding_period = self.get_param("max_holding_period", 5)
        self._position_size = self.get_param("position_size", 1)

        self._entry_bar_count = 0

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """
        시그널 생성

        진입 조건:
            - 현재 종가 > 5봉 전 종가 (상승 추세)
            - 현재 거래량 > 전봉 거래량 (거래량 증가)

        청산 조건:
            - 익절: +4.0%
            - 손절: -2.0%
            - 시간 청산: 5봉 보유
        """
        stock_code = context.stock_code
        required_bars = self._fib_period + 1

        if len(context.price_history) < required_bars:
            return TradingSignal.hold(stock_code, "Insufficient data")

        # 포지션 보유 시 청산 조건 확인
        if context.has_position:
            profit_rate = context.profit_rate / 100

            # 손절
            if profit_rate <= -self._stop_loss_pct:
                self._entry_bar_count = 0
                return TradingSignal.sell(
                    stock_code=stock_code,
                    quantity=context.position_quantity,
                    reason=f"손절: {profit_rate:.2%}",
                    confidence=1.0,
                )

            # 익절
            if profit_rate >= self._take_profit_pct:
                self._entry_bar_count = 0
                return TradingSignal.sell(
                    stock_code=stock_code,
                    quantity=context.position_quantity,
                    reason=f"익절: {profit_rate:.2%}",
                    confidence=1.0,
                )

            # 시간 청산 (피보나치 5일)
            self._entry_bar_count += 1
            if self._entry_bar_count >= self._max_holding_period:
                self._entry_bar_count = 0
                return TradingSignal.sell(
                    stock_code=stock_code,
                    quantity=context.position_quantity,
                    reason=f"피보나치 시간 청산: {self._entry_bar_count}봉 보유",
                    confidence=0.8,
                )

            return TradingSignal.hold(stock_code, "보유 중")

        # 미보유 시 진입 조건 확인
        prices = context.get_recent_prices(required_bars)
        volumes = context.get_recent_volumes(required_bars)

        current_price = context.current_price
        price_fib_ago = prices[-(self._fib_period + 1)]

        current_volume = volumes[-1]
        prev_volume = volumes[-2]

        # 조건 1: 5봉 전 대비 상승
        is_uptrend = current_price > price_fib_ago

        # 조건 2: 거래량 증가
        has_volume_increase = current_volume > prev_volume

        if is_uptrend and has_volume_increase:
            self._entry_bar_count = 0
            change_pct = (current_price - price_fib_ago) / price_fib_ago * 100
            vol_change_pct = (current_volume - prev_volume) / prev_volume * 100 if prev_volume > 0 else 0
            return TradingSignal.buy(
                stock_code=stock_code,
                quantity=self._position_size,
                reason=f"피보나치 상승 +{change_pct:.1f}%, 거래량 +{vol_change_pct:.0f}%",
                confidence=0.85,
            )

        return TradingSignal.hold(stock_code, "진입 조건 미충족")

    def on_entry(self, context: StrategyContext, signal: TradingSignal) -> None:
        logger.info(
            f"[{self.name}] 진입: {context.stock_code} @ {context.current_price:,} - {signal.reason}"
        )
        self._entry_bar_count = 0

    def on_exit(self, context: StrategyContext, signal: TradingSignal) -> None:
        logger.info(
            f"[{self.name}] 청산: {context.stock_code} @ {context.current_price:,} - {signal.reason}"
        )
