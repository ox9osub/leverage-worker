"""
5일 신고가 돌파 전략 (A_Breakout_High5_Vol1.0_TP5)

5일 신고가 돌파 시 매수하는 단순하지만 효과적인 추세 추종 전략.
거래량 확인으로 신뢰도 향상.

대상: KODEX 레버리지 (122630)

진입 조건:
    1. 현재 종가 > 지난 5봉 최고가
    2. 현재 거래량 >= 5봉 평균 거래량

청산 조건:
    - 익절: +5.0%
    - 손절: -2.0%
    - 시간 청산: 10봉 이상 보유

백테스트 결과:
    - 총 수익률: 86.1%
    - 승률: 78.7%
    - MDD: 2.1%
"""

from typing import Any, Dict, List, Optional

from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@register_strategy("breakout_high5")
class BreakoutHigh5Strategy(BaseStrategy):
    """
    5일 신고가 돌파 전략 (A_Breakout_High5_Vol1.0_TP5)

    파라미터:
        lookback_period: 신고가 확인 기간 (기본 5)
        volume_multiplier: 평균 거래량 대비 배수 (기본 1.0)
        take_profit_pct: 익절 비율 (기본 0.05 = 5%)
        stop_loss_pct: 손절 비율 (기본 0.02 = 2%)
        max_holding_period: 최대 보유 기간 (기본 10)
        position_size: 매수 수량 (기본 1)
    """

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        self._lookback_period = self.get_param("lookback_period", 5)
        self._volume_multiplier = self.get_param("volume_multiplier", 1.0)
        self._take_profit_pct = self.get_param("take_profit_pct", 0.05)
        self._stop_loss_pct = self.get_param("stop_loss_pct", 0.02)
        self._max_holding_period = self.get_param("max_holding_period", 10)
        self._position_size = self.get_param("position_size", 1)

        self._entry_bar_count = 0

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """
        시그널 생성

        진입 조건:
            - 현재 종가 > 5봉 최고가 (신고가 돌파)
            - 현재 거래량 >= 5봉 평균 거래량 (거래량 확인)

        청산 조건:
            - 익절: +5.0%
            - 손절: -2.0%
            - 시간 청산: 10봉 이상 보유
        """
        stock_code = context.stock_code
        required_bars = self._lookback_period + 1

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

            # 시간 청산
            self._entry_bar_count += 1
            if self._entry_bar_count >= self._max_holding_period:
                self._entry_bar_count = 0
                return TradingSignal.sell(
                    stock_code=stock_code,
                    quantity=context.position_quantity,
                    reason=f"시간 청산: {self._entry_bar_count}봉 보유",
                    confidence=0.8,
                )

            return TradingSignal.hold(stock_code, "보유 중")

        # 미보유 시 진입 조건 확인
        # 최근 N봉의 고가 (현재 봉 제외)
        high_prices = [
            p.high_price
            for p in context.price_history[-(self._lookback_period + 1) : -1]
        ]
        high_n_bars = max(high_prices) if high_prices else 0

        # 최근 N봉의 평균 거래량 (현재 봉 제외)
        volumes = context.get_recent_volumes(self._lookback_period + 1)[:-1]
        avg_volume = sum(volumes) / len(volumes) if volumes else 0

        current_price = context.current_price
        current_volume = context.price_history[-1].volume if context.price_history else 0

        # 조건 1: 신고가 돌파
        is_breakout = current_price > high_n_bars

        # 조건 2: 거래량 확인
        volume_threshold = avg_volume * self._volume_multiplier
        has_volume = current_volume >= volume_threshold

        if is_breakout and has_volume:
            self._entry_bar_count = 0
            return TradingSignal.buy(
                stock_code=stock_code,
                quantity=self._position_size,
                reason=f"{self._lookback_period}봉 신고가 돌파 (고가: {high_n_bars:,.0f})",
                confidence=0.9,
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
