"""
볼린저 밴드 전략 (C_BB_P15_Std1.5_TP3)

볼린저 밴드 하단 터치 시 매수하는 평균회귀 전략.
가장 안정적인 전략으로 높은 승률과 낮은 MDD가 특징.

대상: KODEX 레버리지 (122630)

진입 조건:
    - 현재 종가 < 볼린저 밴드 하단 (15일, 1.5 표준편차)

청산 조건:
    - 익절: +3.0%
    - 손절: -1.5%
    - 시간 청산: 10봉 이상 보유

백테스트 결과:
    - 총 수익률: 34.3%
    - 승률: 85.4% (가장 높음)
    - MDD: 1.6% (가장 낮음)
"""

import math
from typing import Any, Dict, List, Optional, Tuple

from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


def calculate_bollinger_bands(
    prices: List[float], period: int = 15, std_multiplier: float = 1.5
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    볼린저 밴드 계산

    Args:
        prices: 가격 리스트 (최근 데이터가 마지막)
        period: 이동평균 기간
        std_multiplier: 표준편차 배수

    Returns:
        (상단 밴드, 중간 밴드, 하단 밴드) 또는 None
    """
    if len(prices) < period:
        return None, None, None

    recent_prices = prices[-period:]
    mean = sum(recent_prices) / period

    variance = sum((p - mean) ** 2 for p in recent_prices) / period
    std = math.sqrt(variance) if variance > 0 else 0

    upper_band = mean + (std_multiplier * std)
    lower_band = mean - (std_multiplier * std)

    return upper_band, mean, lower_band


@register_strategy("bollinger_band")
class BollingerBandStrategy(BaseStrategy):
    """
    볼린저 밴드 전략 (C_BB_P15_Std1.5_TP3)

    파라미터:
        bb_period: 볼린저 밴드 기간 (기본 15)
        std_multiplier: 표준편차 배수 (기본 1.5)
        take_profit_pct: 익절 비율 (기본 0.03 = 3%)
        stop_loss_pct: 손절 비율 (기본 0.015 = 1.5%)
        max_holding_period: 최대 보유 기간 (기본 10)
        position_size: 매수 수량 (기본 1)
    """

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        self._bb_period = self.get_param("bb_period", 15)
        self._std_multiplier = self.get_param("std_multiplier", 1.5)
        self._take_profit_pct = self.get_param("take_profit_pct", 0.03)
        self._stop_loss_pct = self.get_param("stop_loss_pct", 0.015)
        self._max_holding_period = self.get_param("max_holding_period", 10)
        self._position_size = self.get_param("position_size", 1)

        self._entry_bar_count = 0

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """
        시그널 생성

        진입 조건:
            - 현재 종가 < 볼린저 밴드 하단

        청산 조건:
            - 익절: +3.0%
            - 손절: -1.5%
            - 시간 청산: 10봉 이상 보유
        """
        stock_code = context.stock_code

        if len(context.price_history) < self._bb_period:
            return TradingSignal.hold(stock_code, "Insufficient data")

        prices = context.get_recent_prices(self._bb_period)

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
        upper_band, middle_band, lower_band = calculate_bollinger_bands(
            prices, self._bb_period, self._std_multiplier
        )

        if lower_band is None:
            return TradingSignal.hold(stock_code, "볼린저 밴드 계산 불가")

        current_price = context.current_price

        # 진입 조건: 하단 밴드 터치 (과매도)
        if current_price < lower_band:
            self._entry_bar_count = 0
            distance_pct = (lower_band - current_price) / lower_band * 100
            return TradingSignal.buy(
                stock_code=stock_code,
                quantity=self._position_size,
                reason=f"볼린저 하단 돌파 (하단: {lower_band:,.0f}, 괴리: {distance_pct:.1f}%)",
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
