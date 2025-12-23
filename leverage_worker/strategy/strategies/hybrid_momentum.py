"""
하이브리드 모멘텀 전략 (S_Hybrid_Mom10_MR15_MT3_ZS1.0_TP3)

모멘텀과 평균회귀의 하이브리드 전략.
단기 상승 추세가 있으면서 극단적으로 과매도 상태가 아닌 시점에 진입.

대상: KODEX 레버리지 (122630)

진입 조건:
    1. 10분(또는 10봉) 전 대비 3% 이상 상승 (모멘텀)
    2. Z-Score > -1.0 (극단적 과매도 아님)

청산 조건:
    - 익절: +3.5%
    - 손절: -2.0%
    - 시간 청산: 10봉 이상 보유

백테스트 결과:
    - 총 수익률: 113.3%
    - 승률: 61.8%
    - MDD: 4.1%
"""

import math
from typing import Any, Dict, List, Optional

from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


def calculate_zscore(prices: List[float], period: int = 15) -> Optional[float]:
    """
    Z-Score 계산

    원본 스펙:
        mean_15 = (close[i-15] + close[i-14] + ... + close[i-1]) / 15  # 현재가 제외
        std_15 = sqrt(sum((close[i-j] - mean_15)^2 for j in 1..15) / 15)
        zscore = (close[i] - mean_15) / std_15

    Args:
        prices: 가격 리스트 (최근 데이터가 마지막, 현재가 포함)
        period: 계산 기간

    Returns:
        Z-Score 값 또는 None (데이터 부족 시)
    """
    # period + 1 개의 데이터 필요 (현재가 + 과거 period개)
    if len(prices) < period + 1:
        return None

    current_price = prices[-1]
    # 현재가 제외, 과거 period개 가격으로 평균/표준편차 계산
    past_prices = prices[-(period + 1):-1]
    mean = sum(past_prices) / period

    variance = sum((p - mean) ** 2 for p in past_prices) / period
    std = math.sqrt(variance) if variance > 0 else 0

    if std == 0:
        return 0.0

    return (current_price - mean) / std


@register_strategy("hybrid_momentum")
class HybridMomentumStrategy(BaseStrategy):
    """
    하이브리드 모멘텀 전략 (S_Hybrid_Mom10_MR15_MT3_ZS1.0_TP3)

    파라미터:
        momentum_period: 모멘텀 비교 기간 (기본 10)
        momentum_threshold: 모멘텀 임계값 (기본 0.03 = 3%)
        zscore_period: Z-Score 계산 기간 (기본 15)
        zscore_threshold: Z-Score 하한 임계값 (기본 -1.0)
        take_profit_pct: 익절 비율 (기본 0.035 = 3.5%)
        stop_loss_pct: 손절 비율 (기본 0.02 = 2%)
        max_holding_period: 최대 보유 기간 (기본 10)
        position_size: 매수 수량 (기본 1)
    """

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        self._momentum_period = self.get_param("momentum_period", 10)
        self._momentum_threshold = self.get_param("momentum_threshold", 0.03)
        self._zscore_period = self.get_param("zscore_period", 15)
        self._zscore_threshold = self.get_param("zscore_threshold", -1.0)
        self._take_profit_pct = self.get_param("take_profit_pct", 0.035)
        self._stop_loss_pct = self.get_param("stop_loss_pct", 0.02)
        self._max_holding_period = self.get_param("max_holding_period", 10)
        self._position_size = self.get_param("position_size", 1)

        # 진입 시점 추적 (시간 청산용)
        self._entry_bar_count = 0

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """
        시그널 생성

        진입 조건:
            - 현재가 > 10봉 전 가격 × 1.03 (모멘텀)
            - Z-Score(15) > -1.0 (과매도 아님)

        청산 조건:
            - 익절: +3.5%
            - 손절: -2.0%
            - 시간 청산: 10봉 이상 보유
        """
        stock_code = context.stock_code
        required_bars = max(self._momentum_period, self._zscore_period) + 1

        if len(context.price_history) < required_bars:
            return TradingSignal.hold(stock_code, "Insufficient data")

        prices = context.get_recent_prices(required_bars)

        # 포지션 보유 시 청산 조건 확인
        if context.has_position:
            profit_rate = context.profit_rate / 100  # % → 소수

            # 손절 (우선 확인)
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
        current_price = context.current_price
        price_n_bars_ago = prices[-(self._momentum_period + 1)]

        # 조건 1: 모멘텀 확인 (N봉 전 대비 threshold 이상 상승)
        momentum_target = price_n_bars_ago * (1 + self._momentum_threshold)
        has_momentum = current_price > momentum_target

        # 조건 2: Z-Score 확인 (과매도 아님)
        zscore = calculate_zscore(prices, self._zscore_period)
        if zscore is None:
            return TradingSignal.hold(stock_code, "Z-Score 계산 불가")

        is_not_oversold = zscore > self._zscore_threshold

        # 진입
        if has_momentum and is_not_oversold:
            self._entry_bar_count = 0
            momentum_pct = (current_price - price_n_bars_ago) / price_n_bars_ago * 100
            return TradingSignal.buy(
                stock_code=stock_code,
                quantity=self._position_size,
                reason=f"모멘텀 +{momentum_pct:.1f}%, Z-Score {zscore:.2f}",
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
