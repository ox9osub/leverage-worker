"""
수수료 최적화 전략 (B_FeeOpt_MinExp2_WR50_TP4)

수수료를 고려한 최소 기대수익 확보 전략.
20일 이동평균 상향 돌파 + ATR 기반 변동성 필터로 수익 기회 포착.

대상: KODEX 레버리지 (122630)

진입 조건:
    1. 현재 종가 > 20일 이동평균 (추세 상향)
    2. ATR(14) / 현재가 > 0.2% (최소 변동성 확보)

청산 조건:
    - 익절: +4.0%
    - 손절: -2.0%
    - 시간 청산: 10봉 이상 보유

백테스트 결과:
    - 총 수익률: 98.2%
    - 승률: 55.6%
    - MDD: 13.6% (주의: 높은 MDD)
"""

from typing import Any, Dict, List, Optional

from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


def calculate_atr(
    high_prices: List[float],
    low_prices: List[float],
    close_prices: List[float],
    period: int = 14,
) -> Optional[float]:
    """
    ATR (Average True Range) 계산

    Args:
        high_prices: 고가 리스트
        low_prices: 저가 리스트
        close_prices: 종가 리스트
        period: ATR 기간

    Returns:
        ATR 값 또는 None
    """
    if len(high_prices) < period + 1:
        return None

    true_ranges = []

    for i in range(1, len(high_prices)):
        high = high_prices[i]
        low = low_prices[i]
        prev_close = close_prices[i - 1]

        tr1 = high - low
        tr2 = abs(high - prev_close)
        tr3 = abs(low - prev_close)

        true_ranges.append(max(tr1, tr2, tr3))

    if len(true_ranges) < period:
        return None

    # 최근 N개의 TR 평균
    recent_trs = true_ranges[-period:]
    return sum(recent_trs) / period


@register_strategy("fee_optimized")
class FeeOptimizedStrategy(BaseStrategy):
    """
    수수료 최적화 전략 (B_FeeOpt_MinExp2_WR50_TP4)

    파라미터:
        sma_period: 이동평균 기간 (기본 20)
        atr_period: ATR 기간 (기본 14)
        min_volatility: 최소 변동성 비율 (기본 0.002 = 0.2%)
        take_profit_pct: 익절 비율 (기본 0.04 = 4%)
        stop_loss_pct: 손절 비율 (기본 0.02 = 2%)
        max_holding_period: 최대 보유 기간 (기본 10)
        position_size: 매수 수량 (기본 1)
    """

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        self._sma_period = self.get_param("sma_period", 20)
        self._atr_period = self.get_param("atr_period", 14)
        self._min_volatility = self.get_param("min_volatility", 0.002)
        self._take_profit_pct = self.get_param("take_profit_pct", 0.04)
        self._stop_loss_pct = self.get_param("stop_loss_pct", 0.02)
        self._max_holding_period = self.get_param("max_holding_period", 10)
        self._position_size = self.get_param("position_size", 1)

        self._entry_bar_count = 0

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """
        시그널 생성

        진입 조건:
            - 현재 종가 > 20일 SMA (추세 상향)
            - ATR 비율 > 0.2% (최소 변동성)

        청산 조건:
            - 익절: +4.0%
            - 손절: -2.0%
            - 시간 청산: 10봉 이상 보유
        """
        stock_code = context.stock_code
        required_bars = max(self._sma_period, self._atr_period + 1)

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
        # 조건 1: 20일 SMA 상향
        sma = context.get_sma(self._sma_period)
        if sma is None:
            return TradingSignal.hold(stock_code, "SMA 계산 불가")

        current_price = context.current_price
        is_above_sma = current_price > sma

        # 조건 2: ATR 변동성 확인
        history = context.price_history[-required_bars:]
        high_prices = [p.high_price for p in history]
        low_prices = [p.low_price for p in history]
        close_prices = [p.close_price for p in history]

        atr = calculate_atr(high_prices, low_prices, close_prices, self._atr_period)
        if atr is None:
            return TradingSignal.hold(stock_code, "ATR 계산 불가")

        atr_ratio = atr / current_price
        has_min_volatility = atr_ratio > self._min_volatility

        if is_above_sma and has_min_volatility:
            self._entry_bar_count = 0
            return TradingSignal.buy(
                stock_code=stock_code,
                quantity=self._position_size,
                reason=f"SMA 상향, ATR 비율 {atr_ratio:.3%}",
                confidence=0.8,
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
