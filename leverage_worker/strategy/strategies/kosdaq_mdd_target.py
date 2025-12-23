"""
코스닥150레버리지 MDD 타겟 전략 (MDD_Target_Business)

MDD(Maximum Drawdown) 타겟을 설정하여 리스크를 먼저 관리하는 전략.
ATR 기반 변동성 필터링으로 적절한 진입 시점 포착.

대상: KODEX 코스닥150레버리지 (233740)

진입 조건:
    1. ATR(14) / 현재가 범위: 0.2% ~ 4% (적정 변동성)
    2. 현재 종가 > 5봉 전 종가 (상승 추세)

청산 조건:
    - 익절: +3.0%
    - 손절: -3.0%
    - 시간 청산: 8봉 이상 보유

백테스트 결과 (133일):
    - 복리 수익률: 5.02%
    - 연환산 수익률: 약 14%
    - 거래 횟수: 10회
    - 승률: 60.0%
    - 평균 보유: 2.1일
    - MDD: 6.25%
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

    recent_trs = true_ranges[-period:]
    return sum(recent_trs) / period


@register_strategy("kosdaq_mdd_target")
class KosdaqMDDTargetStrategy(BaseStrategy):
    """
    코스닥150레버리지 MDD 타겟 전략 (MDD_Target_Business)

    파라미터:
        atr_period: ATR 기간 (기본 14)
        min_volatility: 최소 변동성 비율 (기본 0.002 = 0.2%)
        max_volatility: 최대 변동성 비율 (기본 0.04 = 4%)
        trend_period: 추세 확인 기간 (기본 5)
        take_profit_pct: 익절 비율 (기본 0.03 = 3%)
        stop_loss_pct: 손절 비율 (기본 0.03 = 3%)
        max_holding_period: 최대 보유 기간 (기본 8)
        position_size: 매수 수량 (기본 1)
    """

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        self._atr_period = self.get_param("atr_period", 14)
        self._min_volatility = self.get_param("min_volatility", 0.002)
        self._max_volatility = self.get_param("max_volatility", 0.04)
        self._trend_period = self.get_param("trend_period", 5)
        self._take_profit_pct = self.get_param("take_profit_pct", 0.03)
        self._stop_loss_pct = self.get_param("stop_loss_pct", 0.03)
        self._max_holding_period = self.get_param("max_holding_period", 8)
        self._position_size = self.get_param("position_size", 1)

        self._entry_bar_count = 0

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """
        시그널 생성

        진입 조건:
            - ATR 비율: 0.2% ~ 4% 범위 (적정 변동성)
            - 현재 종가 > 5봉 전 종가 (상승 추세)

        청산 조건:
            - 익절: +3.0%
            - 손절: -3.0%
            - 시간 청산: 8봉 이상 보유
        """
        stock_code = context.stock_code
        required_bars = max(self._atr_period + 1, self._trend_period + 1)

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
        current_price = context.current_price

        # 조건 1: ATR 변동성 확인
        history = context.price_history[-required_bars:]
        high_prices = [p.high_price for p in history]
        low_prices = [p.low_price for p in history]
        close_prices = [p.close_price for p in history]

        atr = calculate_atr(high_prices, low_prices, close_prices, self._atr_period)
        if atr is None:
            return TradingSignal.hold(stock_code, "ATR 계산 불가")

        atr_ratio = atr / current_price
        has_valid_volatility = self._min_volatility < atr_ratio < self._max_volatility

        # 조건 2: 상승 추세 확인
        prices = context.get_recent_prices(self._trend_period + 1)
        price_trend_ago = prices[-(self._trend_period + 1)]
        is_uptrend = current_price > price_trend_ago

        if has_valid_volatility and is_uptrend:
            self._entry_bar_count = 0
            trend_pct = (current_price - price_trend_ago) / price_trend_ago * 100
            return TradingSignal.buy(
                stock_code=stock_code,
                quantity=self._position_size,
                reason=f"MDD 타겟: ATR {atr_ratio:.2%}, 상승 +{trend_pct:.1f}%",
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
