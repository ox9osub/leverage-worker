"""
90pct_st1 (Limit-Order Dip Buy) 전략

매 1분봉 종가 기준 지정가 매수 전략.
ML 불필요 — 순수 rule-based, 매 봉마다 지정가 발령.

매매 로직:
1. 매분 이전 봉 종가 * (1 - discount) 가격에 지정가 매수 발령
2. 체결 시 scalping executor가 TP/SL/timeout 관리
3. trading_end(EOD cutoff) 이후 신규 진입 중단

대상: KODEX 레버리지 (122630)

백테스트 결과 (검증기간 2025-05-23 ~ 2026-02-13, 180거래일):
    - Strategy A: discount=0.1%, TP=0.1%, SL=0.5%, max=30min → 승률 97.49%, EV/day +0.45%
    - Strategy E: discount=0.2%, TP=0.2%, SL=1.0%, max=20min → 승률 96.65%, EV/day +0.80%

파라미터:
    buy_discount_pct  : 지정가 할인율 (기본 0.001 = 0.1%)
    sell_profit_pct   : 익절 비율 (기본 0.001 = 0.1%)
    stop_loss_pct     : 손절 비율 (기본 0.005 = 0.5%)  — ScalpingConfig 경유
    timeout_seconds   : 포지션 최대 보유 시간(초) (기본 1800 = 30분)
    buy_timeout_seconds  : 매수 미체결 취소(초) (기본 60 = 1봉)  — ScalpingConfig 경유
    sell_timeout_seconds : 매도 미체결 타임아웃(초) (기본 1800)  — ScalpingConfig 경유
    trading_start     : 신규 진입 시작 시간 (기본 "09:00")
    trading_end       : 신규 진입 마감 시간 / EOD cutoff (기본 "14:49")
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from leverage_worker.scalping.executor import round_to_tick_size
from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy

logger = logging.getLogger(__name__)


@register_strategy("90pct_st1")
class Pct90St1Strategy(BaseStrategy):
    """
    90pct_st1 (Limit-Order Dip Buy) 전략

    매 1분봉 종가에서 할인된 가격으로 지정가 매수.
    체결 후 scalping executor가 TP/SL/timeout 청산 처리.
    """

    # 이전 봉 종가 1개만 있으면 충분 (price_history[-1].close_price)
    MIN_DATA_REQUIRED = 2

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        # 지정가 매수 할인율 (전봉 종가 기준)
        self._buy_discount_pct = self.get_param("buy_discount_pct", 0.001)   # 0.1%
        # 익절 비율 (매수가 기준)
        self._sell_profit_pct = self.get_param("sell_profit_pct", 0.001)     # 0.1%
        # 포지션 최대 보유 시간(초) — signal.metadata["timeout_seconds"] 로 전달
        self._timeout_seconds = self.get_param("timeout_seconds", 1800)      # 30분

        # 거래 시간 (문자열 "HH:MM" 형식)
        self._trading_start: str = self.get_param("trading_start", "09:00")
        self._trading_end: str = self.get_param("trading_end", "14:49")      # Strategy A EOD

        logger.info(
            f"[{name}] Pct90St1Strategy initialized: "
            f"discount={self._buy_discount_pct*100:.2f}%, "
            f"tp={self._sell_profit_pct*100:.2f}%, "
            f"timeout={self._timeout_seconds}s, "
            f"trading={self._trading_start}~{self._trading_end}"
        )

    def _is_trading_time(self, current_time: datetime) -> bool:
        """신규 진입 허용 시간대 확인"""
        time_str = current_time.strftime("%H:%M")
        return self._trading_start <= time_str <= self._trading_end

    def can_generate_signal(self, context: StrategyContext) -> bool:
        """시그널 생성 가능 여부 확인"""
        # 포지션 보유 중이면 executor 청산 대기 (항상 True)
        if context.has_position:
            return True

        # 미보유 시 거래 시간 확인
        if not self._is_trading_time(context.current_time):
            return False

        # 최소 데이터 확인
        if not context.has_sufficient_data(self.MIN_DATA_REQUIRED):
            return False

        return True

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """
        매매 시그널 생성

        - 포지션 보유 중: HOLD (scalping executor가 TP/SL/timeout 청산 처리)
        - 미보유 + 거래시간: BUY (지정가, 전봉 종가 기준)
        """
        stock_code = context.stock_code

        # 포지션 보유 중 — scalping executor가 청산 처리
        if context.has_position:
            return TradingSignal.hold(stock_code, "포지션 보유 중 - executor 청산 대기")

        # EOD cutoff 확인
        if not self._is_trading_time(context.current_time):
            return TradingSignal.hold(
                stock_code,
                f"신규 진입 시간 외 ({context.current_time.strftime('%H:%M')} / 마감 {self._trading_end})",
            )

        # 이전 봉 종가로 지정가 계산
        prev_close = int(context.price_history[-1].close_price)
        if prev_close <= 0:
            return TradingSignal.hold(stock_code, "이전 봉 종가 데이터 없음")

        buy_price = round_to_tick_size(
            int(prev_close * (1.0 - self._buy_discount_pct)), direction="down"
        )
        sell_price = round_to_tick_size(
            int(buy_price * (1.0 + self._sell_profit_pct)), direction="up"
        )

        logger.debug(
            f"[{stock_code}] BUY 시그널 | "
            f"prev_close={prev_close:,} | "
            f"buy={buy_price:,} (-{self._buy_discount_pct*100:.2f}%) | "
            f"sell={sell_price:,} (+{self._sell_profit_pct*100:.2f}%)"
        )

        # quantity=0: scalping executor가 allocation 기반으로 수량 결정
        signal = TradingSignal.buy(
            stock_code=stock_code,
            quantity=0,
            reason=(
                f"지정가 매수 | prev_close={prev_close:,} → "
                f"buy={buy_price:,} (-{self._buy_discount_pct*100:.2f}%)"
            ),
            confidence=1.0,
        )
        signal.metadata = {
            "limit_price": buy_price,
            "sell_price": sell_price,
            "timeout_seconds": self._timeout_seconds,
            "prev_close": prev_close,
        }
        return signal

    def on_entry(self, context: StrategyContext, signal: TradingSignal) -> None:
        """진입 완료 콜백"""
        buy_price = signal.metadata.get("limit_price", context.current_price)
        logger.info(
            f"[{self.name}] 진입: {context.stock_code} @ {buy_price:,}원 "
            f"| qty={signal.quantity}"
        )

    def on_exit(self, context: StrategyContext, signal: TradingSignal) -> None:
        """청산 완료 콜백"""
        logger.info(
            f"[{self.name}] 청산: {context.stock_code} @ {context.current_price:,}원 "
            f"| {signal.reason}"
        )
