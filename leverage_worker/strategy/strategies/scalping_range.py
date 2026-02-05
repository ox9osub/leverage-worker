"""
스캘핑 레인지 전략

매분 분봉을 분석하여 매수 시그널을 생성.
실제 매수/매도 실행은 ScalpingExecutor가 WebSocket 기반으로 처리.

파라미터:
    trading_start: 작동 시작 시간 (기본 "09:05")
    trading_end: 작동 종료 시간 (기본 "15:15")
    timeframe_minutes: 타임프레임 분 단위 (기본 3)
    candle_elapsed_seconds: 봉 시작 후 경과 시간, -1이면 즉시 (기본 -1)
    dip_threshold_pct: 시가 대비 하락폭 % (기본 0.2)
    position_size: 매수 수량 (기본 1)

    # 스캘핑 실행 파라미터 (ScalpingExecutor로 전달)
    window_seconds: 변동폭 윈도우 (기본 10)
    sell_profit_pct: 매도 수익 목표 (기본 0.001 = 0.1%)
    stop_loss_pct: 시그널 SL (기본 0.01 = 1%)
    take_profit_pct: 시그널 TP (기본 0.003 = 0.3%)
    max_signal_minutes: 시그널 유효 시간 (기본 60)
"""

from dataclasses import dataclass
from datetime import datetime, time
from typing import Any, Dict, List, Optional

from leverage_worker.data.minute_candle_repository import MinuteCandle as OHLCV
from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy
from leverage_worker.utils.logger import get_logger

logger = get_logger("strategy.scalping_range")


@dataclass
class Candle:
    """N분봉 집계 데이터"""

    start_time: datetime
    open_price: int
    high_price: int
    low_price: int
    close_price: int
    volume: int


@register_strategy("scalping_range")
class ScalpingRangeStrategy(BaseStrategy):
    """
    스캘핑 레인지 전략 - 시그널 생성부

    N분봉 시가 대비 일정 하락 시 매수 시그널을 생성.
    실제 매수/매도는 ScalpingExecutor가 처리하므로,
    이 전략은 시그널만 생성하고 청산 로직은 포함하지 않음.
    """

    MIN_DATA_REQUIRED = 1

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(name, params)

        # 작동 시간대
        self._trading_start = self._parse_time(
            self.get_param("trading_start", "09:05")
        )
        self._trading_end = self._parse_time(self.get_param("trading_end", "15:15"))

        # 타임프레임 설정
        self._timeframe_minutes: int = self.get_param("timeframe_minutes", 3)
        self._candle_elapsed_seconds: int = self.get_param("candle_elapsed_seconds", -1)

        # 진입 조건
        self._dip_threshold_pct: float = self.get_param("dip_threshold_pct", 0.2) / 100

        # 포지션 설정
        self._position_size: int = self.get_param("position_size", 1)

        # 스캘핑 실행 파라미터 (metadata로 전달)
        self._sell_profit_pct: float = self.get_param("sell_profit_pct", 0.001)
        self._stop_loss_pct: float = self.get_param("stop_loss_pct", 0.01)
        self._take_profit_pct: float = self.get_param("take_profit_pct", 0.003)
        self._max_signal_minutes: int = self.get_param("max_signal_minutes", 60)

        logger.info(
            f"[{name}] ScalpingRangeStrategy initialized: "
            f"timeframe={self._timeframe_minutes}min, "
            f"dip={self._dip_threshold_pct*100:.2f}%, "
            f"sell_target={self._sell_profit_pct*100:.1f}%, "
            f"SL={self._stop_loss_pct*100:.1f}%, "
            f"TP={self._take_profit_pct*100:.1f}%, "
            f"timeout={self._max_signal_minutes}min"
        )

    def _parse_time(self, time_str: str) -> time:
        """시간 문자열 파싱 (HH:MM)"""
        parts = time_str.split(":")
        return time(int(parts[0]), int(parts[1]))

    def _is_trading_time(self, current_time: datetime) -> bool:
        """작동 시간대 확인"""
        current = current_time.time()
        return self._trading_start <= current <= self._trading_end

    def _get_candle_start(self, current_time: datetime) -> datetime:
        """현재 시간이 속한 N분봉 시작 시간 계산"""
        minute = current_time.minute
        candle_minute = (minute // self._timeframe_minutes) * self._timeframe_minutes
        return current_time.replace(minute=candle_minute, second=0, microsecond=0)

    def _build_candle(
        self, context: StrategyContext, candle_start: datetime
    ) -> Optional[Candle]:
        """1분봉 데이터로부터 N분봉 집계"""
        if not context.price_history:
            return Candle(
                start_time=candle_start,
                open_price=context.current_price,
                high_price=context.current_price,
                low_price=context.current_price,
                close_price=context.current_price,
                volume=0,
            )

        candle_end = candle_start.replace(
            minute=(candle_start.minute + self._timeframe_minutes) % 60
        )
        if candle_end.minute < candle_start.minute:
            candle_end = candle_end.replace(hour=candle_start.hour + 1)

        relevant_candles: List[OHLCV] = []
        for candle in context.price_history:
            try:
                candle_dt = datetime.strptime(candle.candle_datetime, "%Y-%m-%d %H:%M")
                if candle_start <= candle_dt < candle_end:
                    relevant_candles.append(candle)
            except (ValueError, AttributeError):
                continue

        if not relevant_candles:
            return Candle(
                start_time=candle_start,
                open_price=context.current_price,
                high_price=context.current_price,
                low_price=context.current_price,
                close_price=context.current_price,
                volume=0,
            )

        relevant_candles.sort(key=lambda c: c.candle_datetime)

        return Candle(
            start_time=candle_start,
            open_price=relevant_candles[0].open_price,
            high_price=max(c.high_price for c in relevant_candles),
            low_price=min(c.low_price for c in relevant_candles),
            close_price=context.current_price,
            volume=sum(c.volume for c in relevant_candles),
        )

    def _check_candle_elapsed(
        self, current_time: datetime, candle_start: datetime
    ) -> bool:
        """봉 시작 후 경과 시간 조건 확인"""
        if self._candle_elapsed_seconds < 0:
            return True
        elapsed = (current_time - candle_start).total_seconds()
        return elapsed >= self._candle_elapsed_seconds

    def can_generate_signal(self, context: StrategyContext) -> bool:
        """시그널 생성 가능 여부"""
        # 스캘핑은 executor가 포지션을 관리하므로, 포지션 보유 여부와 무관
        if not self._is_trading_time(context.current_time):
            logger.debug(
                f"[scalping_range][{context.stock_code}] 시간대 외: "
                f"{context.current_time.time()} "
                f"(범위: {self._trading_start}~{self._trading_end})"
            )
            return False
        return True

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """매수 시그널 생성"""
        stock_code = context.stock_code
        candle_start = self._get_candle_start(context.current_time)

        if not self._is_trading_time(context.current_time):
            return TradingSignal.hold(stock_code, "작동 시간대 외")

        # N분봉 집계
        candle = self._build_candle(context, candle_start)
        if not candle:
            return TradingSignal.hold(stock_code, "봉 데이터 없음")

        # 봉 경과 시간 조건 확인
        if not self._check_candle_elapsed(context.current_time, candle_start):
            return TradingSignal.hold(stock_code, "봉 경과 시간 미충족")

        # 진입 조건: 시가 대비 하락폭 확인
        if candle.open_price <= 0:
            return TradingSignal.hold(stock_code, "시가 데이터 없음")

        current_price = context.current_price
        open_price = candle.open_price
        dip_rate = (open_price - current_price) / open_price

        if dip_rate >= self._dip_threshold_pct:
            logger.info(
                f"[scalping_range][{stock_code}] 매수 시그널: "
                f"시가({open_price:,}) 대비 하락 {dip_rate*100:.2f}% >= "
                f"{self._dip_threshold_pct*100:.2f}%"
            )

            return TradingSignal.buy(
                stock_code=stock_code,
                quantity=self._position_size,
                reason=(
                    f"스캘핑 시그널: 시가({open_price:,}) 대비 "
                    f"-{dip_rate*100:.2f}%"
                ),
                confidence=0.9,
            )

        logger.debug(
            f"[scalping_range][{stock_code}] 매수 조건 미충족: "
            f"하락률 {dip_rate*100:.2f}% < {self._dip_threshold_pct*100:.2f}%"
        )
        return TradingSignal.hold(
            stock_code, f"진입 조건 미충족 (하락률: {dip_rate*100:.2f}%)"
        )
