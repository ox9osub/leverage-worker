"""
딥 바이(Dip Buy) 전략

N분봉 시가 대비 하락 시 진입, 목표 수익/손절/시간 청산
WebSocket 실시간 데이터 기반 전략
"""

from dataclasses import dataclass
from datetime import datetime, time
from typing import Any, Dict, List, Optional

from leverage_worker.data.minute_candle_repository import MinuteCandle as OHLCV
from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class Candle:
    """N분봉 데이터"""

    start_time: datetime  # 봉 시작 시간
    open_price: float  # 시가
    high_price: float  # 고가
    low_price: float  # 저가
    close_price: float  # 종가 (현재가)
    volume: int  # 거래량


@register_strategy("dip_buy")
class DipBuyStrategy(BaseStrategy):
    """
    딥 바이(Dip Buy) 전략

    N분봉 시가 대비 일정 하락 시 진입하여 목표 수익/손절 시 청산

    파라미터:
        trading_start: 작동 시작 시간 (기본 "09:15")
        trading_end: 작동 종료 시간 (기본 "09:45")
        timeframe_minutes: 타임프레임 분 단위 (기본 3)
        candle_elapsed_seconds: 봉 시작 후 경과 시간, -1이면 즉시 (기본 -1)
        dip_threshold_pct: 시가 대비 하락폭 % (기본 0.3)
        take_profit_pct: 목표 수익률 % (기본 0.2)
        max_holding_bars: 최대 보유 기간 (N분봉 기준) (기본 60)
        stop_loss_pct: 손절 비율 % (기본 2.0)
        position_size: 매수 수량 (기본 1)
    """

    MIN_DATA_REQUIRED = 1  # 최소 1분봉 데이터 필요

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        # 작동 시간대
        self._trading_start = self._parse_time(
            self.get_param("trading_start", "09:15")
        )
        self._trading_end = self._parse_time(self.get_param("trading_end", "09:45"))

        # 타임프레임 설정
        self._timeframe_minutes = self.get_param("timeframe_minutes", 3)
        self._candle_elapsed_seconds = self.get_param("candle_elapsed_seconds", -1)

        # 진입 조건
        self._dip_threshold_pct = self.get_param("dip_threshold_pct", 0.3) / 100

        # 청산 조건
        self._take_profit_pct = self.get_param("take_profit_pct", 0.2) / 100
        self._max_holding_bars = self.get_param("max_holding_bars", 60)
        self._stop_loss_pct = self.get_param("stop_loss_pct", 2.0) / 100

        # 포지션 설정
        self._position_size = self.get_param("position_size", 1)

        # 상태 변수
        self._entry_bar_count = 0
        self._entry_price: Optional[float] = None
        self._last_candle_start: Optional[datetime] = None

        logger.info(
            f"[{name}] DipBuyStrategy initialized: "
            f"timeframe={self._timeframe_minutes}min, "
            f"dip={self._dip_threshold_pct*100:.2f}%, "
            f"tp={self._take_profit_pct*100:.2f}%, "
            f"sl={self._stop_loss_pct*100:.2f}%, "
            f"max_hold={self._max_holding_bars}bars"
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
            # 데이터 없으면 현재가로 초기화
            return Candle(
                start_time=candle_start,
                open_price=context.current_price,
                high_price=context.current_price,
                low_price=context.current_price,
                close_price=context.current_price,
                volume=0,
            )

        # N분봉 시작/종료 시간 계산
        candle_end = candle_start.replace(
            minute=(candle_start.minute + self._timeframe_minutes) % 60
        )
        if candle_end.minute < candle_start.minute:
            # 시간이 넘어가는 경우 (예: 59분 -> 02분)
            candle_end = candle_end.replace(hour=candle_start.hour + 1)

        # 해당 시간대의 1분봉들 필터링
        relevant_candles: List[OHLCV] = []
        for candle in context.price_history:
            try:
                candle_dt = datetime.strptime(candle.candle_datetime, "%Y-%m-%d %H:%M")
                if candle_start <= candle_dt < candle_end:
                    relevant_candles.append(candle)
            except (ValueError, AttributeError):
                continue

        if not relevant_candles:
            # 1분봉 데이터 없으면 현재가로 초기화
            return Candle(
                start_time=candle_start,
                open_price=context.current_price,
                high_price=context.current_price,
                low_price=context.current_price,
                close_price=context.current_price,
                volume=0,
            )

        # 시간순 정렬
        relevant_candles.sort(key=lambda c: c.candle_datetime)

        return Candle(
            start_time=candle_start,
            open_price=relevant_candles[0].open_price,
            high_price=max(c.high_price for c in relevant_candles),
            low_price=min(c.low_price for c in relevant_candles),
            close_price=context.current_price,  # 현재가 사용
            volume=sum(c.volume for c in relevant_candles),
        )

    def _check_candle_elapsed(
        self, current_time: datetime, candle_start: datetime
    ) -> bool:
        """봉 시작 후 경과 시간 조건 확인"""
        if self._candle_elapsed_seconds < 0:
            return True  # -1이면 즉시 작동

        elapsed = (current_time - candle_start).total_seconds()
        return elapsed >= self._candle_elapsed_seconds

    def _update_bar_count(self, candle_start: datetime) -> None:
        """N분봉 완성 시 보유 기간 카운트 증가"""
        if candle_start != self._last_candle_start:
            # 새 봉 시작 = 이전 봉 완성
            if self._last_candle_start is not None and self._entry_price is not None:
                self._entry_bar_count += 1
            self._last_candle_start = candle_start

    def can_generate_signal(self, context: StrategyContext) -> bool:
        """시그널 생성 가능 여부 확인"""
        # 포지션 보유 중이면 청산 로직을 위해 항상 True
        if context.has_position:
            logger.debug(f"[dip_buy][{context.stock_code}] 포지션 보유 중 - 청산 로직 실행 가능")
            return True

        # 미보유 시에는 작동 시간대 확인
        if not self._is_trading_time(context.current_time):
            current = context.current_time.time()
            logger.debug(
                f"[dip_buy][{context.stock_code}] 시간대 외: {current} "
                f"(범위: {self._trading_start}~{self._trading_end})"
            )
            return False

        logger.debug(f"[dip_buy][{context.stock_code}] 시그널 생성 가능")
        return True

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """매매 시그널 생성"""
        stock_code = context.stock_code
        candle_start = self._get_candle_start(context.current_time)

        # 포지션 보유 시 청산 조건 확인
        if context.has_position:
            # 보유 기간 카운팅 (N분봉 완성 시)
            self._update_bar_count(candle_start)
            return self._check_exit_conditions(context)

        # 미보유 시 진입 조건 확인
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
            # 진입
            logger.info(
                f"[dip_buy][{stock_code}] 매수 조건 충족: "
                f"하락률 {dip_rate*100:.2f}% >= {self._dip_threshold_pct*100:.2f}%"
            )
            self._entry_bar_count = 0
            self._entry_price = current_price
            self._last_candle_start = candle_start

            return TradingSignal.buy(
                stock_code=stock_code,
                quantity=self._position_size,
                reason=f"딥 바이 진입: 시가({open_price:,.0f}) 대비 -{dip_rate*100:.2f}%",
                confidence=0.9,
            )

        logger.debug(
            f"[dip_buy][{stock_code}] 매수 조건 미충족: "
            f"하락률 {dip_rate*100:.2f}% < {self._dip_threshold_pct*100:.2f}%"
        )
        return TradingSignal.hold(
            stock_code, f"진입 조건 미충족 (하락률: {dip_rate*100:.2f}%)"
        )

    def _check_exit_conditions(self, context: StrategyContext) -> TradingSignal:
        """청산 조건 확인"""
        stock_code = context.stock_code
        profit_rate = context.profit_rate / 100  # % -> 소수

        # 1. 손절
        if profit_rate <= -self._stop_loss_pct:
            reason = f"손절: {profit_rate*100:.2f}%"
            self._reset_state()
            return TradingSignal.sell(
                stock_code=stock_code,
                quantity=context.position_quantity,
                reason=reason,
                confidence=1.0,
            )

        # 2. 익절
        if profit_rate >= self._take_profit_pct:
            reason = f"익절: {profit_rate*100:.2f}%"
            self._reset_state()
            return TradingSignal.sell(
                stock_code=stock_code,
                quantity=context.position_quantity,
                reason=reason,
                confidence=1.0,
            )

        # 3. 시간 청산 (N분봉 기준)
        if self._entry_bar_count >= self._max_holding_bars:
            reason = f"시간 청산: {self._entry_bar_count}봉 보유 (profit: {profit_rate*100:.2f}%)"
            self._reset_state()
            return TradingSignal.sell(
                stock_code=stock_code,
                quantity=context.position_quantity,
                reason=reason,
                confidence=0.8,
            )

        return TradingSignal.hold(
            stock_code,
            f"보유 중 ({self._entry_bar_count}봉, profit: {profit_rate*100:.2f}%)",
        )

    def _reset_state(self) -> None:
        """상태 초기화"""
        self._entry_bar_count = 0
        self._entry_price = None
        self._last_candle_start = None

    def on_entry(self, context: StrategyContext, signal: TradingSignal) -> None:
        """매수 성공 시 호출"""
        logger.info(
            f"[{self.name}] 진입: {context.stock_code} @ {context.current_price:,} "
            f"- {signal.reason}"
        )
        self._entry_bar_count = 0
        self._entry_price = context.current_price
        self._last_candle_start = self._get_candle_start(context.current_time)

    def on_exit(self, context: StrategyContext, signal: TradingSignal) -> None:
        """매도 성공 시 호출"""
        logger.info(
            f"[{self.name}] 청산: {context.stock_code} @ {context.current_price:,} "
            f"- {signal.reason}"
        )
        self._reset_state()
