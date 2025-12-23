"""
스케줄러 모듈

매매 시간 스케줄링
- 장중: 1초 간격으로 종목별 실행 시점 체크
- 장외: 1분 간격으로 시간 체크
- 장 마감 시 미체결 취소
"""

import threading
import time
from datetime import datetime
from typing import Callable, Dict, List, Optional, Set

from leverage_worker.config.settings import Settings
from leverage_worker.utils.logger import get_logger
from leverage_worker.utils.time_utils import (
    is_trading_hours,
    should_execute_stock,
    get_time_until_market_open,
    get_time_until_market_close,
    is_weekday,
    format_duration,
)

logger = get_logger(__name__)


class TradingScheduler:
    """
    매매 스케줄러

    - 장중: 1초 간격으로 체크, 종목별 interval/offset에 따라 콜백 호출
    - 장외: 1분 간격으로 대기
    - 장 마감 시 on_market_close 콜백 호출
    """

    def __init__(self, settings: Settings):
        self._settings = settings
        self._schedule = settings.schedule
        self._stocks = settings.stocks

        # 스케줄러 상태
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # 콜백
        self._on_stock_tick: Optional[Callable[[str, datetime], None]] = None
        self._on_market_open: Optional[Callable[[], None]] = None
        self._on_market_close: Optional[Callable[[], None]] = None
        self._on_idle: Optional[Callable[[], None]] = None

        # 마지막 장 마감 처리 날짜 (중복 방지)
        self._last_close_date: Optional[str] = None

        # 이전 매매시간 상태 (상태 전환 감지용)
        self._was_trading_hours = False

        logger.info("TradingScheduler initialized")

    def set_on_stock_tick(self, callback: Callable[[str, datetime], None]) -> None:
        """
        종목 틱 콜백 설정

        Args:
            callback: (stock_code, current_time) -> None
        """
        self._on_stock_tick = callback

    def set_on_market_open(self, callback: Callable[[], None]) -> None:
        """장 시작 콜백 설정"""
        self._on_market_open = callback

    def set_on_market_close(self, callback: Callable[[], None]) -> None:
        """장 마감 콜백 설정"""
        self._on_market_close = callback

    def set_on_idle(self, callback: Callable[[], None]) -> None:
        """대기 상태 콜백 설정 (장외 1분마다)"""
        self._on_idle = callback

    def start(self) -> None:
        """스케줄러 시작"""
        if self._running:
            logger.warning("Scheduler already running")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="TradingSchedulerThread",
        )
        self._thread.start()

        logger.info("Scheduler started")

    def stop(self) -> None:
        """스케줄러 중지"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Scheduler stopped")

    def _run_loop(self) -> None:
        """메인 루프"""
        while self._running:
            try:
                now = datetime.now()

                # 주말 체크
                if not is_weekday(now):
                    logger.debug("Weekend - sleeping for 1 minute")
                    time.sleep(60)
                    continue

                # 매매 시간 체크
                is_trading = is_trading_hours(
                    now,
                    self._schedule.trading_start,
                    self._schedule.trading_end,
                )

                if is_trading:
                    # 장 시작 감지
                    if not self._was_trading_hours:
                        self._was_trading_hours = True
                        logger.info("Market opened")
                        if self._on_market_open:
                            self._on_market_open()

                    # 장중: 1초마다 체크
                    self._process_trading_tick(now)
                    time.sleep(1)

                else:
                    # 장 마감 감지
                    if self._was_trading_hours:
                        self._was_trading_hours = False
                        today = now.strftime("%Y%m%d")

                        if self._last_close_date != today:
                            self._last_close_date = today
                            logger.info("Market closed")
                            if self._on_market_close:
                                self._on_market_close()

                    # 장외: 1분마다 체크
                    self._process_idle(now)
                    time.sleep(60)

            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                time.sleep(1)

    def _process_trading_tick(self, now: datetime) -> None:
        """
        장중 틱 처리

        각 종목별 interval/offset 설정에 따라 콜백 호출 여부 결정
        """
        if not self._on_stock_tick:
            return

        for stock_code, stock_config in self._stocks.items():
            # 종목별 interval/offset
            interval = stock_config.get(
                "interval_seconds",
                self._schedule.default_interval_seconds,
            )
            offset = stock_config.get(
                "offset_seconds",
                self._schedule.default_offset_seconds,
            )

            # 실행 시점인지 확인
            if should_execute_stock(now, interval, offset):
                try:
                    self._on_stock_tick(stock_code, now)
                except Exception as e:
                    logger.error(f"Stock tick error [{stock_code}]: {e}")

    def _process_idle(self, now: datetime) -> None:
        """장외 대기 처리"""
        if self._on_idle:
            try:
                self._on_idle()
            except Exception as e:
                logger.error(f"Idle callback error: {e}")

        # 남은 시간 로그
        h, m, s = get_time_until_market_open(now, self._schedule.trading_start)
        if h > 0 or m > 0 or s > 0:
            logger.debug(f"Time until market open: {h}h {m}m {s}s")

    def get_managed_stocks(self) -> List[str]:
        """관리 종목 코드 리스트"""
        return list(self._stocks.keys())

    def get_stock_interval(self, stock_code: str) -> int:
        """종목별 실행 간격 (초)"""
        return self._settings.get_stock_interval(stock_code)

    def get_stock_offset(self, stock_code: str) -> int:
        """종목별 실행 오프셋 (초)"""
        return self._settings.get_stock_offset(stock_code)

    def is_trading_time(self) -> bool:
        """현재 매매 시간인지 확인"""
        now = datetime.now()
        return is_trading_hours(
            now,
            self._schedule.trading_start,
            self._schedule.trading_end,
        )

    def get_status(self) -> Dict:
        """스케줄러 상태 정보"""
        now = datetime.now()
        is_trading = self.is_trading_time()

        status = {
            "running": self._running,
            "is_trading_hours": is_trading,
            "current_time": now.strftime("%H:%M:%S"),
            "trading_start": self._schedule.trading_start,
            "trading_end": self._schedule.trading_end,
            "managed_stocks": len(self._stocks),
        }

        if is_trading:
            h, m, s = get_time_until_market_close(now, self._schedule.trading_end)
            status["time_until_close"] = f"{h}h {m}m {s}s"
        else:
            h, m, s = get_time_until_market_open(now, self._schedule.trading_start)
            status["time_until_open"] = f"{h}h {m}m {s}s"

        return status

    @property
    def is_running(self) -> bool:
        return self._running
