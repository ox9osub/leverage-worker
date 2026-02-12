"""
스케줄러 모듈

매매 시간 스케줄링
- 장중: 1초 간격으로 종목별 실행 시점 체크
- 장외: 1분 간격으로 시간 체크
- 장 마감 시 미체결 취소
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
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
        self._on_prefetch_tick: Optional[Callable[[str, datetime], None]] = None
        self._on_check_fills: Optional[Callable[[], None]] = None
        self._on_market_open: Optional[Callable[[], None]] = None
        self._on_market_close: Optional[Callable[[], None]] = None
        self._on_idle: Optional[Callable[[], None]] = None

        # Prefetch 설정
        self._prefetch_second: int = 55  # 매분 n초에 예수금 사전 조회

        # 특정 시간 콜백 (예: "15:19" -> callback)
        self._specific_time_callbacks: Dict[str, Callable[[], None]] = {}
        # 오늘 실행한 시간 추적 (중복 방지)
        self._executed_times_today: Set[str] = set()
        # 마지막 체크 날짜 (자정 지나면 리셋)
        self._last_check_date: Optional[str] = None

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

    def set_on_check_fills(self, callback: Callable[[], None]) -> None:
        """체결 확인 콜백 설정 (틱 처리 전 1회 호출)"""
        self._on_check_fills = callback

    def set_on_market_open(self, callback: Callable[[], None]) -> None:
        """장 시작 콜백 설정"""
        self._on_market_open = callback

    def set_on_market_close(self, callback: Callable[[], None]) -> None:
        """장 마감 콜백 설정"""
        self._on_market_close = callback

    def set_on_idle(self, callback: Callable[[], None]) -> None:
        """대기 상태 콜백 설정 (장외 1분마다)"""
        self._on_idle = callback

    def set_on_prefetch_tick(
        self, callback: Callable[[str, datetime], None], prefetch_second: int = 55
    ) -> None:
        """
        예수금 사전 조회 콜백 설정

        Args:
            callback: (stock_code, current_time) -> None
            prefetch_second: 매분 n초에 실행 (default: 55)
        """
        self._on_prefetch_tick = callback
        self._prefetch_second = prefetch_second
        logger.info(f"Prefetch tick callback registered (second={prefetch_second})")

    def register_specific_time_callback(
        self, time_str: str, callback: Callable[[], None]
    ) -> None:
        """
        특정 시간에 실행할 콜백 등록

        Args:
            time_str: "HH:MM" 형식 (예: "15:19")
            callback: 실행할 콜백 함수
        """
        self._specific_time_callbacks[time_str] = callback
        logger.info(f"Registered specific time callback: {time_str}")

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
        종목별 틱은 병렬로 처리하여 API 지연으로 인한 누락 방지
        """
        # 1. 체결 확인 (병렬 처리 전 1회)
        if self._on_check_fills:
            try:
                self._on_check_fills()
            except Exception as e:
                logger.error(f"Check fills error: {e}")

        # 2. Prefetch 체크 (매분 n초)
        if self._on_prefetch_tick and now.second == self._prefetch_second:
            for stock_code in self._stocks:
                try:
                    self._on_prefetch_tick(stock_code, now)
                except Exception as e:
                    logger.error(f"Prefetch tick error [{stock_code}]: {e}")

        # 3. 종목별 틱 처리
        if not self._on_stock_tick:
            return

        # 실행할 종목 수집
        stocks_to_execute = []
        for stock_code, stock_config in self._stocks.items():
            interval = (
                stock_config.interval_seconds
                if stock_config.interval_seconds is not None
                else self._schedule.default_interval_seconds
            )
            offset = (
                stock_config.offset_seconds
                if stock_config.offset_seconds is not None
                else self._schedule.default_offset_seconds
            )

            if should_execute_stock(now, interval, offset):
                stocks_to_execute.append(stock_code)

        # 병렬 실행
        if stocks_to_execute:
            with ThreadPoolExecutor(max_workers=len(stocks_to_execute)) as executor:
                futures = {
                    executor.submit(self._on_stock_tick, stock_code, now): stock_code
                    for stock_code in stocks_to_execute
                }
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        stock_code = futures[future]
                        logger.error(f"Stock tick error [{stock_code}]: {e}")

        # 3. 특정 시간 콜백 처리
        self._check_specific_time_callbacks(now)

    def _check_specific_time_callbacks(self, now: datetime) -> None:
        """특정 시간 콜백 체크 및 실행"""
        # 날짜 변경 시 실행 기록 리셋
        current_date = now.strftime("%Y%m%d")
        if self._last_check_date != current_date:
            self._executed_times_today.clear()
            self._last_check_date = current_date

        current_time = now.strftime("%H:%M")

        # 등록된 시간 콜백 확인
        for time_str, callback in self._specific_time_callbacks.items():
            # 이미 오늘 실행했으면 skip
            if time_str in self._executed_times_today:
                continue

            # 정확한 시간 일치
            if current_time == time_str:
                logger.info(f"Executing specific time callback: {time_str}")
                try:
                    callback()
                    self._executed_times_today.add(time_str)
                except Exception as e:
                    logger.error(f"Specific time callback error [{time_str}]: {e}")
                    self._executed_times_today.add(time_str)  # 실패해도 재시도 방지
                continue

            # 놓친 경우 처리 (5초 이내면 실행)
            target_hour, target_minute = map(int, time_str.split(":"))
            time_diff_seconds = (
                (now.hour - target_hour) * 3600
                + (now.minute - target_minute) * 60
                + now.second
            )

            # 0~5초 사이면 늦었지만 실행 (WARNING)
            if 0 < time_diff_seconds <= 5:
                logger.warning(
                    f"Executing delayed specific time callback: {time_str} "
                    f"(delayed by {time_diff_seconds}s)"
                )
                try:
                    callback()
                    self._executed_times_today.add(time_str)
                except Exception as e:
                    logger.error(f"Specific time callback error [{time_str}]: {e}")
                    self._executed_times_today.add(time_str)
                continue

            # 5~10초 놓친 경우 - ERROR 로그만 (실행 안 함)
            if 5 < time_diff_seconds <= 10:
                if time_str not in self._executed_times_today:
                    logger.error(
                        f"Missed specific time callback: {time_str} "
                        f"(missed by {time_diff_seconds}s) - TOO LATE"
                    )
                    self._executed_times_today.add(time_str)  # 로그 중복 방지

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
