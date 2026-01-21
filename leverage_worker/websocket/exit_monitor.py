"""
실시간 매도 모니터링 모듈

포지션 보유 시 WebSocket 실시간 가격으로 TP/SL 체크
기존 60초 폴링의 백업으로도 사용
"""

import asyncio
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set

import pandas as pd
import websockets

from leverage_worker.utils.logger import get_logger
from leverage_worker.utils.time_utils import is_trading_hours
from leverage_worker.websocket.tick_handler import TickData, TickHandler

logger = get_logger(__name__)

# kis_auth 모듈 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "examples_user"))

import kis_auth as ka
from domestic_stock.domestic_stock_functions_ws import ccnl_krx


@dataclass
class ExitMonitorConfig:
    """매도 모니터링 설정"""

    stock_code: str
    strategy_name: str
    avg_price: float
    quantity: int
    entry_time: datetime
    take_profit_pct: float  # 0.003 = 0.3%
    stop_loss_pct: float  # 0.01 = 1.0%
    max_holding_minutes: int  # 60


class ExitMonitor:
    """
    실시간 매도 모니터링

    포지션 보유 시 WebSocket 구독 시작
    실시간 가격으로 TP/SL/Timeout 체크
    매도 체결 후 구독 해제
    """

    # WebSocket 운영 시간 (KRX 정규장 시간)
    WS_MARKET_OPEN = "08:59"
    WS_MARKET_CLOSE = "15:30"

    def __init__(
        self,
        on_exit_signal: Callable[[str, str, int, str, bool], None],
    ):
        """
        Args:
            on_exit_signal: 매도 시그널 콜백
                (stock_code, strategy_name, quantity, reason, is_take_profit)
        """
        self._on_exit_signal = on_exit_signal
        self._tick_handler = TickHandler()

        # 모니터링 상태
        self._monitored: Dict[str, ExitMonitorConfig] = {}  # stock_code -> config
        self._exit_in_progress: Set[str] = set()  # 매도 진행 중인 종목
        self._lock = threading.RLock()

        # WebSocket 상태
        self._ws_thread: Optional[threading.Thread] = None
        self._ws: Optional[websockets.ClientConnection] = None
        self._kws: Optional[ka.KISWebSocket] = None
        self._running = False
        self._subscribed_stocks: Set[str] = set()

    def start(self) -> None:
        """모니터링 시작"""
        if self._running:
            logger.warning("[ExitMonitor] Already running")
            return

        self._running = True
        logger.info("[ExitMonitor] Started (waiting for positions)")

    def stop(self) -> None:
        """모니터링 중지 - WebSocket graceful close 포함"""
        self._running = False

        # WebSocket graceful close
        if self._ws is not None:
            try:
                # KISWebSocket 재연결 방지
                if self._kws is not None:
                    self._kws.retry_count = self._kws.max_retries

                # async close를 동기 코드에서 실행
                asyncio.run(self._ws.close())
                logger.info("[ExitMonitor] WebSocket connection closed gracefully")
            except Exception as e:
                logger.warning(f"[ExitMonitor] WebSocket close error (expected): {e}")
            finally:
                self._ws = None
                self._kws = None

        # 스레드 종료 대기
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=3.0)
            if self._ws_thread.is_alive():
                logger.warning("[ExitMonitor] WebSocket thread did not terminate in time")
        self._ws_thread = None

        with self._lock:
            self._monitored.clear()
            self._exit_in_progress.clear()
            self._subscribed_stocks.clear()

        logger.info("[ExitMonitor] Stopped")

    def add_position(self, config: ExitMonitorConfig) -> None:
        """
        포지션 모니터링 등록

        매수 체결 시 호출
        """
        if not self._running:
            logger.warning("[ExitMonitor] Not running, cannot add position")
            return

        with self._lock:
            stock_code = config.stock_code

            if stock_code in self._monitored:
                logger.warning(f"[ExitMonitor] {stock_code} already monitored, updating")
                self._monitored[stock_code] = config
                return

            self._monitored[stock_code] = config

            # WebSocket이 실행 중이 아니면 시작
            if not self._ws_thread or not self._ws_thread.is_alive():
                self._start_websocket([stock_code])
            else:
                # 실행 중이면 동적 구독 추가
                self._subscribe_stock(stock_code)

            logger.info(
                f"[ExitMonitor] Added {stock_code} "
                f"(TP: {config.take_profit_pct:.2%}, SL: {config.stop_loss_pct:.2%}, "
                f"Timeout: {config.max_holding_minutes}min)"
            )

    def remove_position(self, stock_code: str) -> None:
        """
        포지션 모니터링 해제

        매도 체결 시 호출
        """
        with self._lock:
            if stock_code not in self._monitored:
                return

            del self._monitored[stock_code]
            self._exit_in_progress.discard(stock_code)

            # 구독 해제
            self._unsubscribe_stock(stock_code)

            logger.info(f"[ExitMonitor] Removed {stock_code}")

    def is_monitored(self, stock_code: str) -> bool:
        """모니터링 중인지 확인"""
        return stock_code in self._monitored

    def is_exit_in_progress(self, stock_code: str) -> bool:
        """매도 진행 중인지 확인 (중복 주문 방지)"""
        return stock_code in self._exit_in_progress

    def get_monitored_stocks(self) -> List[str]:
        """모니터링 중인 종목 목록"""
        with self._lock:
            return list(self._monitored.keys())

    @property
    def is_running(self) -> bool:
        """실행 중 여부"""
        return self._running

    @property
    def is_ws_connected(self) -> bool:
        """WebSocket 연결 여부"""
        return self._ws_thread is not None and self._ws_thread.is_alive()

    def _is_ws_market_hours(self) -> bool:
        """WebSocket 운영 시간인지 확인 (08:59~15:30)"""
        return is_trading_hours(datetime.now(), self.WS_MARKET_OPEN, self.WS_MARKET_CLOSE)

    def _start_websocket(self, stock_codes: List[str]) -> None:
        """WebSocket 연결 시작 (별도 스레드)"""
        # 시장 시간 체크
        if not self._is_ws_market_hours():
            logger.info(
                f"[ExitMonitor] WebSocket skipped - outside market hours "
                f"({self.WS_MARKET_OPEN}~{self.WS_MARKET_CLOSE})"
            )
            return

        self._ws_thread = threading.Thread(
            target=self._run_websocket,
            args=(stock_codes,),
            name="ExitMonitorWSThread",
            daemon=True,
        )
        self._ws_thread.start()
        logger.info(f"[ExitMonitor] WebSocket started for {stock_codes}")

    def _run_websocket(self, initial_stocks: List[str]) -> None:
        """WebSocket 실행 (별도 스레드)"""
        try:
            # WebSocket 인증
            ka.auth_ws()
            logger.info("[ExitMonitor] WebSocket authenticated")

            # WebSocket 객체 생성
            self._kws = ka.KISWebSocket(api_url="/tryitout", max_retries=10)

            # 초기 종목 구독
            for stock_code in initial_stocks:
                ka.KISWebSocket.subscribe(
                    request=ccnl_krx,
                    data=[stock_code],
                )
                self._subscribed_stocks.add(stock_code)
                logger.info(f"[ExitMonitor] Subscribed to {stock_code}")

            # WebSocket 시작 (블로킹) - ws 객체 저장을 위한 커스텀 래퍼 사용
            self._kws.start(
                on_result=self._on_ws_result,
                result_all_data=False,
            )

        except Exception as e:
            logger.error(f"[ExitMonitor] WebSocket error: {e}")

    def _subscribe_stock(self, stock_code: str) -> None:
        """종목 구독 추가 (동적)"""
        if stock_code in self._subscribed_stocks:
            return

        # 시장 시간 체크
        if not self._is_ws_market_hours():
            logger.debug(
                f"[ExitMonitor] Subscribe skipped for {stock_code} - outside market hours"
            )
            return

        # NOTE: KIS WebSocket의 동적 구독은 연결 후 send_multiple 호출 필요
        # 현재 구조에서는 새 연결이 필요할 수 있음
        # 일단 subscribe 등록 후 재연결 방식 사용
        if self._kws and self._ws:
            try:
                # 동적 구독 시도 (연결 중일 때)
                ka.KISWebSocket.subscribe(
                    request=ccnl_krx,
                    data=[stock_code],
                )
                self._subscribed_stocks.add(stock_code)
                logger.info(f"[ExitMonitor] Dynamically subscribed to {stock_code}")
            except Exception as e:
                logger.warning(f"[ExitMonitor] Dynamic subscribe failed: {e}")
        else:
            # WebSocket이 없으면 새로 시작
            self._start_websocket([stock_code])

    def _unsubscribe_stock(self, stock_code: str) -> None:
        """종목 구독 해제"""
        if stock_code not in self._subscribed_stocks:
            return

        self._subscribed_stocks.discard(stock_code)
        logger.info(f"[ExitMonitor] Unsubscribed from {stock_code}")

        # 모니터링 종목이 없으면 WebSocket 유지 (재사용 위해)

    def _on_ws_result(
        self,
        ws: websockets.ClientConnection,
        tr_id: str,
        df: pd.DataFrame,
        data_info: dict,
    ) -> None:
        """WebSocket 데이터 수신 콜백"""
        if not self._running:
            return

        # WebSocket 연결 객체 저장 (동적 구독/해제용)
        if self._ws is None:
            self._ws = ws

        # H0STCNT0 (체결가)만 처리
        if tr_id != "H0STCNT0":
            return

        # 체결 데이터 파싱
        tick_data = self._tick_handler.parse(df, tr_id)
        if not tick_data:
            return

        # 모니터링 중인 종목인지 확인
        stock_code = tick_data.stock_code

        with self._lock:
            config = self._monitored.get(stock_code)
            if not config:
                return

            # 매도 진행 중이면 스킵
            if stock_code in self._exit_in_progress:
                return

            # TP/SL/Timeout 체크
            exit_result = self._check_exit(config, tick_data.price, tick_data.timestamp)

            if exit_result:
                reason, is_take_profit = exit_result
                self._exit_in_progress.add(stock_code)

                logger.info(
                    f"[ExitMonitor] Exit signal: {stock_code} - {reason} "
                    f"(price: {tick_data.price:,}, avg: {config.avg_price:,.0f})"
                )

                # 매도 시그널 콜백 호출
                self._on_exit_signal(
                    stock_code,
                    config.strategy_name,
                    config.quantity,
                    reason,
                    is_take_profit,
                )

    def _check_exit(
        self,
        config: ExitMonitorConfig,
        current_price: int,
        current_time: datetime,
    ) -> Optional[tuple]:
        """
        TP/SL/Timeout 체크

        Returns:
            (reason, is_take_profit) 또는 None
        """
        profit_rate = (current_price - config.avg_price) / config.avg_price

        # 손절 (우선 확인)
        if profit_rate <= -config.stop_loss_pct:
            return (f"손절: {profit_rate:.2%}", False)

        # 익절
        if profit_rate >= config.take_profit_pct:
            return (f"익절: {profit_rate:.2%}", True)

        # 시간 청산
        holding_seconds = (current_time - config.entry_time).total_seconds()
        holding_minutes = holding_seconds / 60

        if holding_minutes >= config.max_holding_minutes:
            return (f"시간 청산: {holding_minutes:.0f}분 보유", False)

        return None
