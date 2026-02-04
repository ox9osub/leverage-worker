"""
실시간 WebSocket 클라이언트

KISWebSocket을 래핑하여 별도 스레드에서 실행
체결 데이터 수신 시 콜백 호출
"""

import asyncio
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional

import pandas as pd
import websockets

from leverage_worker.utils.logger import get_logger
from leverage_worker.websocket.tick_handler import TickData, TickHandler

logger = get_logger(__name__)

# kis_auth 모듈 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "examples_user"))

import kis_auth as ka
from domestic_stock.domestic_stock_functions_ws import ccnl_krx, ccnl_notice
from leverage_worker.websocket.order_notice_handler import OrderNoticeData, OrderNoticeHandler


class RealtimeWSClient:
    """
    실시간 WebSocket 클라이언트

    별도 스레드에서 KISWebSocket을 실행하여 실시간 체결 데이터 수신
    체결 데이터 수신 시 on_tick 콜백 호출
    """

    def __init__(
        self,
        on_tick: Callable[[TickData], None],
        on_error: Optional[Callable[[Exception], None]] = None,
        on_order_notice: Optional[Callable[[OrderNoticeData], None]] = None,
        is_paper: bool = True,
        hts_id: str = "",
    ):
        """
        Args:
            on_tick: 체결 데이터 수신 시 호출할 콜백
            on_error: 에러 발생 시 호출할 콜백
            on_order_notice: 체결통보 수신 시 호출할 콜백
            is_paper: 모의투자 여부 (True면 모의투자 WebSocket 사용)
            hts_id: HTS ID (체결통보 구독용)
        """
        self._on_tick = on_tick
        self._on_error = on_error
        self._on_order_notice = on_order_notice
        self._is_paper = is_paper
        self._hts_id = hts_id
        self._tick_handler = TickHandler()
        self._order_notice_handler = OrderNoticeHandler()

        self._ws_thread: Optional[threading.Thread] = None
        self._ws: Optional[websockets.ClientConnection] = None
        self._kws: Optional[ka.KISWebSocket] = None
        self._running = False
        self._stock_codes: List[str] = []
        self._order_notice_subscribed = False
        self._last_ws_data_time: Optional[datetime] = None

    def start(self, stock_codes: List[str]) -> None:
        """
        WebSocket 연결 시작 (별도 스레드)

        Args:
            stock_codes: 구독할 종목코드 목록
        """
        if self._running:
            logger.warning("WebSocket client already running")
            return

        self._stock_codes = stock_codes
        self._running = True

        # 별도 스레드에서 WebSocket 실행
        self._ws_thread = threading.Thread(
            target=self._run_websocket,
            name="WebSocketThread",
            daemon=True,
        )
        self._ws_thread.start()
        logger.info(f"WebSocket client started for {len(stock_codes)} stocks")

    def stop(self) -> None:
        """WebSocket 연결 중지 - graceful close 포함"""
        self._running = False

        # WebSocket graceful close
        if self._ws is not None:
            try:
                # KISWebSocket 재연결 방지
                if self._kws is not None:
                    self._kws.retry_count = self._kws.max_retries

                # async close를 동기 코드에서 실행
                asyncio.run(self._ws.close())
                logger.info("WebSocket connection closed gracefully")
            except Exception as e:
                logger.warning(f"WebSocket close error (expected): {e}")
            finally:
                self._ws = None
                self._kws = None

        # 스레드 종료 대기
        if self._ws_thread and self._ws_thread.is_alive():
            self._ws_thread.join(timeout=3.0)
            if self._ws_thread.is_alive():
                logger.warning("WebSocket thread did not terminate in time")
        self._ws_thread = None
        self._order_notice_subscribed = False
        self._last_ws_data_time = None

        logger.info("WebSocket client stopped")

    def _run_websocket(self) -> None:
        """WebSocket 실행 (별도 스레드에서 호출)"""
        try:
            # WebSocket 인증 (모드에 맞는 키 사용)
            ws_svr = "vps" if self._is_paper else "prod"
            ka.auth_ws(svr=ws_svr)
            logger.info(f"WebSocket authenticated (svr={ws_svr})")

            # WebSocket 객체 생성 및 인스턴스 변수 저장
            self._kws = ka.KISWebSocket(api_url="/tryitout", max_retries=10)

            # 종목별 체결가 구독 등록
            for stock_code in self._stock_codes:
                ka.KISWebSocket.subscribe(
                    request=ccnl_krx,
                    data=[stock_code],
                )
                logger.info(f"Subscribed to {stock_code}")

            # 체결통보 구독 (HTS ID 기반, 1회만)
            if self._hts_id and self._on_order_notice:
                try:
                    env_dv = "demo" if self._is_paper else "real"
                    ka.KISWebSocket.subscribe(
                        request=ccnl_notice,
                        data=[self._hts_id],
                        kwargs={"env_dv": env_dv},
                    )
                    self._order_notice_subscribed = True
                    logger.info(
                        f"Subscribed to order notice (hts_id={self._hts_id}, env={env_dv})"
                    )
                except Exception as e:
                    logger.error(
                        f"[WS] 체결통보 구독 실패: {e} - REST 폴백으로 동작합니다"
                    )
                    self._order_notice_subscribed = False
            else:
                if not self._hts_id:
                    logger.warning("[WS] hts_id 미설정 - 체결통보 구독 불가")
                if not self._on_order_notice:
                    logger.warning("[WS] on_order_notice 콜백 미설정")

            # WebSocket 시작 (블로킹)
            self._kws.start(
                on_result=self._on_ws_result,
                result_all_data=False,
            )

        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            if self._on_error:
                self._on_error(e)

    def _on_ws_result(
        self,
        ws: websockets.ClientConnection,
        tr_id: str,
        df: pd.DataFrame,
        data_info: dict,
    ) -> None:
        """
        WebSocket 데이터 수신 콜백

        KISWebSocket의 on_result 콜백으로 설정됨
        체결 데이터(H0STCNT0)만 처리하여 on_tick 콜백 호출
        """
        if not self._running:
            return

        # WS 건강 상태 갱신 (모든 데이터 수신 시)
        self._last_ws_data_time = datetime.now()

        # WebSocket 연결 객체 저장 (graceful close용)
        if self._ws is None:
            self._ws = ws

        # H0STCNI0 (실전) / H0STCNI9 (모의) 체결통보 처리
        if tr_id in ("H0STCNI0", "H0STCNI9"):
            notice = self._order_notice_handler.parse(df)
            if notice and self._on_order_notice:
                logger.info(
                    f"[WS] 체결통보: {notice.stock_code} "
                    f"{'매도' if notice.side == '01' else '매수'} "
                    f"x{notice.filled_qty} @ {notice.filled_price:,}원"
                )
                self._on_order_notice(notice)
            return

        # H0STCNT0 (체결가)만 처리
        if tr_id != "H0STCNT0":
            return

        # 체결 데이터 파싱
        tick_data = self._tick_handler.parse(df, tr_id)
        if tick_data:
            logger.debug(
                f"[WS] {tick_data.stock_code} 체결: {tick_data.price:,}원 "
                f"({tick_data.change_rate:+.2f}%)"
            )
            # 콜백 호출
            self._on_tick(tick_data)

    @property
    def is_running(self) -> bool:
        """실행 중 여부"""
        return self._running

    @property
    def is_order_notice_active(self) -> bool:
        """WebSocket 체결통보 수신 가능 여부 (10초 이내 데이터 수신 = 정상)"""
        if not self._running or not self._order_notice_subscribed:
            return False
        if self._last_ws_data_time is None:
            return False
        return (datetime.now() - self._last_ws_data_time).total_seconds() < 10
