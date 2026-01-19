"""
트레이딩 엔진 모듈

전체 시스템 통합 및 메인 로직
- 컴포넌트 초기화
- 매매 루프
- 시그널 처리
"""

import signal
import sys
import threading
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Set

from leverage_worker.config.settings import Settings, TradingMode
from leverage_worker.core.emergency import EmergencyStop, create_emergency_stop_handler
from leverage_worker.core.health_checker import (
    HealthChecker,
    create_api_health_check,
    create_db_health_check,
    create_scheduler_health_check,
)
from leverage_worker.core.recovery_manager import RecoveryManager
from leverage_worker.core.scheduler import TradingScheduler
from leverage_worker.core.session_manager import SessionManager
from leverage_worker.data.daily_candle_repository import DailyCandle, DailyCandleRepository
from leverage_worker.data.database import MarketDataDB, TradingDB
from leverage_worker.data.minute_candle_repository import MinuteCandleRepository
from leverage_worker.notification.daily_report import DailyReportGenerator
from leverage_worker.notification.slack_notifier import SlackNotifier
from leverage_worker.strategy import (
    BaseStrategy,
    StrategyContext,
    StrategyRegistry,
    TradingSignal,
)
from leverage_worker.trading.broker import KISBroker, Position, OrderSide
from leverage_worker.trading.order_manager import ManagedOrder, OrderManager
from leverage_worker.trading.position_manager import PositionManager
from leverage_worker.utils.logger import get_logger
from leverage_worker.utils.log_constants import LogEventType
from leverage_worker.utils.math_utils import calculate_allocation_amount
from leverage_worker.utils.structured_logger import get_structured_logger
from leverage_worker.utils.time_utils import get_current_minute_key
from leverage_worker.websocket import RealtimeWSClient, TickData

logger = get_logger(__name__)
structured_logger = get_structured_logger()


class TradingEngine:
    """
    트레이딩 엔진

    - 컴포넌트 초기화 및 연결
    - 매매 루프 실행
    - Graceful Shutdown
    """

    def __init__(self, settings: Settings):
        self._settings = settings
        self._running = False

        # 컴포넌트 초기화
        logger.info(f"Initializing TradingEngine (mode: {settings.mode.value})")

        # 1. Database (시세 DB / 매매 DB 분리)
        # 시세 DB: 모의/실전 공유 (market_data.db)
        self._market_db = MarketDataDB(settings.market_data_db_path)
        # 매매 DB: 모의/실전 분리 (trading_paper.db / trading_live.db)
        self._trading_db = TradingDB(settings.trading_db_path)

        # 2. Minute Candle Repository (분봉 데이터 - 시세 DB)
        self._price_repo = MinuteCandleRepository(self._market_db)

        # 2-1. Daily Candle Repository (일봉 데이터 - 시세 DB)
        self._daily_repo = DailyCandleRepository(self._market_db)

        # 2-2. 일봉 캐시: stock_code -> List[DailyCandle]
        self._daily_candles_cache: Dict[str, List[DailyCandle]] = {}

        # 3. Session Manager (인증)
        self._session = SessionManager(settings)

        # 4. Broker
        self._broker: Optional[KISBroker] = None

        # 5. Position Manager
        self._position_manager: Optional[PositionManager] = None

        # 6. Order Manager
        self._order_manager: Optional[OrderManager] = None

        # 7. Scheduler
        self._scheduler = TradingScheduler(settings)

        # 8. Slack Notifier
        self._slack = SlackNotifier(
            webhook_url=settings.notification.slack_webhook_url,
            token=settings.notification.slack_token,
            channel=settings.notification.slack_channel,
            is_paper_mode=settings.is_paper_trading(),
        )

        # 9. Daily Report Generator (매매 DB 사용)
        self._report_generator = DailyReportGenerator(self._trading_db, self._slack)

        # 10. 전략 인스턴스 캐시: (stock_code, strategy_name) -> BaseStrategy
        self._strategies: Dict[tuple, BaseStrategy] = {}

        # 11. Health Checker
        self._health_checker = HealthChecker(
            check_interval_seconds=60,
            on_unhealthy_callback=self._on_health_change,
        )

        # 12. Recovery Manager
        self._recovery_manager = RecoveryManager(
            on_crash_detected=self._on_crash_detected,
        )

        # 13. Emergency Stop (핸들러는 start()에서 설정)
        self._emergency_stop = EmergencyStop(
            check_interval_seconds=5,
        )

        # 14. WebSocket 클라이언트 (실시간 전략용)
        self._ws_client: Optional[RealtimeWSClient] = None
        self._ws_stock_codes: Set[str] = set()  # WebSocket 구독 종목

        # 15. 동시성 제어 (스케줄러/WebSocket 공유 리소스 보호)
        self._tick_lock = threading.Lock()

        # 세션 ID
        self._session_id = str(uuid.uuid4())[:8]

        # 시그널 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("TradingEngine initialized")
        structured_logger.module_init(
            "TradingEngine",
            mode=settings.mode.value,
            session_id=self._session_id,
            stocks_count=len(settings.stocks),
        )

    def _signal_handler(self, signum, frame) -> None:
        """시그널 핸들러 (Ctrl+C 등)"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()

    def start(self) -> None:
        """엔진 시작"""
        try:
            structured_logger.module_start("TradingEngine", session_id=self._session_id)

            # 0. 설정 검증
            logger.info("Validating configuration...")
            validation = self._settings.validate()
            if not validation.is_valid:
                for error in validation.errors:
                    logger.error(f"Config error: {error}")
                raise RuntimeError(f"Configuration validation failed: {validation.errors}")
            for warning in validation.warnings:
                logger.warning(f"Config warning: {warning}")

            # 0-1. 이전 크래시 확인 및 복구
            crashed_session = self._recovery_manager.check_previous_crash()
            if crashed_session:
                logger.warning(
                    f"Recovered from previous crash: session={crashed_session.session_id}, "
                    f"active_orders={len(crashed_session.active_orders)}"
                )
                self._slack.send_message(
                    f"⚠️ 이전 세션 크래시 감지\n"
                    f"세션 ID: {crashed_session.session_id}\n"
                    f"마지막 하트비트: {crashed_session.last_heartbeat}\n"
                    f"미처리 주문: {len(crashed_session.active_orders)}건"
                )

            # 1. 인증
            logger.info("Authenticating...")
            if not self._session.authenticate():
                raise RuntimeError("Authentication failed")

            # 2. 토큰 자동 갱신 시작
            self._session.start_auto_refresh()

            # 3. 브로커 초기화
            self._broker = KISBroker(self._session)

            # 3-1. 계좌 잔고 조회 및 출력 (API 연결 확인)
            logger.info("Fetching account balance...")
            self._print_account_balance()

            # 4. 포지션 매니저 초기화 (매매 DB 사용)
            self._position_manager = PositionManager(self._broker, self._trading_db)
            self._position_manager.load_from_db()
            self._position_manager.sync_with_broker()

            # 5. 주문 매니저 초기화 (매매 DB 사용)
            self._order_manager = OrderManager(
                self._broker,
                self._position_manager,
                self._trading_db,
            )
            self._order_manager.set_on_fill_callback(self._on_order_fill)

            # 5-1. 일봉 데이터 로드 (전략 판단용)
            logger.info("Loading daily candle data...")
            self._load_daily_candles()

            # 5-2. 분봉 이력 로드 (초기 데이터 확보)
            logger.info("Loading minute candle history...")
            self._load_minute_candles()

            # 6. 전략 로드
            self._load_strategies()

            # 7. 스케줄러 콜백 설정
            self._scheduler.set_on_stock_tick(self._on_stock_tick)
            self._scheduler.set_on_check_fills(self._on_check_fills)
            self._scheduler.set_on_market_open(self._on_market_open)
            self._scheduler.set_on_market_close(self._on_market_close)
            self._scheduler.set_on_idle(self._on_idle)

            # 8. Slack 시작 알림
            self._slack.notify_start(
                mode=self._settings.mode.value,
                stocks_count=len(self._settings.stocks),
            )

            # 8-1. WebSocket 시작 (실시간 전략용)
            self._start_websocket()

            # 9. 스케줄러 시작
            self._running = True
            self._scheduler.start()

            # 10. 헬스체크 시작
            self._health_checker.register_check(
                "api", create_api_health_check(self._session)
            )
            self._health_checker.register_check(
                "market_db", create_db_health_check(self._market_db)
            )
            self._health_checker.register_check(
                "trading_db", create_db_health_check(self._trading_db)
            )
            self._health_checker.register_check(
                "scheduler", create_scheduler_health_check(self._scheduler)
            )
            self._health_checker.start_background_check()

            # 11. 복구 관리자 세션 시작
            self._recovery_manager.start_session(self._session_id)

            # 12. 긴급 중지 핸들러 설정 및 시작
            emergency_handler = create_emergency_stop_handler(
                order_manager=self._order_manager,
                slack_notifier=self._slack,
                on_stopped=self.stop,
            )
            self._emergency_stop._on_emergency_stop = emergency_handler
            self._emergency_stop.start()

            logger.info("TradingEngine started")

            # 메인 스레드 대기
            while self._running:
                import time
                time.sleep(1)

                # 활성 주문 목록 업데이트 (복구용)
                if self._order_manager:
                    active_orders = [
                        o.order_id for o in self._order_manager.get_active_orders()
                    ]
                    self._recovery_manager.update_active_orders(active_orders)

        except Exception as e:
            logger.error(f"Engine start error: {e}")
            self._slack.notify_error("엔진 시작 오류", str(e))
            raise

    def stop(self) -> None:
        """엔진 중지"""
        if not self._running:
            return

        self._running = False
        logger.info("Stopping TradingEngine...")

        try:
            # 1. 긴급 중지 감시 중지
            self._emergency_stop.stop()

            # 2. 헬스체크 중지
            self._health_checker.stop_background_check()

            # 3. 스케줄러 중지
            self._scheduler.stop()

            # 3-1. WebSocket 중지
            if self._ws_client:
                self._ws_client.stop()
                logger.info("WebSocket client stopped")

            # 4. 미체결 주문 취소
            if self._order_manager:
                cancelled = self._order_manager.cancel_all_pending()
                logger.info(f"Cancelled {cancelled} pending orders")

            # 5. 토큰 갱신 중지
            self._session.stop_auto_refresh()

            # 6. 복구 관리자 세션 종료 (정상 종료 기록)
            self._recovery_manager.stop_session()

            # 7. DB 연결 종료
            self._market_db.close_all()
            self._trading_db.close_all()

            # 8. 시그널 요약 전송
            self._slack.send_signal_summary()

            # 9. Slack 종료 알림
            self._slack.notify_stop()

            logger.info("TradingEngine stopped")
            structured_logger.module_stop("TradingEngine", session_id=self._session_id)

        except Exception as e:
            logger.error(f"Engine stop error: {e}")
            structured_logger.module_error(
                "TradingEngine",
                error=str(e),
                session_id=self._session_id,
            )

    def _load_daily_candles(self) -> None:
        """
        시작 시 일봉 데이터 로드

        각 종목에 대해 최근 100일치 일봉 데이터를 API에서 조회하여
        DB에 저장하고 캐시에 보관
        """
        from datetime import timedelta

        today = datetime.now()
        end_date = today.strftime("%Y%m%d")
        # 100일 전부터 조회 (주말/공휴일 감안하여 충분히)
        start_date = (today - timedelta(days=150)).strftime("%Y%m%d")

        for stock_code in self._settings.stocks.keys():
            try:
                # API에서 일봉 조회
                candle_data = self._broker.get_daily_candles(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                )

                if not candle_data:
                    logger.warning(f"No daily candle data for {stock_code}")
                    continue

                # DB에 저장
                daily_candles: List[DailyCandle] = []
                for data in candle_data:
                    candle = DailyCandle(
                        stock_code=stock_code,
                        trade_date=data["trade_date"],
                        open_price=data["open_price"],
                        high_price=data["high_price"],
                        low_price=data["low_price"],
                        close_price=data["close_price"],
                        volume=data["volume"],
                        trade_amount=data.get("trade_amount"),
                        change_rate=data.get("change_rate"),
                    )
                    daily_candles.append(candle)

                # DB에 배치 저장
                self._daily_repo.upsert_batch(daily_candles)

                # 캐시에 저장 (날짜순 정렬 - 오래된 것이 앞)
                daily_candles.sort(key=lambda x: x.trade_date)
                self._daily_candles_cache[stock_code] = daily_candles

                logger.info(
                    f"Loaded {len(daily_candles)} daily candles for {stock_code}"
                )

            except Exception as e:
                logger.error(f"Failed to load daily candles for {stock_code}: {e}")

    def _load_minute_candles(self) -> None:
        """
        시작 시 분봉 이력 로드 (60개 이상 확보)

        각 종목에 대해 분봉 데이터를 API에서 연속 2회 조회하여 DB에 저장
        ML 전략에 필요한 최소 60개 분봉 데이터를 확보
        """
        for stock_code in self._settings.stocks.keys():
            try:
                total_saved = 0

                # 1차: 현재 시간 기준 30개 로드
                candle_data_1 = self._broker.get_minute_candles(stock_code=stock_code)
                saved_1 = self._save_minute_candles(stock_code, candle_data_1)
                total_saved += saved_1

                # 가장 오래된 분봉의 시간 추출하여 2차 호출
                if candle_data_1:
                    oldest_time = candle_data_1[-1].get("time", "")  # HHMMSS
                    if oldest_time and len(oldest_time) >= 6:
                        # 2차: 이전 시간대 30개 추가 로드
                        candle_data_2 = self._broker.get_minute_candles(
                            stock_code=stock_code,
                            target_hour=oldest_time,
                        )
                        saved_2 = self._save_minute_candles(stock_code, candle_data_2)
                        total_saved += saved_2

                logger.info(
                    f"Loaded {total_saved} minute candles for {stock_code} (trading hours only)"
                )

            except Exception as e:
                logger.error(f"Failed to load minute candles for {stock_code}: {e}")

    def _save_minute_candles(
        self, stock_code: str, candle_data: list
    ) -> int:
        """
        분봉 데이터 DB 저장 (헬퍼 함수)

        Args:
            stock_code: 종목코드
            candle_data: API에서 조회한 분봉 데이터 리스트

        Returns:
            저장된 분봉 개수
        """
        if not candle_data:
            return 0

        saved_count = 0
        for data in candle_data:
            trade_date = data.get("trade_date", "")
            time_str = data.get("time", "")
            if len(trade_date) >= 8 and len(time_str) >= 4:
                # 장중 시간 필터 (09:00 ~ 15:30)
                hour_min = time_str[:4]  # HHMM
                if not ("0900" <= hour_min <= "1530"):
                    continue

                # YYYYMMDD + HHMMSS -> YYYYMMDD_HHMM 형식으로 변환
                minute_key = f"{trade_date}_{hour_min}"
                self._price_repo.upsert_from_api_response(
                    stock_code=stock_code,
                    current_price=data["close_price"],
                    volume=data["volume"],
                    minute_key=minute_key,
                )
                saved_count += 1

        return saved_count

    def _load_strategies(self) -> None:
        """전략 인스턴스 로드"""
        for stock_code, stock_config in self._settings.stocks.items():
            strategies = stock_config.strategies

            for strategy_config in strategies:
                name = strategy_config.get("name")
                params = strategy_config.get("params", {})

                if not name:
                    continue

                strategy = StrategyRegistry.get(name, params)
                if strategy:
                    key = (stock_code, name)
                    self._strategies[key] = strategy
                    logger.debug(f"Strategy loaded: {stock_code} -> {name}")
                else:
                    logger.warning(f"Strategy not found: {name}")

        logger.info(f"Loaded {len(self._strategies)} strategy instances")

    def _print_account_balance(self) -> None:
        """계좌 잔고 조회 및 출력 (API 연결 확인용)"""
        try:
            positions, summary = self._broker.get_balance()

            logger.info("=" * 50)
            logger.info("📊 Account Balance")
            logger.info("=" * 50)

            # 계좌 요약
            if summary:
                deposit = summary.get("deposit", 0)
                total_eval = summary.get("total_eval", 0)
                total_pl = summary.get("total_profit_loss", 0)

                logger.info(f"  Deposit:      {deposit:>15,} KRW")
                logger.info(f"  Total Eval:   {total_eval:>15,} KRW")
                logger.info(f"  Total P/L:    {total_pl:>+15,} KRW")

            # 보유 종목
            if positions:
                logger.info("-" * 50)
                logger.info("  Holdings:")
                for pos in positions:
                    pl_sign = "+" if pos.profit_loss >= 0 else ""
                    logger.info(
                        f"    {pos.stock_name} ({pos.stock_code}): "
                        f"{pos.quantity}주 @ {pos.avg_price:,.0f} → "
                        f"{pos.current_price:,} ({pl_sign}{pos.profit_rate:.2f}%)"
                    )
            else:
                logger.info("  No holdings")

            logger.info("=" * 50)
            logger.info("✅ API connection verified")

        except Exception as e:
            logger.error(f"Failed to fetch balance: {e}")
            raise RuntimeError(f"API connection failed: {e}")

    def _on_check_fills(self) -> None:
        """체결 확인 콜백 (병렬 틱 처리 전 1회 호출)"""
        try:
            self._order_manager.check_fills()
        except Exception as e:
            logger.error(f"Check fills error: {e}")

    def _on_order_fill(self, order: ManagedOrder, filled_qty: int) -> None:
        """체결 콜백 - 슬랙 알림 전송"""
        try:
            # 손익 계산 (매도인 경우)
            profit_loss = 0
            profit_rate = 0.0

            if order.side == OrderSide.SELL:
                position = self._position_manager.get_position(order.stock_code)
                if position:
                    profit_loss = int(
                        (order.filled_price - position.avg_price) * filled_qty
                    )
                    if position.avg_price > 0:
                        profit_rate = (
                            (order.filled_price - position.avg_price)
                            / position.avg_price
                            * 100
                        )

            # 전략 승률 가져오기
            win_rate = None
            if order.strategy_name:
                win_rate = self._settings.get_strategy_win_rate(
                    order.stock_code, order.strategy_name
                )

            self._slack.notify_fill(
                fill_type=order.side.value,
                stock_code=order.stock_code,
                stock_name=order.stock_name,
                quantity=filled_qty,
                price=order.filled_price,
                strategy_name=order.strategy_name or "",
                profit_loss=profit_loss,
                profit_rate=profit_rate,
                strategy_win_rate=win_rate,
            )
        except Exception as e:
            logger.error(f"Order fill notification error: {e}")

    # ===== WebSocket 관련 메서드 =====

    def _start_websocket(self) -> None:
        """WebSocket 연결 시작 (별도 스레드)"""
        ws_stock_codes = self._get_ws_strategy_stocks()
        if not ws_stock_codes:
            logger.info("No WebSocket strategies configured, skipping WebSocket")
            return

        self._ws_stock_codes = ws_stock_codes
        self._ws_client = RealtimeWSClient(
            on_tick=self._on_ws_tick,
            on_error=self._on_ws_error,
        )
        self._ws_client.start(list(ws_stock_codes))
        logger.info(f"WebSocket started for {len(ws_stock_codes)} stocks: {ws_stock_codes}")

    def _get_ws_strategy_stocks(self) -> Set[str]:
        """WebSocket 전략이 설정된 종목 목록 조회"""
        ws_stocks = set()
        for stock_code, stock_config in self._settings.stocks.items():
            for strategy_config in stock_config.strategies:
                # execution_mode가 "websocket"인 전략 찾기
                if strategy_config.get("execution_mode") == "websocket":
                    ws_stocks.add(stock_code)
                    break
        return ws_stocks

    def _on_ws_error(self, error: Exception) -> None:
        """WebSocket 에러 콜백"""
        logger.error(f"WebSocket error: {error}")
        self._slack.notify_error("WebSocket 에러", str(error))

    def _on_ws_tick(self, tick_data: TickData) -> None:
        """
        WebSocket 체결 데이터 콜백

        실시간 전략(execution_mode="websocket")만 실행
        기존 _on_stock_tick과 유사하지만:
        - REST API 대신 WebSocket 데이터 사용
        - WebSocket 전략만 실행
        """
        with self._tick_lock:
            try:
                stock_code = tick_data.stock_code
                now = tick_data.timestamp

                # WebSocket 전략 종목 확인
                if stock_code not in self._ws_stock_codes:
                    return

                stock_config = self._settings.stocks.get(stock_code)
                if not stock_config:
                    return

                # 현재가 로그
                stock_name = stock_config.name
                change_sign = "+" if tick_data.change >= 0 else ""
                logger.debug(
                    f"[WS][{stock_name}] 체결: {tick_data.price:,}원 "
                    f"({change_sign}{tick_data.change_rate:.2f}%)"
                )

                # DB 저장 (분봉 upsert)
                minute_key = get_current_minute_key(now)
                self._price_repo.upsert_from_api_response(
                    stock_code=stock_code,
                    current_price=tick_data.price,
                    volume=tick_data.accumulated_volume,
                    minute_key=minute_key,
                )

                # 중복 주문 방지
                if self._order_manager.has_pending_order(stock_code):
                    logger.debug(f"[WS][{stock_code}] 미체결 주문 존재 - 시그널 생성 스킵")
                    return

                # WebSocket 전략만 실행
                strategies = stock_config.strategies
                if not strategies:
                    return

                # 가격 히스토리 로드 (분봉)
                price_history = self._price_repo.get_recent_prices(stock_code, count=60)

                # 일봉 데이터 로드 (캐시에서)
                daily_candles = self._daily_candles_cache.get(stock_code, [])

                # 현재 포지션
                position = self._position_manager.get_position(stock_code)
                broker_position = self._get_broker_position(stock_code)

                for strategy_config in strategies:
                    # WebSocket 전략만 실행
                    if strategy_config.get("execution_mode") != "websocket":
                        continue

                    strategy_name = strategy_config.get("name")
                    key = (stock_code, strategy_name)
                    strategy = self._strategies.get(key)

                    if not strategy:
                        logger.warning(f"[WS][{stock_code}] 전략 '{strategy_name}' 인스턴스 없음")
                        continue

                    # 포지션 보유 시 해당 전략으로만 매도 가능
                    if position and position.strategy_name != strategy_name:
                        logger.debug(
                            f"[WS][{stock_code}] 포지션 전략({position.strategy_name}) != "
                            f"현재 전략({strategy_name}) - 스킵"
                        )
                        continue

                    logger.debug(f"[WS][{stock_code}] 전략 '{strategy_name}' 실행 시작")

                    # 전략 컨텍스트 생성
                    context = StrategyContext(
                        stock_code=stock_code,
                        stock_name=stock_config.name,
                        current_price=tick_data.price,
                        current_time=now,
                        price_history=price_history,
                        position=broker_position,
                        daily_candles=daily_candles,
                        today_trade_count=self._order_manager.get_today_trade_count(
                            stock_code
                        ),
                    )

                    # 시그널 생성 가능 여부 확인
                    if not strategy.can_generate_signal(context):
                        continue

                    # 시그널 생성
                    signal = strategy.generate_signal(context)

                    # 시그널 처리
                    self._process_signal(signal, context, strategy)

            except Exception as e:
                logger.error(f"WebSocket tick error [{tick_data.stock_code}]: {e}")

    # ===== 스케줄러 기반 메서드 =====

    def _on_stock_tick(self, stock_code: str, now: datetime) -> None:
        """
        종목 틱 콜백 (스케줄러 기반 전략용)

        1. 현재가 조회
        2. DB 저장
        3. 전략별 시그널 생성
        4. 주문 실행

        Note: 체결 확인은 스케줄러에서 병렬 처리 전 1회 호출
        """
        with self._tick_lock:
            try:
                # 1. 현재가 조회
                price_info = self._broker.get_current_price(stock_code)
                if not price_info:
                    logger.warning(f"Failed to get price: {stock_code}")
                    return

                # 현재가 로그 출력
                stock_config = self._settings.stocks.get(stock_code)
                stock_name = stock_config.name if stock_config else stock_code
                change_sign = "+" if price_info.change >= 0 else ""
                logger.info(
                    f"[{stock_name}] 현재가: {price_info.current_price:,}원 "
                    f"({change_sign}{price_info.change_rate:.2f}%)"
                )

                # 2. DB 저장 (분봉 upsert)
                minute_key = get_current_minute_key(now)
                self._price_repo.upsert_from_api_response(
                    stock_code=stock_code,
                    current_price=price_info.current_price,
                    volume=price_info.volume,
                    minute_key=minute_key,
                )

                # 3. 중복 주문 방지
                if self._order_manager.has_pending_order(stock_code):
                    return

                # 4. 전략별 시그널 생성
                stock_config = self._settings.stocks.get(stock_code)
                if not stock_config:
                    return

                strategies = stock_config.strategies

                if not strategies:
                    # 전략 없음 → 가격만 저장
                    return

                # 가격 히스토리 로드 (분봉)
                price_history = self._price_repo.get_recent_prices(stock_code, count=60)

                # 일봉 데이터 로드 (캐시에서)
                daily_candles = self._daily_candles_cache.get(stock_code, [])

                # 현재 포지션
                position = self._position_manager.get_position(stock_code)
                broker_position = self._get_broker_position(stock_code)

                for strategy_config in strategies:
                    # WebSocket 전략은 스킵 (별도 처리)
                    if strategy_config.get("execution_mode") == "websocket":
                        continue

                    strategy_name = strategy_config.get("name")
                    key = (stock_code, strategy_name)
                    strategy = self._strategies.get(key)

                    if not strategy:
                        continue

                    # 포지션 보유 시 해당 전략으로만 매도 가능
                    if position and position.strategy_name != strategy_name:
                        continue

                    # 전략 컨텍스트 생성
                    context = StrategyContext(
                        stock_code=stock_code,
                        stock_name=stock_config.name,
                        current_price=price_info.current_price,
                        current_time=now,
                        price_history=price_history,
                        position=broker_position,
                        daily_candles=daily_candles,
                        today_trade_count=self._order_manager.get_today_trade_count(
                            stock_code
                        ),
                    )

                    # 시그널 생성 가능 여부 확인 (데이터 충분성, 가격 유효성)
                    if not strategy.can_generate_signal(context):
                        validation = context.validate_price_data()
                        if not validation.is_valid:
                            logger.warning(
                                f"[{stock_code}] Cannot generate signal: {validation.errors}"
                            )
                        continue

                    # 시그널 생성
                    signal = strategy.generate_signal(context)

                    # 시그널 처리
                    self._process_signal(signal, context, strategy)

            except Exception as e:
                logger.error(f"Stock tick error [{stock_code}]: {e}")

    def _get_broker_position(self, stock_code: str) -> Optional[Position]:
        """브로커에서 Position 객체 조회"""
        managed = self._position_manager.get_position(stock_code)
        if managed:
            return Position(
                stock_code=managed.stock_code,
                stock_name=managed.stock_name,
                quantity=managed.quantity,
                avg_price=managed.avg_price,
                current_price=managed.current_price,
                eval_amount=managed.eval_amount,
                profit_loss=managed.profit_loss,
                profit_rate=managed.profit_rate,
            )
        return None

    def _process_signal(
        self,
        signal: TradingSignal,
        context: StrategyContext,
        strategy: BaseStrategy,
    ) -> None:
        """시그널 처리"""
        if signal.is_hold:
            return

        stock_code = signal.stock_code
        stock_name = context.stock_name

        if signal.is_buy:
            # 매수 시그널
            logger.info(f"[{stock_code}] 매수 시그널 처리 시작: {signal.reason}")
            strategy.on_entry(context, signal)

            # 전략 승률 및 allocation 가져오기
            win_rate = self._settings.get_strategy_win_rate(stock_code, strategy.name)
            allocation = self._settings.get_strategy_allocation(stock_code, strategy.name)

            # 매수 가능 수량 조회 (종목증거금율 반영, 가장 정확)
            max_buyable_qty = self._broker.get_buyable_quantity(stock_code)
            if max_buyable_qty > 0:
                # allocation 비율 적용
                quantity = int(max_buyable_qty * (allocation / 100))
                if quantity < 1:
                    logger.warning(f"[{stock_code}] 계산된 수량 0 → 최소 1주로 설정")
                    quantity = 1
                logger.info(
                    f"[{stock_code}] 매수 수량 계산: {quantity}주 "
                    f"(매수가능: {max_buyable_qty}주, allocation: {allocation}%)"
                )
            else:
                quantity = signal.quantity
                logger.warning(f"[{stock_code}] 매수가능수량 조회 실패 → 시그널 수량 사용: {quantity}주")

            # 시그널 알림 (주문 전)
            self._slack.notify_signal(
                signal_type="BUY",
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=quantity,
                price=context.current_price,
                strategy_name=strategy.name,
                reason=signal.reason,
                strategy_win_rate=win_rate,
            )

            order_id = self._order_manager.place_buy_order(
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=quantity,
                strategy_name=strategy.name,
            )

            if order_id:
                logger.info(f"[{stock_code}] 매수 주문 성공: {order_id}")
                self._slack.notify_buy(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    quantity=quantity,
                    price=context.current_price,
                    strategy_name=strategy.name,
                    reason=signal.reason,
                    strategy_win_rate=win_rate,
                )
            else:
                logger.warning(f"[{stock_code}] 매수 주문 실패 (order_manager 반환값 None)")

        elif signal.is_sell:
            # 매도 시그널
            strategy.on_exit(context, signal)

            # 전략 승률 가져오기
            win_rate = self._settings.get_strategy_win_rate(stock_code, strategy.name)

            # 시그널 알림 (주문 전)
            self._slack.notify_signal(
                signal_type="SELL",
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=signal.quantity,
                price=context.current_price,
                strategy_name=strategy.name,
                reason=signal.reason,
                strategy_win_rate=win_rate,
            )

            # 손익 계산
            position = context.position
            profit_loss = 0
            profit_rate = 0.0

            if position:
                profit_loss = int(
                    (context.current_price - position.avg_price) * signal.quantity
                )
                profit_rate = context.profit_rate

            order_id = self._order_manager.place_sell_order(
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=signal.quantity,
                strategy_name=strategy.name,
            )

            if order_id:
                self._slack.notify_sell(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    quantity=signal.quantity,
                    price=context.current_price,
                    profit_loss=profit_loss,
                    profit_rate=profit_rate,
                    strategy_name=strategy.name,
                    reason=signal.reason,
                    strategy_win_rate=win_rate,
                )

    def _on_market_open(self) -> None:
        """장 시작 콜백"""
        logger.info("Market opened - syncing positions")

        # 정규장 시작 알림
        self._slack.send_market_open_notification()

        # 포지션 동기화
        self._position_manager.sync_with_broker()

    def _on_market_close(self) -> None:
        """장 마감 콜백"""
        logger.info("Market closed")

        try:
            # 1. 미체결 주문 취소
            cancelled = self._order_manager.cancel_all_pending()
            logger.info(f"Cancelled {cancelled} pending orders at market close")

            # 2. 일일 리포트 생성 및 전송
            report = self._report_generator.generate_and_send()
            logger.info(
                f"Daily report: {report.total_trades} trades, "
                f"PnL: {report.realized_pnl:,}원"
            )

            # 3. 시그널 요약 전송
            self._slack.send_signal_summary()

        except Exception as e:
            logger.error(f"Market close error: {e}")
            self._slack.notify_error("장 마감 처리 오류", str(e))

    def _on_idle(self) -> None:
        """장외 대기 콜백"""
        # 필요시 상태 로깅
        status = self._scheduler.get_status()
        logger.debug(f"Idle - {status.get('time_until_open', 'N/A')} until market open")

    def get_status(self) -> Dict:
        """엔진 상태 조회"""
        return {
            "running": self._running,
            "mode": self._settings.mode.value,
            "scheduler": self._scheduler.get_status(),
            "positions": len(self._position_manager.get_all_positions()) if self._position_manager else 0,
            "active_orders": len(self._order_manager.get_active_orders()) if self._order_manager else 0,
            "strategies": len(self._strategies),
            "session_id": self._session_id,
            "health": self._health_checker.get_last_health().to_dict() if self._health_checker.get_last_health() else None,
        }

    def _on_health_change(self, health) -> None:
        """
        헬스 상태 변경 콜백

        Args:
            health: SystemHealth 객체
        """
        from leverage_worker.core.health_checker import HealthStatus

        if health.overall_status == HealthStatus.UNHEALTHY:
            # 심각한 상태 - 알림 전송
            unhealthy_components = [
                name for name, comp in health.components.items()
                if comp.status == HealthStatus.UNHEALTHY
            ]
            logger.error(f"System UNHEALTHY: {unhealthy_components}")

            self._slack.send_alert(
                title="🚨 시스템 헬스 이상",
                message=f"비정상 컴포넌트: {', '.join(unhealthy_components)}\n"
                        f"세션 ID: {self._session_id}",
                level="critical",
            )

            structured_logger.log(
                LogEventType.HEALTH_CHECK,
                "TradingEngine",
                f"System unhealthy: {unhealthy_components}",
                level="ERROR",
                unhealthy_components=unhealthy_components,
                session_id=self._session_id,
            )

        elif health.overall_status == HealthStatus.DEGRADED:
            # 저하 상태 - 경고 로깅
            degraded_components = [
                name for name, comp in health.components.items()
                if comp.status == HealthStatus.DEGRADED
            ]
            logger.warning(f"System DEGRADED: {degraded_components}")

            structured_logger.log(
                LogEventType.HEALTH_CHECK,
                "TradingEngine",
                f"System degraded: {degraded_components}",
                level="WARNING",
                degraded_components=degraded_components,
                session_id=self._session_id,
            )

    def _on_crash_detected(self, crashed_session) -> None:
        """
        크래시 감지 콜백

        Args:
            crashed_session: SessionState 객체
        """
        logger.warning(
            f"Previous crash detected - session: {crashed_session.session_id}, "
            f"last heartbeat: {crashed_session.last_heartbeat}"
        )

        structured_logger.log(
            LogEventType.RECOVERY_START,
            "TradingEngine",
            f"Crash recovery initiated for session {crashed_session.session_id}",
            level="WARNING",
            crashed_session_id=crashed_session.session_id,
            active_orders_count=len(crashed_session.active_orders),
            positions_count=len(crashed_session.positions),
        )

        # 미처리 주문이 있었으면 동기화 필요
        if crashed_session.active_orders:
            logger.info(
                f"Found {len(crashed_session.active_orders)} unprocessed orders from crashed session"
            )
