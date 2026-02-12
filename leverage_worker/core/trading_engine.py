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
from collections import deque
from datetime import datetime
from typing import Dict, List, Optional, Set

from leverage_worker.config.settings import Settings, TradingMode
from leverage_worker.core.daily_liquidation import DailyLiquidationManager, LiquidationResult
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
from leverage_worker.utils.logger import get_logger, attach_slack_handler
from leverage_worker.utils.log_constants import LogEventType
from leverage_worker.utils.math_utils import calculate_allocation_amount
from leverage_worker.utils.structured_logger import get_structured_logger
from leverage_worker.scalping.executor import ScalpingExecutor
from leverage_worker.scalping.models import ScalpingConfig
from leverage_worker.websocket import ExitMonitor, ExitMonitorConfig, OrderNoticeData, RealtimeWSClient, TickData

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

        # ERROR 로그 Slack 전송 핸들러 연결
        attach_slack_handler(self._slack)

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

        # 15. 실시간 매도 모니터링 (realtime_exit: true 전략용)
        self._exit_monitor: Optional[ExitMonitor] = None

        # 15-1. 스캘핑 실행기: (stock_code, strategy_name) -> ScalpingExecutor
        self._scalping_executors: Dict[tuple, ScalpingExecutor] = {}

        # 16. 동시성 제어 (스케줄러/WebSocket 공유 리소스 보호)
        self._tick_lock = threading.Lock()
        self._check_fills_lock = threading.Lock()
        self._pnl_lock = threading.Lock()
        self._pending_fill_signals: deque = deque()  # thread-safe FIFO

        # 17. Daily Liquidation Manager
        self._liquidation_manager: Optional["DailyLiquidationManager"] = None

        # 18. 청산 진행 플래그 (전략 실행 skip용)
        self._liquidation_in_progress = False

        # 19. Prefetch 캐시 (예수금 사전 조회)
        self._prefetch_cache: Dict[str, tuple[int, datetime]] = {}  # stock_code -> (deposit, timestamp)
        self._prefetch_cache_ttl: int = settings.get_prefetch_cache_ttl()
        self._buy_fee_rate: float = settings.get_buy_fee_rate()

        # 세션 ID
        self._session_id = str(uuid.uuid4())[:8]

        # 당일 누적 실현손익 (DB에서 복구 - 장중 재시작 대응)
        self._daily_realized_pnl: int = self._report_generator.get_today_realized_pnl()
        logger.info(f"Daily realized PnL initialized: {self._daily_realized_pnl:,}원")

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

            # 5-1. Daily Liquidation Manager 초기화
            self._liquidation_manager = DailyLiquidationManager(
                order_manager=self._order_manager,
                position_manager=self._position_manager,
                slack_notifier=self._slack,
            )
            logger.info("DailyLiquidationManager initialized")

            # 5-2. 일봉 데이터 로드 (전략 판단용)
            logger.info("Loading daily candle data...")
            self._load_daily_candles()

            # 5-3. 분봉 이력 로드 (초기 데이터 확보)
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
            self._scheduler.set_on_prefetch_tick(
                self._on_prefetch_tick,
                self._settings.get_prefetch_second(),
            )
            logger.info(
                f"Prefetch tick registered (second={self._settings.get_prefetch_second()}, "
                f"ttl={self._prefetch_cache_ttl}s, fee_rate={self._buy_fee_rate:.4%})"
            )

            # 7-1. 15:19 당일 청산 콜백 등록
            self._scheduler.register_specific_time_callback("15:19", self._on_daily_liquidation)
            logger.info("Registered 15:19 daily liquidation callback")

            # 8. Slack 시작 알림
            self._slack.notify_start(
                mode=self._settings.mode.value,
                stocks_count=len(self._settings.stocks),
            )

            # 8-1. WebSocket 시작 (실시간 전략용)
            self._start_websocket()

            # 8-2. 실시간 매도 모니터링 시작
            self._start_exit_monitor()

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

            # 3-2. 실시간 매도 모니터링 중지
            if self._exit_monitor:
                self._exit_monitor.stop()
                logger.info("Exit monitor stopped")

            # 3-3. 스캘핑 executor 중지
            for _key, executor in self._scalping_executors.items():
                if executor.is_active:
                    executor.deactivate()
            if self._scalping_executors:
                logger.info(
                    f"Scalping executors deactivated: "
                    f"{len(self._scalping_executors)}"
                )

            # 4. 미체결 주문 취소
            if self._order_manager:
                cancelled = self._order_manager.cancel_all_pending()
                logger.info(f"Cancelled {cancelled} pending orders")

            # 5. 토큰 갱신 중지
            self._session.stop_auto_refresh()

            # 6. 복구 관리자 세션 종료 (정상 종료 기록)
            self._recovery_manager.stop_session()

            # 7. 일일 리포트 생성 및 전송 (DB 종료 전)
            try:
                report = self._report_generator.generate_and_send()
                logger.info(
                    f"Daily report on stop: {report.total_trades} trades, "
                    f"PnL: {report.realized_pnl:,}원"
                )
            except Exception as e:
                logger.error(f"Daily report error on stop: {e}")

            # 8. DB 연결 종료
            self._market_db.close_all()
            self._trading_db.close_all()

            # 9. 시그널 요약 전송
            self._slack.send_signal_summary()

            # 10. Slack 종료 알림
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
        failed_strategies = []

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

                    # 스캘핑 전략의 경우 ScalpingExecutor 생성
                    if strategy_config.get("execution_mode") == "scalping":
                        scalping_config = ScalpingConfig.from_params(params)
                        allocation = float(strategy_config.get("allocation", 100))
                        executor = ScalpingExecutor(
                            stock_code=stock_code,
                            stock_name=stock_config.name,
                            config=scalping_config,
                            broker=self._broker,
                            strategy_name=name,
                            allocation=allocation,
                            ws_client=self._ws_client,
                            slack_notifier=self._slack,
                            position_manager=self._position_manager,
                            trading_db=self._trading_db,
                            report_generator=self._report_generator,
                        )
                        self._scalping_executors[key] = executor
                        logger.info(
                            f"ScalpingExecutor created: {stock_code} -> {name}"
                        )

                    # ML 전략의 경우 모델 로드 미리 시도
                    if hasattr(strategy, "_ensure_model_loaded"):
                        if not strategy._ensure_model_loaded():
                            failed_strategies.append((stock_code, name))
                else:
                    logger.warning(f"Strategy not found: {name}")
                    failed_strategies.append((stock_code, name))

        logger.info(f"Loaded {len(self._strategies)} strategy instances")

        # 실패한 전략이 있으면 Slack 알림
        if failed_strategies:
            failed_list = "\n".join([f"- {code}: {name}" for code, name in failed_strategies])
            self._slack.notify_error(
                "전략 로드 실패",
                f"다음 전략이 로드되지 않았습니다:\n{failed_list}"
            )

    def _print_account_balance(self) -> None:
        """계좌 잔고 조회 및 출력 (API 연결 확인용)"""
        try:
            positions, summary = self._broker.get_balance()

            logger.info("=" * 50)
            logger.info("📊 Account Balance")
            logger.info("=" * 50)

            # balance 조회 실패 시 (summary가 비어있으면 API 에러)
            if not summary:
                logger.warning(
                    f"Account verification failed (first attempt) - "
                    f"CANO: '{self._settings.account_number}', "
                    f"ACNT_PRDT_CD: '{self._settings.account_product_code}', "
                    f"Mode: {self._settings.mode.value}"
                )
                logger.warning("Attempting token refresh and retry...")

                # 토큰 재발급 후 재시도 (1회)
                if self._session.force_reauthenticate():
                    positions, summary = self._broker.get_balance()

                if not summary:
                    logger.error(
                        f"Account verification failed after token refresh - "
                        f"CANO: '{self._settings.account_number}', "
                        f"ACNT_PRDT_CD: '{self._settings.account_product_code}', "
                        f"Mode: {self._settings.mode.value}"
                    )
                    logger.error("=" * 50)
                    raise RuntimeError(
                        "Account verification failed (INVALID_CHECK_ACNO). "
                        "Check account_number in credentials YAML. "
                        "For paper trading, ensure the account is registered for 모의투자."
                    )

            # 계좌 요약
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

        except RuntimeError:
            raise
        except Exception as e:
            logger.error(f"Failed to fetch balance: {e}")
            raise RuntimeError(f"API connection failed: {e}")

    def _on_check_fills(self) -> None:
        """체결 확인 콜백 (병렬 틱 처리 전 1회 호출)"""
        # 1. 큐에 쌓인 fill signal 처리 (WS on_fill 콜백에서 발생)
        self._process_pending_fill_signals()

        # 2. WebSocket 체결통보가 활성이면 REST 폴링 불필요
        if self._ws_client and self._ws_client.is_order_notice_active:
            return

        # 3. REST 폴백 (WS 비정상 시)
        if not self._check_fills_lock.acquire(blocking=False):
            logger.debug("check_fills already running, skipping")
            return
        try:
            self._order_manager.check_fills()
        except Exception as e:
            logger.error(f"Check fills error: {e}")
        finally:
            self._check_fills_lock.release()

    def _on_order_fill(
        self, order: ManagedOrder, filled_qty: int, avg_price: float = 0.0
    ) -> None:
        """체결 콜백 - 슬랙 알림 전송 및 ExitMonitor 등록/해제

        Args:
            order: 체결된 주문
            filled_qty: 체결 수량
            avg_price: 손익 계산용 평균 매입가 (매도 시 position 삭제 전 저장된 값)
        """
        try:
            # 손익 계산 (매도인 경우)
            profit_loss = 0
            profit_rate = 0.0

            # 체결가 방어: filled_price가 0이면 avg_price 또는 주문가로 fallback
            fill_price = order.filled_price
            if fill_price <= 0:
                fill_price = int(avg_price) if avg_price > 0 else order.price
                logger.warning(
                    f"[{order.stock_code}] filled_price=0 → fallback: {fill_price:,}원"
                )

            if order.side == OrderSide.SELL:
                # 전달받은 avg_price로 손익 계산 (position 삭제 전 저장된 값)
                if avg_price > 0:
                    profit_loss = int((fill_price - avg_price) * filled_qty)
                    profit_rate = (
                        (fill_price - avg_price) / avg_price * 100
                    )

                # 당일 누적 실현손익 업데이트
                with self._pnl_lock:
                    self._daily_realized_pnl += profit_loss

                # 매도 체결 시 ExitMonitor 해제
                self._unregister_position_from_exit_monitor(order.stock_code)

            elif order.side == OrderSide.BUY:
                # 매수 체결 시 ExitMonitor 등록 (realtime_exit: true인 경우)
                self._register_position_for_exit_monitor(order, filled_qty)

            # 전략 승률 가져오기
            win_rate = None
            if order.strategy_name:
                win_rate = self._settings.get_strategy_win_rate(
                    order.stock_code, order.strategy_name
                )

            # PnL 읽기 시 lock
            with self._pnl_lock:
                daily_pnl = self._daily_realized_pnl if order.side == OrderSide.SELL else None

            self._slack.notify_fill(
                fill_type=order.side.value,
                stock_code=order.stock_code,
                stock_name=order.stock_name,
                quantity=filled_qty,
                price=fill_price,
                strategy_name=order.strategy_name or "",
                profit_loss=profit_loss,
                profit_rate=profit_rate,
                strategy_win_rate=win_rate,
                daily_cumulative_pnl=daily_pnl,
                total_filled=order.filled_qty,
                order_quantity=order.original_quantity or order.quantity,
            )

            # 체결 완료 시 시그널 기록 리셋 (다음 시그널도 알림 받기 위함)
            if order.strategy_name:
                self._slack.reset_signal_for_key(order.stock_code, order.strategy_name)

            # 전략 on_fill 콜백 → 후속 시그널 큐잉
            self._notify_strategy_fill(order, filled_qty, avg_price)

        except Exception as e:
            logger.error(f"Order fill notification error: {e}")

    def _notify_strategy_fill(
        self, order: ManagedOrder, filled_qty: int, avg_price: float
    ) -> None:
        """전략에 체결 통보 → 후속 시그널 큐잉"""
        if not order.strategy_name:
            return
        strategy_key = (order.stock_code, order.strategy_name)
        strategy = self._strategies.get(strategy_key)
        if not strategy:
            return
        try:
            # 최소 컨텍스트 생성
            position = self._position_manager.get_position(order.stock_code)
            context = StrategyContext(
                stock_code=order.stock_code,
                stock_name=order.stock_name,
                current_price=order.filled_price,
                current_time=datetime.now(),
                price_history=[],  # 체결 시점에는 미제공
                position=position,
            )
            signal_info = TradingSignal(
                signal_type=order.side.value,
                stock_code=order.stock_code,
                quantity=filled_qty,
                reason=f"fill_callback:{order.order_id}",
            )
            result = strategy.on_fill(context, signal_info, order.filled_price, filled_qty)
            if result and not result.is_hold:
                # EC-6: WS 스레드에서 직접 주문하면 블로킹 → 큐에 저장
                self._pending_fill_signals.append((result, context, strategy))
                logger.info(
                    f"[on_fill] {order.stock_code} 후속 시그널 큐잉: "
                    f"{result.signal_type} x{result.quantity}"
                )
        except Exception as e:
            logger.error(f"Strategy on_fill error: {e}")

    def _process_pending_fill_signals(self) -> None:
        """큐에 쌓인 fill 후속 시그널 처리 (scheduler 스레드에서 호출)"""
        while self._pending_fill_signals:
            try:
                signal_data, context, strategy = self._pending_fill_signals.popleft()
                logger.info(
                    f"[on_fill 후속] {signal_data.stock_code} "
                    f"{signal_data.signal_type} x{signal_data.quantity} 처리"
                )
                self._process_signal(signal_data, context, strategy)
            except IndexError:
                break
            except Exception as e:
                logger.error(f"Pending fill signal processing error: {e}")

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
            on_order_notice=self._on_ws_order_notice,
            is_paper=self._settings.mode == TradingMode.PAPER,
            hts_id=self._settings.hts_id,
        )
        self._ws_client.start(list(ws_stock_codes))
        logger.info(f"WebSocket started for {len(ws_stock_codes)} stocks: {ws_stock_codes}")

    def _get_ws_strategy_stocks(self) -> Set[str]:
        """WebSocket 전략이 설정된 종목 목록 조회 (websocket + scalping 모두 포함)"""
        ws_stocks = set()
        for stock_code, stock_config in self._settings.stocks.items():
            for strategy_config in stock_config.strategies:
                mode = strategy_config.get("execution_mode")
                if mode in ("websocket", "scalping"):
                    ws_stocks.add(stock_code)
                    break
        return ws_stocks

    def _on_ws_error(self, error: Exception) -> None:
        """WebSocket 에러 콜백"""
        logger.error(f"WebSocket error: {error}")
        self._slack.notify_error("WebSocket 에러", str(error))

    def _on_ws_order_notice(self, notice: OrderNoticeData) -> None:
        """WebSocket 체결통보 수신 콜백 - OrderManager + Scalping Executor 라우팅"""
        try:
            logger.info(
                f"[WS 체결통보] {notice.stock_code} "
                f"주문번호={notice.order_no} 체결수량={notice.filled_qty}"
            )

            # 1. Try OrderManager first (regular orders)
            order = self._order_manager.process_ws_fill(
                order_no=notice.order_no,
                filled_qty=notice.filled_qty,
                filled_price=notice.filled_price,
            )

            if order:
                return  # Handled by OrderManager

            # 2. Route to ScalpingExecutor
            for key, executor in self._scalping_executors.items():
                if executor.is_active:
                    handled = executor.process_ws_fill(
                        notice.order_no,
                        notice.filled_qty,
                        notice.filled_price,
                    )
                    if handled:
                        logger.info(
                            f"[WS 체결] ScalpingExecutor handled: "
                            f"{key[0]} {key[1]}"
                        )
                        return

            # 3. Order not found (cancelled/outdated)
            logger.debug(
                f"[WS 체결] Order {notice.order_no} not found in active orders"
            )

        except Exception as e:
            logger.error(f"Order notice handling error: {e}")

    # ===== 실시간 매도 모니터링 (ExitMonitor) =====

    def _start_exit_monitor(self) -> None:
        """실시간 매도 모니터링 시작"""
        self._exit_monitor = ExitMonitor(
            on_exit_signal=self._on_exit_monitor_signal,
            is_paper=self._settings.mode == TradingMode.PAPER,
        )
        self._exit_monitor.start()

        # 기존 포지션에 대해 모니터링 등록
        self._register_existing_positions_for_exit_monitor()
        logger.info("[ExitMonitor] Initialized")

    def _register_existing_positions_for_exit_monitor(self) -> None:
        """기존 포지션에 대해 매도 모니터링 등록"""
        if not self._exit_monitor:
            return

        positions = self._position_manager.get_all_positions()
        for stock_code, position in positions.items():
            if not position.strategy_name:
                continue

            # realtime_exit: true 설정된 전략만
            stock_config = self._settings.stocks.get(stock_code)
            if not stock_config:
                continue

            for strategy_config in stock_config.strategies:
                if strategy_config.get("name") != position.strategy_name:
                    continue

                # realtime_exit 확인
                if not strategy_config.get("realtime_exit", False):
                    continue

                params = strategy_config.get("params", {})
                # 기존 포지션은 signal_price를 알 수 없으므로 avg_price 사용
                config = ExitMonitorConfig(
                    stock_code=stock_code,
                    strategy_name=position.strategy_name,
                    avg_price=position.avg_price,
                    quantity=position.quantity,
                    entry_time=position.entry_time or datetime.now(),
                    take_profit_pct=params.get("take_profit_pct", 0.003),
                    stop_loss_pct=params.get("stop_loss_pct", 0.01),
                    max_holding_minutes=params.get("max_holding_minutes", 60),
                    signal_price=position.avg_price,  # 기존 포지션은 avg_price로 대체
                )
                self._exit_monitor.add_position(config)
                logger.info(f"[ExitMonitor] Registered existing position: {stock_code}")
                break

    def _on_exit_monitor_signal(
        self,
        stock_code: str,
        strategy_name: str,
        quantity: int,
        reason: str,
        is_take_profit: bool,
    ) -> None:
        """실시간 매도 모니터링 시그널 콜백"""
        with self._tick_lock:
            try:
                # 중복 주문 방지
                if self._order_manager.has_pending_order(stock_code):
                    logger.warning(f"[ExitMonitor] {stock_code} 미체결 주문 존재 - 스킵")
                    return

                stock_config = self._settings.stocks.get(stock_code)
                stock_name = stock_config.name if stock_config else stock_code

                position = self._position_manager.get_position(stock_code)
                if not position:
                    logger.warning(f"[ExitMonitor] {stock_code} 포지션 없음 - 스킵")
                    if self._exit_monitor:
                        self._exit_monitor.remove_position(stock_code)
                    return

                # 현재가 조회
                price_info = self._broker.get_current_price(stock_code)
                current_price = price_info.current_price if price_info else position.current_price

                # 전략 승률 가져오기
                win_rate = self._settings.get_strategy_win_rate(stock_code, strategy_name)

                # Slack 시그널 알림
                self._slack.notify_signal(
                    signal_type="SELL",
                    stock_code=stock_code,
                    stock_name=stock_name,
                    quantity=quantity,
                    price=current_price,
                    strategy_name=strategy_name,
                    reason=f"[실시간] {reason}",
                    strategy_win_rate=win_rate,
                )

                order_id = None
                if is_take_profit:
                    # TP: 매수1호가 지정가 매도 (1초 후 미체결 시 시장가)
                    bid_price = self._broker.get_bidding_price(stock_code)
                    if not bid_price or bid_price <= 0:
                        logger.warning(f"[ExitMonitor] {stock_code} 매수1호가 조회 실패, 시장가 매도로 전환")
                        order_id = self._order_manager.place_sell_order(
                            stock_code=stock_code,
                            stock_name=stock_name,
                            quantity=quantity,
                            strategy_name=strategy_name,
                        )
                    else:
                        logger.info(
                            f"[ExitMonitor] {stock_code} TP 지정가 매도: {quantity}주 @ {bid_price:,}원 (매수1호가)"
                        )

                        order_id = self._order_manager.place_sell_order_with_fallback(
                            stock_code=stock_code,
                            stock_name=stock_name,
                            quantity=quantity,
                            strategy_name=strategy_name,
                            limit_price=bid_price,
                            fallback_seconds=1.0,
                        )
                else:
                    # SL/Timeout: 시장가 매도
                    logger.info(f"[ExitMonitor] {stock_code} 시장가 매도: {quantity}주")

                    order_id = self._order_manager.place_sell_order(
                        stock_code=stock_code,
                        stock_name=stock_name,
                        quantity=quantity,
                        strategy_name=strategy_name,
                    )

                if order_id:
                    # 손익 계산 및 알림
                    profit_loss = int((current_price - position.avg_price) * quantity)
                    profit_rate = (current_price - position.avg_price) / position.avg_price * 100

                    self._slack.notify_sell(
                        stock_code=stock_code,
                        stock_name=stock_name,
                        quantity=quantity,
                        price=current_price,
                        profit_loss=profit_loss,
                        profit_rate=profit_rate,
                        strategy_name=strategy_name,
                        reason=f"[실시간] {reason}",
                        strategy_win_rate=win_rate,
                    )
                else:
                    # 매도 주문 실패 시 exit_in_progress 해제 (재시도 가능하도록)
                    if self._exit_monitor:
                        self._exit_monitor.clear_exit_in_progress(stock_code)
                    logger.warning(f"[ExitMonitor] {stock_code} 매도 주문 실패 - exit_in_progress 해제")

            except Exception as e:
                logger.error(f"[ExitMonitor] Error processing exit signal: {e}")

    def _register_position_for_exit_monitor(
        self, order: ManagedOrder, filled_qty: int
    ) -> None:
        """매수 체결 시 매도 모니터링 등록"""
        if not self._exit_monitor:
            return

        if order.side != OrderSide.BUY:
            return

        if not order.strategy_name:
            return

        # realtime_exit: true 설정 확인
        stock_config = self._settings.stocks.get(order.stock_code)
        if not stock_config:
            return

        for strategy_config in stock_config.strategies:
            if strategy_config.get("name") != order.strategy_name:
                continue

            if not strategy_config.get("realtime_exit", False):
                return  # realtime_exit 비활성화

            params = strategy_config.get("params", {})
            config = ExitMonitorConfig(
                stock_code=order.stock_code,
                strategy_name=order.strategy_name,
                avg_price=order.filled_price,
                quantity=filled_qty,
                entry_time=datetime.now(),
                take_profit_pct=params.get("take_profit_pct", 0.003),
                stop_loss_pct=params.get("stop_loss_pct", 0.01),
                max_holding_minutes=params.get("max_holding_minutes", 60),
                signal_price=float(order.signal_price) if order.signal_price > 0 else order.filled_price,
            )
            self._exit_monitor.add_position(config)
            logger.info(f"[ExitMonitor] Registered: {order.stock_code}")
            break

    def _unregister_position_from_exit_monitor(self, stock_code: str) -> None:
        """매도 체결 시 매도 모니터링 해제"""
        if not self._exit_monitor:
            return

        self._exit_monitor.remove_position(stock_code)

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

                # NOTE: DB 저장은 스케줄러(_on_stock_tick)에서 30개 분봉 단위로 처리

                # 중복 주문 방지
                if self._order_manager.has_pending_order(stock_code):
                    logger.debug(f"[WS][{stock_code}] 미체결 주문 존재 - 시그널 생성 스킵")
                    return

                # WebSocket 전략만 실행
                strategies = stock_config.strategies
                if not strategies:
                    return

                # 가격 히스토리 로드 (분봉)
                price_history = self._price_repo.get_recent_prices(stock_code, count=500)

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

                    # 시그널 생성 (WebSocket 모드)
                    signal = strategy.generate_signal(context, "websocket")

                    # 시그널 처리
                    self._process_signal(signal, context, strategy)

                # 스캘핑 executor에 tick 전달 (별도 처리, 중복 주문 방지와 무관)
                for key, executor in self._scalping_executors.items():
                    if key[0] == stock_code and executor.is_active:
                        executor.on_tick(tick_data.price, now)

            except Exception as e:
                logger.error(f"WebSocket tick error [{tick_data.stock_code}]: {e}")

    # ===== 스케줄러 기반 메서드 =====

    def _on_prefetch_tick(self, stock_code: str, now: datetime) -> None:
        """
        예수금 사전 조회 콜백 (매분 n초에 실행)

        신호 발생 전에 미리 예수금을 조회하여 캐싱합니다.
        이를 통해 신호 발생 시 API 호출 없이 빠르게 수량을 계산할 수 있습니다.
        """
        try:
            # 예수금 조회 (current_price=0으로 호출하면 API의 기본값 사용)
            _, deposit = self._broker.get_buyable_quantity(stock_code, 0)

            self._prefetch_cache[stock_code] = (deposit, now)

            logger.debug(f"[prefetch][{stock_code}] 캐시 저장: 예수금 {deposit:,}원")
        except Exception as e:
            logger.error(f"[prefetch][{stock_code}] 조회 실패: {e}")

    def _get_prefetch_cache(self, stock_code: str) -> Optional[int]:
        """
        유효한 예수금 캐시 반환 (TTL 체크)

        Returns:
            예수금 (deposit) 또는 None (캐시 없거나 만료)
        """
        cached = self._prefetch_cache.get(stock_code)
        if cached:
            deposit, timestamp = cached
            elapsed = (datetime.now() - timestamp).total_seconds()
            if elapsed < self._prefetch_cache_ttl:
                return deposit
        return None

    def _on_stock_tick(self, stock_code: str, now: datetime) -> None:
        """
        종목 틱 콜백 (스케줄러 기반 전략용)

        1. 분봉 데이터 조회 (30개)
        2. DB 저장
        3. 전략별 시그널 생성
        4. 주문 실행

        Note: 체결 확인은 스케줄러에서 병렬 처리 전 1회 호출
        """
        # 청산 진행 중이면 전략 실행 skip
        if self._liquidation_in_progress:
            logger.debug(f"[{stock_code}] Skipping stock tick: liquidation in progress")
            return

        with self._tick_lock:
            try:
                # 1. 분봉 데이터 조회 (30개)
                candle_data = self._broker.get_minute_candles(stock_code=stock_code)
                if not candle_data:
                    logger.warning(f"Failed to get minute candles: {stock_code}")
                    return

                # 2. DB 저장 (30개 분봉 upsert)
                self._save_minute_candles(stock_code, candle_data)

                # 현재가 로그 출력 (가장 최근 분봉 기준)
                latest_candle = candle_data[0]  # 최신순 정렬
                current_price = latest_candle["close_price"]
                change_rate = latest_candle.get("change_rate", 0.0)

                stock_config = self._settings.stocks.get(stock_code)
                stock_name = stock_config.name if stock_config else stock_code

                # 당일 등락률
                change_sign = "+" if change_rate > 0 else ""
                change_rate_str = f"({change_sign}{change_rate:.1f}%)"

                # 보유 포지션 수익률 계산
                position_profit_str = ""
                position = self._position_manager.get_position(stock_code)
                if position and position.avg_price > 0:
                    position_profit_rate = (current_price - position.avg_price) / position.avg_price * 100
                    position_sign = "+" if position_profit_rate >= 0 else ""
                    position_profit_str = f" / 현재포지션 대비 ({position_sign}{position_profit_rate:.1f}%)"

                logger.info(
                    f"[{stock_name}] 현재가: {current_price:,}원 {change_rate_str}{position_profit_str}"
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
                price_history = self._price_repo.get_recent_prices(stock_code, count=500)

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

                    # 스캘핑 전략: executor에 라우팅 (별도 처리)
                    if strategy_config.get("execution_mode") == "scalping":
                        executor = self._scalping_executors.get(key)
                        if executor:
                            context = StrategyContext(
                                stock_code=stock_code,
                                stock_name=stock_config.name,
                                current_price=current_price,
                                current_time=now,
                                price_history=price_history,
                                position=broker_position,
                                daily_candles=daily_candles,
                                today_trade_count=self._order_manager.get_today_trade_count(
                                    stock_code
                                ),
                            )
                            if strategy.can_generate_signal(context):
                                signal = strategy.generate_signal(context)

                                # LONG 시그널: 기존 로직
                                if signal.is_buy and not executor.is_active:
                                    # main_beam_1 등 limit_order 전략: 즉시 지정가 매수
                                    if signal.metadata.get("limit_price"):
                                        executor.activate_limit_order(
                                            buy_price=signal.metadata["limit_price"],
                                            sell_price=signal.metadata["sell_price"],
                                            timeout_seconds=signal.metadata.get(
                                                "timeout_seconds", 60
                                            ),
                                            quantity=signal.quantity,
                                        )
                                    else:
                                        # 기존 boundary_tracker 기반 스캘핑
                                        executor.activate_signal(
                                            signal_price=current_price,
                                            tp_pct=executor._config.take_profit_pct,
                                            sl_pct=executor._config.stop_loss_pct,
                                            timeout_minutes=executor._config.max_signal_minutes,
                                        )
                                    # Slack notification now handled in executor methods

                                # NEW: SHORT 시그널 → 활성화 중일 때만 처리
                                elif signal.is_sell and executor.is_active:
                                    executor.handle_short_signal(
                                        short_price=current_price,
                                        reason=signal.reason
                                    )
                        continue

                    # 포지션 보유 시 해당 전략으로만 매도 가능
                    # (unmanaged 포지션은 첫 번째 매칭 전략이 처리)
                    is_other_strategy_position = (
                        position
                        and position.strategy_name is not None
                        and position.strategy_name != strategy_name
                    )

                    # 전략 컨텍스트 생성
                    context = StrategyContext(
                        stock_code=stock_code,
                        stock_name=stock_config.name,
                        current_price=current_price,
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

                    # 다른 전략의 포지션이어도 모니터링 로그는 출력
                    if is_other_strategy_position:
                        strategy.generate_signal(context)
                        continue

                    # 시그널 생성
                    signal = strategy.generate_signal(context)

                    # ExitMonitor가 모니터링 중인 종목의 매도 시그널은 스킵
                    # (WebSocket에서 실시간 처리하므로 폴링 스킵)
                    if signal.is_sell and self._exit_monitor:
                        if self._exit_monitor.is_exit_in_progress(stock_code):
                            logger.debug(
                                f"[{stock_code}] 실시간 매도 진행 중 - 폴링 스킵"
                            )
                            continue
                        if (
                            self._exit_monitor.is_monitored(stock_code)
                            and self._exit_monitor.is_ws_connected
                        ):
                            logger.debug(
                                f"[{stock_code}] ExitMonitor 모니터링 중 - 폴링 스킵"
                            )
                            continue
                        # WebSocket 끊김 시 → 폴링이 백업으로 처리

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

            # TP/SL 가격 계산 (수량 계산보다 먼저 - signal_price 필요)
            params = strategy.params if hasattr(strategy, 'params') else {}
            tp_rate = params.get("take_profit_pct", 0.003)
            sl_rate = params.get("stop_loss_pct", 0.01)
            price_offset_pct = params.get("price_offset_pct", 0.0)

            # 오프셋 적용된 시그널 가격 (TP/SL 기준가, 수량 계산에도 사용)
            signal_price = int(context.current_price * (1 + price_offset_pct))
            tp_price = int(signal_price * (1 + tp_rate))
            sl_price = int(signal_price * (1 - sl_rate))

            if price_offset_pct != 0.0:
                logger.info(
                    f"[{stock_code}] price_offset: {price_offset_pct:+.4f} "
                    f"현재가 {context.current_price:,} → 시그널가 {signal_price:,}"
                )

            # 캐시된 예수금 사용 또는 실시간 조회
            cached_deposit = self._get_prefetch_cache(stock_code)
            if cached_deposit:
                max_buy_amt = cached_deposit
                logger.info(f"[{stock_code}] prefetch 캐시 사용: 예수금 {max_buy_amt:,}원")
            else:
                _, max_buy_amt = self._broker.get_buyable_quantity(stock_code, 0)
                logger.warning(f"[{stock_code}] prefetch 캐시 없음 → 실시간 조회: 예수금 {max_buy_amt:,}원")

            # 수수료 적용하여 매수가능수량 계산
            price_with_fee = int(signal_price * (1 + self._buy_fee_rate))
            buyable_qty = max_buy_amt // price_with_fee if price_with_fee > 0 else 0

            if buyable_qty > 0:
                # allocation 비율 적용
                quantity = int(buyable_qty * (allocation / 100))
                if quantity < 1:
                    logger.warning(f"[{stock_code}] 계산된 수량 0 → 최소 1주로 설정")
                    quantity = 1
                logger.info(
                    f"[{stock_code}] 매수 수량 계산: {quantity}주 "
                    f"(예수금: {max_buy_amt:,}원, 가격: {signal_price:,}원, "
                    f"수수료율: {self._buy_fee_rate:.4%}, allocation: {allocation}%)"
                )
            else:
                quantity = signal.quantity
                max_buy_amt = context.current_price * quantity  # fallback
                logger.warning(f"[{stock_code}] 매수가능수량 조회 실패 → 시그널 수량 사용: {quantity}주")

            # 시그널 알림 (주문 전) - 매수 시그널은 매번 전송
            self._slack.notify_signal(
                signal_type="BUY",
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=quantity,
                price=context.current_price,
                strategy_name=strategy.name,
                reason=signal.reason,
                strategy_win_rate=win_rate,
                tp_price=tp_price,
                tp_rate=tp_rate,
                sl_price=sl_price,
                sl_rate=-sl_rate,  # 음수로 표시
                force=True,
            )

            # 지정가 추격 매수 (매도호가1로 주문 + 0.5초마다 정정)
            # deposit: 가격 상승 시 수량 자동 조정용 (실제 최대매수금액 사용)
            order_id = self._order_manager.place_buy_order_with_chase(
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=quantity,
                deposit=max_buy_amt,
                strategy_name=strategy.name,
                interval=0.5,
                max_retry=10,
                signal_price=signal_price,
            )

            if order_id:
                logger.info(f"[{stock_code}] 지정가 추격 매수 프로세스 종료: {order_id}")
                order = self._order_manager.get_order(order_id)
                actual_qty = order.filled_qty if order else quantity
                actual_price = order.filled_price if order and order.filled_price > 0 else context.current_price

                self._slack.notify_buy(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    quantity=actual_qty,
                    price=actual_price,
                    strategy_name=strategy.name,
                    reason=signal.reason,
                    strategy_win_rate=win_rate,
                )
            else:
                logger.error(f"[{stock_code}] 지정가 추격 매수 실패")

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

            # 익절 여부 확인 (TP 달성 시 지정가, 그 외 시장가)
            is_take_profit = "익절" in signal.reason

            if is_take_profit and position:
                # TP: 매수1호가 지정가 매도 (즉시 체결)
                bid_price = self._broker.get_bidding_price(stock_code)
                if not bid_price or bid_price <= 0:
                    logger.warning(f"[{stock_code}] 매수1호가 조회 실패, 시장가 매도로 전환")
                    order_id = self._order_manager.place_sell_order(
                        stock_code=stock_code,
                        stock_name=stock_name,
                        quantity=signal.quantity,
                        strategy_name=strategy.name,
                    )
                else:
                    logger.info(
                        f"[{stock_code}] TP 지정가 매도: {signal.quantity}주 @ {bid_price:,}원 (매수1호가)"
                    )

                    order_id = self._order_manager.place_sell_order_with_fallback(
                        stock_code=stock_code,
                        stock_name=stock_name,
                        quantity=signal.quantity,
                        strategy_name=strategy.name,
                        limit_price=bid_price,
                        fallback_seconds=1.0,
                    )
            else:
                # SL/Timeout/Overnight: 시장가 매도
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

        # 당일 누적 실현손익 - DB에서 조회
        # - 09:00 정상 시작: DB에 당일 매도 없음 → 0
        # - 장 중 재시작: DB에 당일 매도 있음 → 복구
        self._daily_realized_pnl = self._report_generator.get_today_realized_pnl()
        logger.info(f"Daily realized PnL on market open: {self._daily_realized_pnl:,}원")

        # 정규장 시작 알림
        self._slack.send_market_open_notification()

        # 포지션 동기화
        self._position_manager.sync_with_broker()

        # Overnight 포지션 처리
        self._process_overnight_positions()

        # ExitMonitor 재시작 (포지션 있으면 WebSocket 연결)
        if self._exit_monitor:
            self._exit_monitor.start()
            self._register_existing_positions_for_exit_monitor()
            logger.info("[ExitMonitor] Restarted on market open")

    def _process_overnight_positions(self) -> None:
        """
        Overnight 포지션 처리

        전략별 overnight_action 설정에 따라:
        - "timeout": 장 시작 시 시장가 매도
        - "hold": 포지션 유지, 일반 TP/SL 체크 계속
        """
        positions = self._position_manager.get_all_positions()

        if not positions:
            logger.info("Overnight 포지션 없음")
            return

        logger.info(f"Overnight 포지션 {len(positions)}개 확인")

        for stock_code, pos in positions.items():
            strategy_name = pos.strategy_name

            if not strategy_name:
                logger.warning(f"[{stock_code}] 전략 미지정 포지션 - 스킵")
                continue

            # 전략 설정에서 overnight_action 가져오기
            overnight_action = "hold"  # 기본값
            strategies = self._settings.get_stock_strategies(stock_code)

            for strat_config in strategies:
                if strat_config.get("name") == strategy_name:
                    overnight_action = strat_config.get("overnight_action", "hold")
                    break

            logger.info(
                f"[{stock_code}] Overnight 포지션: {pos.quantity}주, "
                f"전략: {strategy_name}, action: {overnight_action}"
            )

            if overnight_action == "timeout":
                # 장 시작 시 시장가 매도
                stock_config = self._settings.stocks.get(stock_code)
                stock_name = stock_config.name if stock_config else stock_code

                logger.info(f"[{stock_code}] Overnight timeout → 시장가 매도")

                order_id = self._order_manager.place_sell_order(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    quantity=pos.quantity,
                    strategy_name=strategy_name,
                )

                if order_id:
                    # 수익률 계산
                    profit_rate = 0.0
                    if pos.avg_price > 0 and pos.current_price > 0:
                        profit_rate = (pos.current_price - pos.avg_price) / pos.avg_price * 100

                    self._slack.notify_sell(
                        stock_code=stock_code,
                        stock_name=stock_name,
                        quantity=pos.quantity,
                        price=pos.current_price,
                        profit_loss=int((pos.current_price - pos.avg_price) * pos.quantity),
                        profit_rate=profit_rate,
                        strategy_name=strategy_name,
                        reason="Overnight timeout",
                        strategy_win_rate=self._settings.get_strategy_win_rate(
                            stock_code, strategy_name
                        ),
                    )
            # else "hold": 포지션 유지, 일반 TP/SL 체크 계속

    def _on_market_close(self) -> None:
        """장 마감 콜백"""
        try:
            # 1. 미체결 주문 취소
            cancelled = self._order_manager.cancel_all_pending()
            logger.info(f"Cancelled {cancelled} pending orders at market close")

            # 2. ExitMonitor WebSocket 연결 해제 (장외 리소스 절약)
            if self._exit_monitor:
                self._exit_monitor.stop()
                logger.info("[ExitMonitor] Stopped on market close")

            # 3. 일일 리포트 생성 및 전송
            report = self._report_generator.generate_and_send()
            logger.info(
                f"Daily report: {report.total_trades} trades, "
                f"PnL: {report.realized_pnl:,}원"
            )

            # 4. 시그널 요약 전송
            self._slack.send_signal_summary()

        except Exception as e:
            logger.error(f"Market close error: {e}")
            self._slack.notify_error("장 마감 처리 오류", str(e))

    def _on_daily_liquidation(self) -> None:
        """15:19 당일 청산 콜백"""
        try:
            logger.info("=" * 60)
            logger.info("Starting daily liquidation at 15:19")
            logger.info("=" * 60)

            # 청산 진행 플래그 설정 (전략 실행 skip)
            self._liquidation_in_progress = True

            # ExitMonitor 정지 (중복 매도 방지)
            if self._exit_monitor:
                self._exit_monitor.stop()
                logger.info("[ExitMonitor] Stopped for liquidation")

            # 스캘핑 executor 정지 (중복 매도 방지)
            for _key, executor in self._scalping_executors.items():
                if executor.is_active:
                    executor.deactivate()
            if self._scalping_executors:
                logger.info("[ScalpingExecutor] Deactivated for liquidation")

            # Slack 시작 알림
            self._slack.send_message("⏰ [청산시작] 15:19 당일 청산 시작")

            # 청산 실행
            result = self._liquidation_manager.execute_liquidation()

            # 결과 로깅
            logger.info(f"Liquidation completed: {result.successful_orders}/{result.total_positions} positions closed")
            logger.info(f"Total liquidation value: {result.total_liquidation_value:,}원")
            logger.info(f"Total PnL: {result.total_pnl:+,}원")

            # Slack 완료 알림
            self._send_liquidation_summary(result)

        except Exception as e:
            logger.error(f"Daily liquidation failed: {e}", exc_info=True)
            self._slack.notify_error("15:19 청산 실패", str(e))
        finally:
            # 청산 진행 플래그 해제
            self._liquidation_in_progress = False

    def _send_liquidation_summary(self, result: LiquidationResult) -> None:
        """청산 결과 Slack 알림"""
        # 포지션이 없었던 경우 별도 처리
        if result.total_positions == 0:
            self._slack.send_message("ℹ️ [청산완료] 청산할 포지션이 없습니다.")
            return

        duration = (result.completed_at - result.started_at).total_seconds()

        if result.failed_orders == 0 and len(result.partial_fills) == 0:
            # 완전 성공
            message = (
                f"✅ [청산완료] {result.successful_orders}/{result.total_positions} 성공\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"💰 총 청산금액: {result.total_liquidation_value:,}원\n"
                f"📈 실현손익: *{result.total_pnl:+,}원* "
                f"({result.total_pnl / result.total_liquidation_value * 100:+.2f}%)\n"
                f"⏱️ 소요시간: {duration:.1f}초"
            )
        else:
            # 부분 실패 또는 부분 체결
            error_details = "\n".join([f"  - {code}: {msg}" for code, msg in result.errors])
            partial_details = "\n".join(
                [f"  - {code}: {qty}주 미체결" for code, qty in result.partial_fills.items()]
            )

            message = (
                f"⚠️ [청산실패] {result.successful_orders}/{result.total_positions} 성공, "
                f"{result.failed_orders} 실패\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
            )

            if error_details:
                message += f"❌ 실패:\n{error_details}\n"

            if partial_details:
                message += f"⚠️ 부분 체결:\n{partial_details}\n"

            message += f"\n⚠️ 수동 확인 필요!"

        self._slack.send_message(message)

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
