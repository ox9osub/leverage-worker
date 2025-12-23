"""
트레이딩 엔진 모듈

전체 시스템 통합 및 메인 로직
- 컴포넌트 초기화
- 매매 루프
- 시그널 처리
"""

import signal
import sys
from datetime import datetime
from typing import Dict, List, Optional

from leverage_worker.config.settings import Settings, TradingMode
from leverage_worker.core.scheduler import TradingScheduler
from leverage_worker.core.session_manager import SessionManager
from leverage_worker.data.database import Database
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
from leverage_worker.trading.order_manager import OrderManager
from leverage_worker.trading.position_manager import PositionManager
from leverage_worker.utils.logger import get_logger
from leverage_worker.utils.time_utils import get_current_minute_key

logger = get_logger(__name__)


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

        # 1. Database
        self._db = Database()

        # 2. Minute Candle Repository (분봉 데이터)
        self._price_repo = MinuteCandleRepository(self._db)

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
        )

        # 9. Daily Report Generator
        self._report_generator = DailyReportGenerator(self._db, self._slack)

        # 10. 전략 인스턴스 캐시: (stock_code, strategy_name) -> BaseStrategy
        self._strategies: Dict[tuple, BaseStrategy] = {}

        # 시그널 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("TradingEngine initialized")

    def _signal_handler(self, signum, frame) -> None:
        """시그널 핸들러 (Ctrl+C 등)"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()

    def start(self) -> None:
        """엔진 시작"""
        try:
            # 1. 인증
            logger.info("Authenticating...")
            if not self._session.authenticate():
                raise RuntimeError("Authentication failed")

            # 2. 토큰 자동 갱신 시작
            self._session.start_auto_refresh()

            # 3. 브로커 초기화
            self._broker = KISBroker(self._session)

            # 4. 포지션 매니저 초기화
            self._position_manager = PositionManager(self._broker, self._db)
            self._position_manager.load_from_db()
            self._position_manager.sync_with_broker()

            # 5. 주문 매니저 초기화
            self._order_manager = OrderManager(
                self._broker,
                self._position_manager,
                self._db,
            )

            # 6. 전략 로드
            self._load_strategies()

            # 7. 스케줄러 콜백 설정
            self._scheduler.set_on_stock_tick(self._on_stock_tick)
            self._scheduler.set_on_market_open(self._on_market_open)
            self._scheduler.set_on_market_close(self._on_market_close)
            self._scheduler.set_on_idle(self._on_idle)

            # 8. Slack 시작 알림
            self._slack.notify_start(
                mode=self._settings.mode.value,
                stocks_count=len(self._settings.stocks),
            )

            # 9. 스케줄러 시작
            self._running = True
            self._scheduler.start()

            logger.info("TradingEngine started")

            # 메인 스레드 대기
            while self._running:
                import time
                time.sleep(1)

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
            # 1. 스케줄러 중지
            self._scheduler.stop()

            # 2. 미체결 주문 취소
            if self._order_manager:
                cancelled = self._order_manager.cancel_all_pending()
                logger.info(f"Cancelled {cancelled} pending orders")

            # 3. 토큰 갱신 중지
            self._session.stop_auto_refresh()

            # 4. DB 연결 종료
            self._db.close_all()

            # 5. Slack 종료 알림
            self._slack.notify_stop()

            logger.info("TradingEngine stopped")

        except Exception as e:
            logger.error(f"Engine stop error: {e}")

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

    def _on_stock_tick(self, stock_code: str, now: datetime) -> None:
        """
        종목 틱 콜백

        1. 현재가 조회
        2. DB 저장
        3. 이전 주문 체결 확인
        4. 전략별 시그널 생성
        5. 주문 실행
        """
        try:
            # 1. 현재가 조회
            price_info = self._broker.get_current_price(stock_code)
            if not price_info:
                logger.warning(f"Failed to get price: {stock_code}")
                return

            # 2. DB 저장 (분봉 upsert)
            minute_key = get_current_minute_key(now)
            self._price_repo.upsert_from_api_response(
                stock_code=stock_code,
                current_price=price_info.current_price,
                volume=price_info.volume,
                minute_key=minute_key,
            )

            # 3. 체결 확인
            self._order_manager.check_fills()

            # 4. 중복 주문 방지
            if self._order_manager.has_pending_order(stock_code):
                return

            # 5. 전략별 시그널 생성
            stock_config = self._settings.stocks.get(stock_code)
            if not stock_config:
                return

            strategies = stock_config.strategies

            if not strategies:
                # 전략 없음 → 가격만 저장
                return

            # 가격 히스토리 로드
            price_history = self._price_repo.get_recent_prices(stock_code, count=60)

            # 현재 포지션
            position = self._position_manager.get_position(stock_code)
            broker_position = self._get_broker_position(stock_code)

            for strategy_config in strategies:
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
                    today_trade_count=self._order_manager.get_today_trade_count(stock_code),
                )

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
            strategy.on_entry(context, signal)

            order_id = self._order_manager.place_buy_order(
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=signal.quantity,
                strategy_name=strategy.name,
            )

            if order_id:
                self._slack.notify_buy(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    quantity=signal.quantity,
                    price=context.current_price,
                    strategy_name=strategy.name,
                    reason=signal.reason,
                )

        elif signal.is_sell:
            # 매도 시그널
            strategy.on_exit(context, signal)

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
                )

    def _on_market_open(self) -> None:
        """장 시작 콜백"""
        logger.info("Market opened - syncing positions")

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
        }
