"""
설정 관리 모듈

TradingMode: 모의/실전 투자 모드
Settings: 전역 설정 관리 클래스
ConfigValidationResult: 설정 검증 결과
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, Any, List, Optional

import yaml

logger = logging.getLogger(__name__)


class TradingMode(Enum):
    """매매 모드"""
    PAPER = "paper"  # 모의투자
    LIVE = "live"    # 실전투자


@dataclass
class ScheduleConfig:
    """스케줄 설정"""
    trading_start: str = "08:50"
    trading_end: str = "15:30"
    default_interval_seconds: int = 5
    default_offset_seconds: int = 0
    idle_check_interval_seconds: int = 60  # 장외 시간 체크 주기


@dataclass
class SessionConfig:
    """세션 설정"""
    token_refresh_hours_before: int = 8  # 만료 8시간 전 갱신
    token_validity_hours: int = 24


@dataclass
class StockConfig:
    """개별 종목 설정"""
    code: str
    name: str
    interval_seconds: Optional[int] = None  # None이면 전역 기본값 사용
    offset_seconds: Optional[int] = None
    strategies: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class NotificationConfig:
    """알림 설정"""
    slack_webhook_url: Optional[str] = None
    slack_token: Optional[str] = None  # Bot Token (xoxb-...)
    slack_channel: Optional[str] = None  # Channel ID (C...)
    enable_trade_alerts: bool = True
    enable_daily_report: bool = True


@dataclass
class ConfigValidationResult:
    """설정 검증 결과"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_error(self, message: str) -> None:
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str) -> None:
        self.warnings.append(message)


class Settings:
    """전역 설정 관리 클래스"""

    def __init__(
        self,
        mode: TradingMode = TradingMode.PAPER,
        config_path: Optional[Path] = None,
    ):
        self.mode = mode
        self.schedule = ScheduleConfig()
        self.session = SessionConfig()
        self.notification = NotificationConfig()
        self._credentials: Dict[str, Any] = {}
        self._stocks: Dict[str, StockConfig] = {}
        self._execution: Dict[str, Any] = {}
        self._config_path: Optional[Path] = None

        # 설정 자동 로드
        if config_path is None:
            # 기본 경로: leverage_worker/config/
            config_path = Path(__file__).parent

        self.load(config_path)

    def load(self, config_path: Path) -> None:
        """전체 설정 로드"""
        self._config_path = config_path
        self._load_credentials(config_path)
        self._load_trading_config(config_path)

    def _load_credentials(self, config_path: Path) -> None:
        """API 자격증명 로드 (~/KIS/config/kis_devlp.yaml 단일 파일)"""
        devlp_path = Path.home() / "KIS" / "config" / "kis_devlp.yaml"

        if not devlp_path.exists():
            raise FileNotFoundError(f"Credential file not found: {devlp_path}")

        with open(devlp_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

        if self.mode == TradingMode.PAPER:
            self._credentials = {
                "app_key": cfg.get("paper_app", ""),
                "app_secret": cfg.get("paper_sec", ""),
                "account_number": cfg.get("my_paper_stock", ""),
                "account_product_code": cfg.get("my_prod", "01"),
                "hts_id": cfg.get("my_htsid", ""),
            }
        else:
            self._credentials = {
                "app_key": cfg.get("my_app", ""),
                "app_secret": cfg.get("my_sec", ""),
                "account_number": cfg.get("my_acct_stock", ""),
                "account_product_code": cfg.get("my_prod", "01"),
                "hts_id": cfg.get("my_htsid", ""),
            }

    def _load_trading_config(self, config_path: Path) -> None:
        """종목별 전략 설정 로드"""
        trading_config_path = config_path / "trading_config.yaml"

        if not trading_config_path.exists():
            raise FileNotFoundError(f"Trading config not found: {trading_config_path}")

        with open(trading_config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        # 스케줄 설정
        schedule_cfg = config.get("schedule", {})
        self.schedule = ScheduleConfig(
            trading_start=schedule_cfg.get("trading_start", "08:50"),
            trading_end=schedule_cfg.get("trading_end", "15:30"),
            default_interval_seconds=schedule_cfg.get("default_interval_seconds", 5),
            default_offset_seconds=schedule_cfg.get("default_offset_seconds", 0),
            idle_check_interval_seconds=schedule_cfg.get("idle_check_interval_seconds", 60),
        )

        # 세션 설정
        session_cfg = config.get("session", {})
        self.session = SessionConfig(
            token_refresh_hours_before=session_cfg.get("token_refresh_hours_before", 8),
            token_validity_hours=session_cfg.get("token_validity_hours", 24),
        )

        # 알림 설정
        notification_cfg = config.get("notification", {})
        self.notification = NotificationConfig(
            slack_webhook_url=notification_cfg.get("slack_webhook_url"),
            slack_token=notification_cfg.get("slack_token"),
            slack_channel=notification_cfg.get("slack_channel"),
            enable_trade_alerts=notification_cfg.get("enable_trade_alerts", True),
            enable_daily_report=notification_cfg.get("enable_daily_report", True),
        )

        # 실행 설정
        self._execution = config.get("execution", {})

        # 종목 설정
        stocks_cfg = config.get("stocks", {})
        for code, stock_data in stocks_cfg.items():
            self._stocks[code] = StockConfig(
                code=code,
                name=stock_data.get("name", code),
                interval_seconds=stock_data.get("interval_seconds"),
                offset_seconds=stock_data.get("offset_seconds"),
                strategies=stock_data.get("strategies", []),
            )

    @property
    def app_key(self) -> str:
        return self._credentials.get("app_key", "")

    @property
    def app_secret(self) -> str:
        return self._credentials.get("app_secret", "")

    @property
    def account_number(self) -> str:
        return self._credentials.get("account_number", "")

    @property
    def account_product_code(self) -> str:
        return self._credentials.get("account_product_code", "01")

    @property
    def hts_id(self) -> str:
        return self._credentials.get("hts_id", "")

    @property
    def market_data_db_path(self) -> Path:
        """시세 DB 경로 (모의/실전 공유)"""
        return Path(__file__).parent.parent / "data" / "market_data.db"

    @property
    def trading_db_path(self) -> Path:
        """매매 DB 경로 (모의/실전 분리)"""
        if self.mode == TradingMode.PAPER:
            return Path(__file__).parent.parent / "data" / "trading_paper.db"
        return Path(__file__).parent.parent / "data" / "trading_live.db"

    @property
    def stocks(self) -> Dict[str, StockConfig]:
        """종목 설정 딕셔너리"""
        return self._stocks

    def get_stock_interval(self, stock_code: str) -> int:
        """종목별 interval 반환 (override 또는 기본값)"""
        stock = self._stocks.get(stock_code)
        if stock and stock.interval_seconds is not None:
            return stock.interval_seconds
        return self.schedule.default_interval_seconds

    def get_stock_offset(self, stock_code: str) -> int:
        """종목별 offset 반환 (override 또는 기본값)"""
        stock = self._stocks.get(stock_code)
        if stock and stock.offset_seconds is not None:
            return stock.offset_seconds
        return self.schedule.default_offset_seconds

    def get_stock_strategies(self, stock_code: str) -> List[Dict[str, Any]]:
        """종목의 전략 목록 반환"""
        stock = self._stocks.get(stock_code)
        if stock:
            return stock.strategies
        return []

    def get_strategy_win_rate(
        self, stock_code: str, strategy_name: str
    ) -> Optional[float]:
        """전략의 백테스트 승률 반환"""
        stock = self._stocks.get(stock_code)
        if stock:
            for strategy in stock.strategies:
                if strategy.get("name") == strategy_name:
                    return strategy.get("win_rate")
        return None

    def get_strategy_allocation(
        self, stock_code: str, strategy_name: str
    ) -> float:
        """전략의 자금 할당 비율 반환 (기본값: 100%)"""
        stock = self._stocks.get(stock_code)
        if stock:
            for strategy in stock.strategies:
                if strategy.get("name") == strategy_name:
                    return float(strategy.get("allocation", 100))
        return 100.0

    def get_prefetch_second(self) -> int:
        """예수금 사전 조회 시점 (초) 반환"""
        return self._execution.get("prefetch_second", 55)

    def get_prefetch_cache_ttl(self) -> int:
        """예수금 캐시 유효 시간 (초) 반환"""
        return self._execution.get("prefetch_cache_ttl", 10)

    def get_buy_fee_rate(self) -> float:
        """매수 수수료율 반환 (기본값: 0.015%)"""
        return self._execution.get("buy_fee_rate", 0.00015)

    def get_server_url(self) -> str:
        """API 서버 URL 반환"""
        if self.mode == TradingMode.LIVE:
            return "https://openapi.koreainvestment.com:9443"
        return "https://openapivts.koreainvestment.com:29443"

    def get_websocket_url(self) -> str:
        """WebSocket URL 반환"""
        if self.mode == TradingMode.LIVE:
            return "ws://ops.koreainvestment.com:21000"
        return "ws://ops.koreainvestment.com:31000"

    def is_paper_trading(self) -> bool:
        """모의투자 여부"""
        return self.mode == TradingMode.PAPER

    def get_env_division(self) -> str:
        """API 호출용 환경 구분 (real/demo)"""
        return "demo" if self.mode == TradingMode.PAPER else "real"

    def validate(self, strategy_registry=None) -> ConfigValidationResult:
        """
        설정 유효성 검증

        Args:
            strategy_registry: StrategyRegistry 인스턴스 (전략 이름 검증용)

        Returns:
            ConfigValidationResult 객체
        """
        result = ConfigValidationResult(is_valid=True)

        # 1. 자격증명 검증
        if not self.app_key:
            result.add_error("app_key is missing")
        elif len(self.app_key) < 8:
            result.add_warning("app_key seems too short")

        if not self.app_secret:
            result.add_error("app_secret is missing")
        elif len(self.app_secret) < 8:
            result.add_warning("app_secret seems too short")

        if not self.account_number:
            result.add_error("account_number is missing")
        elif not self.account_number.isdigit() or len(self.account_number) != 8:
            result.add_error(
                f"account_number '{self.account_number}' is invalid "
                f"(expected 8-digit number, got {len(self.account_number)} chars)"
            )

        if not self.account_product_code.isdigit() or len(self.account_product_code) != 2:
            result.add_error(
                f"account_product_code '{self.account_product_code}' is invalid "
                f"(expected 2-digit number like '01')"
            )

        # 2. 종목 설정 검증
        if not self._stocks:
            result.add_warning("No stocks configured")
        else:
            for code, stock in self._stocks.items():
                # 종목코드 형식 검증 (6자리 숫자)
                if not code.isdigit() or len(code) != 6:
                    result.add_warning(f"Stock code '{code}' may be invalid (expected 6 digits)")

                # 전략 설정 검증
                if not stock.strategies:
                    result.add_warning(f"Stock '{code}' ({stock.name}) has no strategies")
                else:
                    for i, strategy_cfg in enumerate(stock.strategies):
                        strategy_name = strategy_cfg.get("name")
                        if not strategy_name:
                            result.add_error(
                                f"Stock '{code}' strategy[{i}]: 'name' is missing"
                            )
                        elif strategy_registry:
                            # 전략 레지스트리에서 전략 존재 확인
                            if not strategy_registry.has_strategy(strategy_name):
                                result.add_error(
                                    f"Stock '{code}': strategy '{strategy_name}' not found in registry"
                                )

                        # allocation 검증
                        allocation = strategy_cfg.get("allocation", 0)
                        if allocation <= 0:
                            result.add_warning(
                                f"Stock '{code}' strategy '{strategy_name}': "
                                f"allocation is 0 or negative"
                            )
                        elif allocation > 100:
                            result.add_warning(
                                f"Stock '{code}' strategy '{strategy_name}': "
                                f"allocation > 100% ({allocation}%)"
                            )

        # 3. 스케줄 검증
        try:
            start_parts = self.schedule.trading_start.split(":")
            end_parts = self.schedule.trading_end.split(":")
            start_hour = int(start_parts[0])
            end_hour = int(end_parts[0])

            if start_hour < 8 or start_hour > 10:
                result.add_warning(f"Unusual trading_start: {self.schedule.trading_start}")
            if end_hour < 15 or end_hour > 16:
                result.add_warning(f"Unusual trading_end: {self.schedule.trading_end}")
        except (ValueError, IndexError):
            result.add_error("Invalid trading_start or trading_end format")

        # 4. 알림 설정 검증
        if self.notification.enable_trade_alerts:
            if not self.notification.slack_webhook_url and not self.notification.slack_token:
                result.add_warning("Trade alerts enabled but no Slack credentials configured")

        # 로깅
        if not result.is_valid:
            for error in result.errors:
                logger.error(f"Config validation error: {error}")
        for warning in result.warnings:
            logger.warning(f"Config validation warning: {warning}")

        return result

    def validate_or_raise(self, strategy_registry=None) -> None:
        """
        설정 검증 후 에러가 있으면 예외 발생

        Args:
            strategy_registry: StrategyRegistry 인스턴스

        Raises:
            ValueError: 설정에 에러가 있을 경우
        """
        result = self.validate(strategy_registry)
        if not result.is_valid:
            raise ValueError(f"Configuration validation failed: {', '.join(result.errors)}")
