"""
설정 관리 모듈

TradingMode: 모의/실전 투자 모드
Settings: 전역 설정 관리 클래스
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, Any, List, Optional

import yaml


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
        """API 자격증명 로드"""
        credential_file = "kis_prod.yaml" if self.mode == TradingMode.LIVE else "kis_paper.yaml"
        credential_path = config_path / "credentials" / credential_file

        if not credential_path.exists():
            raise FileNotFoundError(f"Credential file not found: {credential_path}")

        with open(credential_path, encoding="utf-8") as f:
            self._credentials = yaml.safe_load(f) or {}

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
