"""
구조화 로거

JSON 포맷의 구조화된 로깅
- 모듈별 시작/수행/종료 추적
- 상관관계 ID로 요청 추적
- 성능 측정
"""

import json
import logging
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from leverage_worker.utils.log_constants import LogEventType, LogCategory, get_category
from leverage_worker.utils.logger import SensitiveDataFilter


@dataclass
class StructuredLogEntry:
    """구조화된 로그 엔트리"""
    timestamp: str
    event_type: str
    category: str
    module: str
    message: str
    level: str = "INFO"
    correlation_id: Optional[str] = None
    session_id: Optional[str] = None
    duration_ms: Optional[float] = None
    stock_code: Optional[str] = None
    order_id: Optional[str] = None
    strategy_name: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        """JSON 문자열로 변환"""
        data = {k: v for k, v in asdict(self).items() if v is not None}
        return json.dumps(data, ensure_ascii=False, default=str)


class StructuredLogger:
    """
    구조화 로거

    - JSON 포맷 로깅
    - 이벤트 타입별 분류
    - 상관관계 ID 추적
    - 성능 측정 지원
    """

    _instance: Optional["StructuredLogger"] = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """싱글톤 패턴"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, log_dir: Optional[Path] = None, session_id: Optional[str] = None):
        if hasattr(self, "_initialized") and self._initialized:
            return

        if log_dir is None:
            log_dir = Path(__file__).parent.parent.parent / "logs" / "structured"

        log_dir.mkdir(parents=True, exist_ok=True)

        self._log_dir = log_dir
        self._session_id = session_id or str(uuid.uuid4())[:8]
        self._correlation_id: Optional[str] = None
        self._local = threading.local()

        # 로거 설정
        self._logger = logging.getLogger("structured")
        self._logger.setLevel(logging.DEBUG)
        self._logger.addFilter(SensitiveDataFilter())

        # 중복 방지
        if not self._logger.handlers:
            log_filename = f"structured_{datetime.now().strftime('%Y%m%d')}.log"
            log_path = log_dir / log_filename

            handler = logging.FileHandler(log_path, encoding="utf-8")
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._logger.addHandler(handler)

        self._initialized = True

    def set_correlation_id(self, correlation_id: str) -> None:
        """현재 스레드의 상관관계 ID 설정"""
        self._local.correlation_id = correlation_id

    def get_correlation_id(self) -> Optional[str]:
        """현재 스레드의 상관관계 ID 조회"""
        return getattr(self._local, "correlation_id", None)

    def clear_correlation_id(self) -> None:
        """현재 스레드의 상관관계 ID 초기화"""
        self._local.correlation_id = None

    @contextmanager
    def correlation_context(self, correlation_id: Optional[str] = None):
        """
        상관관계 ID 컨텍스트 매니저

        with logger.correlation_context():
            logger.log(...)  # 자동으로 correlation_id 포함
        """
        prev_id = self.get_correlation_id()
        new_id = correlation_id or str(uuid.uuid4())[:8]
        self.set_correlation_id(new_id)
        try:
            yield new_id
        finally:
            if prev_id:
                self.set_correlation_id(prev_id)
            else:
                self.clear_correlation_id()

    def log(
        self,
        event_type: LogEventType,
        module: str,
        message: str,
        level: str = "INFO",
        stock_code: Optional[str] = None,
        order_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
        duration_ms: Optional[float] = None,
        **metadata,
    ) -> None:
        """
        구조화된 로그 기록

        Args:
            event_type: 이벤트 타입
            module: 모듈명
            message: 로그 메시지
            level: 로그 레벨 (DEBUG, INFO, WARNING, ERROR)
            stock_code: 종목코드
            order_id: 주문 ID
            strategy_name: 전략명
            duration_ms: 소요 시간 (ms)
            **metadata: 추가 메타데이터
        """
        entry = StructuredLogEntry(
            timestamp=datetime.now().isoformat(),
            event_type=event_type.value,
            category=get_category(event_type).value,
            module=module,
            message=message,
            level=level,
            correlation_id=self.get_correlation_id(),
            session_id=self._session_id,
            duration_ms=duration_ms,
            stock_code=stock_code,
            order_id=order_id,
            strategy_name=strategy_name,
            metadata=metadata if metadata else {},
        )

        self._logger.info(entry.to_json())

    def module_init(self, module: str, **metadata) -> None:
        """모듈 초기화 로그"""
        self.log(LogEventType.MODULE_INIT, module, f"{module} initialized", **metadata)

    def module_start(self, module: str, **metadata) -> None:
        """모듈 시작 로그"""
        self.log(LogEventType.MODULE_START, module, f"{module} started", **metadata)

    def module_stop(self, module: str, **metadata) -> None:
        """모듈 종료 로그"""
        self.log(LogEventType.MODULE_STOP, module, f"{module} stopped", **metadata)

    def module_error(self, module: str, error: str, **metadata) -> None:
        """모듈 에러 로그"""
        self.log(
            LogEventType.MODULE_ERROR,
            module,
            f"{module} error: {error}",
            level="ERROR",
            **metadata,
        )

    @contextmanager
    def measure_time(self, event_type: LogEventType, module: str, message: str, **metadata):
        """
        성능 측정 컨텍스트 매니저

        with logger.measure_time(LogEventType.API_REQUEST, "Broker", "Get price"):
            broker.get_price(...)
        """
        start = time.perf_counter()
        try:
            yield
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            self.log(event_type, module, message, duration_ms=duration_ms, **metadata)


# 싱글톤 인스턴스 가져오기
_structured_logger_instance: Optional[StructuredLogger] = None


def get_structured_logger(log_dir: Optional[Path] = None) -> StructuredLogger:
    """구조화 로거 싱글톤 인스턴스 가져오기"""
    global _structured_logger_instance
    if _structured_logger_instance is None:
        _structured_logger_instance = StructuredLogger(log_dir)
    return _structured_logger_instance
