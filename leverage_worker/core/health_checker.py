"""
헬스체크 시스템

시스템 구성요소의 상태를 주기적으로 확인
- API 연결 상태
- DB 연결 상태
- 토큰 유효성
- 스케줄러 상태
"""

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from leverage_worker.utils.logger import get_logger
from leverage_worker.utils.structured_logger import get_structured_logger
from leverage_worker.utils.log_constants import LogEventType

logger = get_logger(__name__)
structured_logger = get_structured_logger()


class HealthStatus(Enum):
    """헬스 상태"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"  # 부분적 문제
    UNHEALTHY = "unhealthy"


@dataclass
class ComponentHealth:
    """개별 컴포넌트 헬스 정보"""
    name: str
    status: HealthStatus
    message: str = ""
    last_check: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_healthy(self) -> bool:
        return self.status == HealthStatus.HEALTHY


@dataclass
class SystemHealth:
    """전체 시스템 헬스 정보"""
    status: HealthStatus
    components: Dict[str, ComponentHealth]
    last_check: datetime = field(default_factory=datetime.now)

    def is_healthy(self) -> bool:
        return self.status == HealthStatus.HEALTHY

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "last_check": self.last_check.isoformat(),
            "components": {
                name: {
                    "status": comp.status.value,
                    "message": comp.message,
                    "last_check": comp.last_check.isoformat(),
                    "metadata": comp.metadata,
                }
                for name, comp in self.components.items()
            },
        }


class HealthChecker:
    """
    헬스체크 시스템

    - 주기적으로 시스템 구성요소 상태 확인
    - 이상 감지 시 콜백 호출 (Slack 알림 등)
    - 최근 헬스 상태 캐싱
    """

    def __init__(
        self,
        check_interval_seconds: int = 60,
        on_unhealthy_callback: Optional[Callable[[SystemHealth], None]] = None,
    ):
        self._check_interval = check_interval_seconds
        self._on_unhealthy = on_unhealthy_callback

        # 헬스 체크 함수 등록: name -> check_func
        self._checks: Dict[str, Callable[[], ComponentHealth]] = {}

        # 최근 헬스 상태
        self._last_health: Optional[SystemHealth] = None

        # 백그라운드 스레드
        self._thread: Optional[threading.Thread] = None
        self._running = False

        # 연속 실패 카운터
        self._failure_counts: Dict[str, int] = {}

        logger.info("HealthChecker initialized")
        structured_logger.module_init("HealthChecker", interval=check_interval_seconds)

    def register_check(self, name: str, check_func: Callable[[], ComponentHealth]) -> None:
        """
        헬스 체크 함수 등록

        Args:
            name: 컴포넌트 이름
            check_func: 헬스 체크 함수 (ComponentHealth 반환)
        """
        self._checks[name] = check_func
        self._failure_counts[name] = 0
        logger.debug(f"Health check registered: {name}")

    def unregister_check(self, name: str) -> None:
        """헬스 체크 함수 해제"""
        self._checks.pop(name, None)
        self._failure_counts.pop(name, None)

    def check_now(self) -> SystemHealth:
        """
        즉시 헬스 체크 실행

        Returns:
            SystemHealth 객체
        """
        components: Dict[str, ComponentHealth] = {}
        overall_status = HealthStatus.HEALTHY

        for name, check_func in self._checks.items():
            try:
                health = check_func()
                components[name] = health

                if health.status == HealthStatus.UNHEALTHY:
                    self._failure_counts[name] = self._failure_counts.get(name, 0) + 1
                    overall_status = HealthStatus.UNHEALTHY
                elif health.status == HealthStatus.DEGRADED:
                    if overall_status == HealthStatus.HEALTHY:
                        overall_status = HealthStatus.DEGRADED
                else:
                    self._failure_counts[name] = 0

            except Exception as e:
                logger.error(f"Health check failed for {name}: {e}")
                components[name] = ComponentHealth(
                    name=name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Check failed: {str(e)}",
                )
                self._failure_counts[name] = self._failure_counts.get(name, 0) + 1
                overall_status = HealthStatus.UNHEALTHY

        system_health = SystemHealth(
            status=overall_status,
            components=components,
            last_check=datetime.now(),
        )

        self._last_health = system_health

        # 구조화 로그
        structured_logger.log(
            LogEventType.HEALTH_CHECK,
            "HealthChecker",
            f"Health check completed: {overall_status.value}",
            level="WARNING" if overall_status != HealthStatus.HEALTHY else "INFO",
            health_status=overall_status.value,
            unhealthy_components=[
                name for name, comp in components.items()
                if comp.status == HealthStatus.UNHEALTHY
            ],
        )

        # 비정상 상태 시 콜백 호출
        if overall_status != HealthStatus.HEALTHY and self._on_unhealthy:
            try:
                self._on_unhealthy(system_health)
            except Exception as e:
                logger.error(f"Unhealthy callback failed: {e}")

        return system_health

    def start_background_check(self) -> None:
        """백그라운드 헬스 체크 시작"""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._check_loop, daemon=True)
        self._thread.start()

        logger.info("Background health check started")
        structured_logger.module_start("HealthChecker")

    def stop_background_check(self) -> None:
        """백그라운드 헬스 체크 중지"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Background health check stopped")
        structured_logger.module_stop("HealthChecker")

    def _check_loop(self) -> None:
        """헬스 체크 루프"""
        while self._running:
            try:
                self.check_now()
            except Exception as e:
                logger.error(f"Health check loop error: {e}")

            time.sleep(self._check_interval)

    @property
    def last_health(self) -> Optional[SystemHealth]:
        """최근 헬스 상태"""
        return self._last_health

    def get_failure_count(self, name: str) -> int:
        """연속 실패 횟수 조회"""
        return self._failure_counts.get(name, 0)


# 헬스 체크 헬퍼 함수들
def create_api_health_check(session_manager) -> Callable[[], ComponentHealth]:
    """API 연결 헬스 체크 함수 생성"""
    def check() -> ComponentHealth:
        try:
            if not session_manager.is_authenticated:
                return ComponentHealth(
                    name="api",
                    status=HealthStatus.UNHEALTHY,
                    message="Not authenticated",
                )

            if not session_manager.is_token_valid:
                return ComponentHealth(
                    name="api",
                    status=HealthStatus.UNHEALTHY,
                    message="Token invalid",
                )

            return ComponentHealth(
                name="api",
                status=HealthStatus.HEALTHY,
                message="API connection healthy",
            )
        except Exception as e:
            return ComponentHealth(
                name="api",
                status=HealthStatus.UNHEALTHY,
                message=str(e),
            )

    return check


def create_db_health_check(database) -> Callable[[], ComponentHealth]:
    """DB 연결 헬스 체크 함수 생성"""
    def check() -> ComponentHealth:
        try:
            # 간단한 쿼리로 연결 확인
            result = database.fetch_one("SELECT 1 as test")
            if result:
                return ComponentHealth(
                    name="database",
                    status=HealthStatus.HEALTHY,
                    message="Database connection healthy",
                )
            return ComponentHealth(
                name="database",
                status=HealthStatus.UNHEALTHY,
                message="Database query failed",
            )
        except Exception as e:
            return ComponentHealth(
                name="database",
                status=HealthStatus.UNHEALTHY,
                message=str(e),
            )

    return check


def create_scheduler_health_check(scheduler) -> Callable[[], ComponentHealth]:
    """스케줄러 헬스 체크 함수 생성"""
    def check() -> ComponentHealth:
        try:
            if not scheduler.is_running:
                return ComponentHealth(
                    name="scheduler",
                    status=HealthStatus.DEGRADED,
                    message="Scheduler not running (may be outside market hours)",
                )

            return ComponentHealth(
                name="scheduler",
                status=HealthStatus.HEALTHY,
                message="Scheduler running",
            )
        except Exception as e:
            return ComponentHealth(
                name="scheduler",
                status=HealthStatus.UNHEALTHY,
                message=str(e),
            )

    return check
