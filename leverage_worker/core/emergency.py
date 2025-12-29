"""
긴급 중지 기능

외부 파일 감지를 통한 긴급 중지
- EMERGENCY_STOP 파일 감지
- 미체결 주문 취소
- Slack 알림
- 엔진 중지
"""

import threading
import time
from pathlib import Path
from typing import Callable, Optional

from leverage_worker.utils.logger import get_logger
from leverage_worker.utils.structured_logger import get_structured_logger
from leverage_worker.utils.log_constants import LogEventType

logger = get_logger(__name__)
structured_logger = get_structured_logger()


class EmergencyStop:
    """
    긴급 중지 기능

    - EMERGENCY_STOP 파일 감지
    - 감지 시 콜백 호출
    - 5초마다 체크
    """

    DEFAULT_STOP_FILE = Path.home() / ".leverage_worker" / "EMERGENCY_STOP"

    def __init__(
        self,
        stop_file_path: Optional[Path] = None,
        check_interval_seconds: int = 5,
        on_emergency_stop: Optional[Callable[[str], None]] = None,
    ):
        """
        Args:
            stop_file_path: 긴급 중지 파일 경로
            check_interval_seconds: 체크 주기 (초)
            on_emergency_stop: 긴급 중지 시 콜백 (reason을 인자로 받음)
        """
        self._stop_file = stop_file_path or self.DEFAULT_STOP_FILE
        self._check_interval = check_interval_seconds
        self._on_emergency_stop = on_emergency_stop

        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._triggered = False

        # 디렉토리 생성
        self._stop_file.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"EmergencyStop initialized: watching {self._stop_file}")
        structured_logger.module_init("EmergencyStop", stop_file=str(self._stop_file))

    def start(self) -> None:
        """긴급 중지 감시 시작"""
        if self._running:
            return

        self._running = True
        self._triggered = False
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()

        logger.info("Emergency stop monitoring started")
        structured_logger.module_start("EmergencyStop")

    def stop(self) -> None:
        """긴급 중지 감시 중지"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

        logger.info("Emergency stop monitoring stopped")
        structured_logger.module_stop("EmergencyStop")

    def _watch_loop(self) -> None:
        """파일 감시 루프"""
        while self._running:
            try:
                if self._stop_file.exists() and not self._triggered:
                    self._handle_emergency_stop()
            except Exception as e:
                logger.error(f"Emergency stop check error: {e}")

            time.sleep(self._check_interval)

    def _handle_emergency_stop(self) -> None:
        """긴급 중지 처리"""
        self._triggered = True

        # 파일 내용 읽기 (중지 사유)
        reason = "EMERGENCY_STOP file detected"
        try:
            content = self._stop_file.read_text(encoding="utf-8").strip()
            if content:
                reason = content
        except Exception:
            pass

        logger.critical(f"EMERGENCY STOP triggered: {reason}")

        structured_logger.log(
            LogEventType.EMERGENCY_STOP,
            "EmergencyStop",
            f"Emergency stop triggered: {reason}",
            level="ERROR",
            reason=reason,
        )

        # 콜백 호출
        if self._on_emergency_stop:
            try:
                self._on_emergency_stop(reason)
            except Exception as e:
                logger.error(f"Emergency stop callback failed: {e}")

        # 파일 삭제 (재시작 가능하도록)
        try:
            self._stop_file.unlink()
            logger.info("Emergency stop file removed")
        except Exception as e:
            logger.error(f"Failed to remove emergency stop file: {e}")

    def trigger_manually(self, reason: str = "Manual trigger") -> None:
        """
        수동으로 긴급 중지 트리거

        Args:
            reason: 중지 사유
        """
        if self._triggered:
            logger.warning("Emergency stop already triggered")
            return

        # 파일 생성
        self._stop_file.write_text(reason, encoding="utf-8")
        logger.info(f"Emergency stop file created: {reason}")

    def is_triggered(self) -> bool:
        """긴급 중지 트리거 여부"""
        return self._triggered

    def reset(self) -> None:
        """긴급 중지 상태 초기화"""
        self._triggered = False

        # 파일 삭제
        if self._stop_file.exists():
            self._stop_file.unlink()

        logger.info("Emergency stop state reset")


def create_emergency_stop_handler(
    order_manager,
    slack_notifier=None,
    on_stopped: Optional[Callable[[], None]] = None,
) -> Callable[[str], None]:
    """
    긴급 중지 핸들러 생성

    Args:
        order_manager: OrderManager 인스턴스
        slack_notifier: SlackNotifier 인스턴스 (선택)
        on_stopped: 중지 완료 후 콜백

    Returns:
        긴급 중지 콜백 함수
    """
    def handler(reason: str) -> None:
        logger.critical(f"Executing emergency stop: {reason}")

        # 1. 미체결 주문 취소
        try:
            cancelled = order_manager.cancel_all_pending()
            logger.info(f"Cancelled {cancelled} pending orders")
        except Exception as e:
            logger.error(f"Failed to cancel orders: {e}")

        # 2. Slack 알림
        if slack_notifier:
            try:
                slack_notifier.send_alert(
                    title="🚨 긴급 중지 발생",
                    message=f"사유: {reason}\n미체결 주문이 모두 취소되었습니다.",
                    level="critical",
                )
            except Exception as e:
                logger.error(f"Failed to send Slack alert: {e}")

        # 3. 완료 콜백
        if on_stopped:
            try:
                on_stopped()
            except Exception as e:
                logger.error(f"On-stopped callback failed: {e}")

    return handler
