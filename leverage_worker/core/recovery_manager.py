"""
장애 복구 관리자

비정상 종료 감지 및 복구
- 하트비트 파일 관리
- 크래시 로그 저장
- 미체결 주문 복구
"""

import json
import os
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from leverage_worker.utils.logger import get_logger
from leverage_worker.utils.structured_logger import get_structured_logger
from leverage_worker.utils.log_constants import LogEventType

logger = get_logger(__name__)
structured_logger = get_structured_logger()


@dataclass
class SessionState:
    """세션 상태 정보"""
    session_id: str
    pid: int
    status: str  # running, stopped, crashed
    started_at: str
    last_heartbeat: str
    active_orders: List[str]
    positions: List[str]
    metadata: Dict[str, Any]


class RecoveryManager:
    """
    장애 복구 관리자

    - 하트비트 파일로 생존 확인
    - 비정상 종료 감지
    - 세션 상태 복구
    """

    def __init__(
        self,
        state_dir: Optional[Path] = None,
        heartbeat_interval_seconds: int = 30,
        on_crash_detected: Optional[Callable[[SessionState], None]] = None,
    ):
        if state_dir is None:
            state_dir = Path.home() / ".leverage_worker"

        state_dir.mkdir(parents=True, exist_ok=True)

        self._state_dir = state_dir
        self._heartbeat_interval = heartbeat_interval_seconds
        self._on_crash_detected = on_crash_detected

        self._state_file = state_dir / "session_state.json"
        self._crash_log_file = state_dir / "crash_log.json"

        self._session_id: Optional[str] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

        logger.info(f"RecoveryManager initialized: {state_dir}")
        structured_logger.module_init("RecoveryManager", state_dir=str(state_dir))

    def check_previous_crash(self) -> Optional[SessionState]:
        """
        이전 세션의 비정상 종료 확인

        Returns:
            크래시된 세션 상태 또는 None
        """
        if not self._state_file.exists():
            return None

        try:
            with open(self._state_file, encoding="utf-8") as f:
                data = json.load(f)

            state = SessionState(
                session_id=data.get("session_id", ""),
                pid=data.get("pid", 0),
                status=data.get("status", "unknown"),
                started_at=data.get("started_at", ""),
                last_heartbeat=data.get("last_heartbeat", ""),
                active_orders=data.get("active_orders", []),
                positions=data.get("positions", []),
                metadata=data.get("metadata", {}),
            )

            # running 상태로 남아있으면 크래시
            if state.status == "running":
                logger.warning(f"Previous session crash detected: {state.session_id}")

                structured_logger.log(
                    LogEventType.RECOVERY_START,
                    "RecoveryManager",
                    f"Crash detected for session {state.session_id}",
                    level="WARNING",
                    crashed_session_id=state.session_id,
                    last_heartbeat=state.last_heartbeat,
                    active_orders_count=len(state.active_orders),
                )

                # 크래시 로그 저장
                self._save_crash_log(state)

                # 콜백 호출
                if self._on_crash_detected:
                    try:
                        self._on_crash_detected(state)
                    except Exception as e:
                        logger.error(f"Crash detection callback failed: {e}")

                return state

            return None

        except Exception as e:
            logger.error(f"Failed to read session state: {e}")
            return None

    def _save_crash_log(self, state: SessionState) -> None:
        """크래시 로그 저장"""
        try:
            crash_logs = []
            if self._crash_log_file.exists():
                with open(self._crash_log_file, encoding="utf-8") as f:
                    crash_logs = json.load(f)

            crash_entry = {
                "detected_at": datetime.now().isoformat(),
                "session": asdict(state),
            }
            crash_logs.append(crash_entry)

            # 최근 100개만 유지
            crash_logs = crash_logs[-100:]

            with open(self._crash_log_file, "w", encoding="utf-8") as f:
                json.dump(crash_logs, f, ensure_ascii=False, indent=2)

        except Exception as e:
            logger.error(f"Failed to save crash log: {e}")

    def start_session(self, session_id: str) -> None:
        """
        새 세션 시작

        Args:
            session_id: 세션 ID
        """
        self._session_id = session_id

        # 세션 상태 저장
        self._save_state("running")

        # 하트비트 스레드 시작
        self._running = True
        self._thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._thread.start()

        logger.info(f"Session started: {session_id}")
        structured_logger.module_start("RecoveryManager", session_id=session_id)

    def stop_session(self) -> None:
        """세션 정상 종료"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

        # 상태를 stopped로 변경
        self._save_state("stopped")

        logger.info(f"Session stopped: {self._session_id}")
        structured_logger.module_stop("RecoveryManager", session_id=self._session_id)

    def update_active_orders(self, order_ids: List[str]) -> None:
        """활성 주문 목록 업데이트"""
        self._save_state("running", active_orders=order_ids)

    def update_positions(self, stock_codes: List[str]) -> None:
        """포지션 목록 업데이트"""
        self._save_state("running", positions=stock_codes)

    def _heartbeat_loop(self) -> None:
        """하트비트 루프"""
        while self._running:
            try:
                self._save_state("running")
            except Exception as e:
                logger.error(f"Heartbeat update failed: {e}")

            time.sleep(self._heartbeat_interval)

    def _save_state(
        self,
        status: str,
        active_orders: Optional[List[str]] = None,
        positions: Optional[List[str]] = None,
    ) -> None:
        """세션 상태 저장"""
        try:
            # 기존 상태 읽기
            existing = {}
            if self._state_file.exists():
                with open(self._state_file, encoding="utf-8") as f:
                    existing = json.load(f)

            now = datetime.now().isoformat()

            state = {
                "session_id": self._session_id,
                "pid": os.getpid(),
                "status": status,
                "started_at": existing.get("started_at", now),
                "last_heartbeat": now,
                "active_orders": active_orders if active_orders is not None else existing.get("active_orders", []),
                "positions": positions if positions is not None else existing.get("positions", []),
                "metadata": existing.get("metadata", {}),
            }

            with open(self._state_file, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)

        except Exception as e:
            logger.error(f"Failed to save session state: {e}")

    def get_crash_logs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        크래시 로그 조회

        Args:
            limit: 최대 조회 개수

        Returns:
            크래시 로그 리스트 (최신순)
        """
        if not self._crash_log_file.exists():
            return []

        try:
            with open(self._crash_log_file, encoding="utf-8") as f:
                crash_logs = json.load(f)
            return list(reversed(crash_logs[-limit:]))
        except Exception as e:
            logger.error(f"Failed to read crash logs: {e}")
            return []

    def clear_state(self) -> None:
        """상태 파일 삭제"""
        if self._state_file.exists():
            self._state_file.unlink()
        logger.info("Session state cleared")
