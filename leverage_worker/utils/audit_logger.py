"""
감사 추적 로거

금융 거래에 대한 영구적인 감사 추적 기록
- SQLite DB에 저장
- 체크섬으로 데이터 무결성 보장
- 모든 주문, 체결, 포지션 변경 기록
"""

import hashlib
import json
import sqlite3
import threading
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class AuditEvent:
    """감사 이벤트 데이터"""
    timestamp: str
    event_type: str
    module: str
    stock_code: Optional[str] = None
    stock_name: Optional[str] = None
    order_id: Optional[str] = None
    side: Optional[str] = None
    quantity: Optional[int] = None
    price: Optional[float] = None
    amount: Optional[float] = None
    strategy_name: Optional[str] = None
    status: Optional[str] = None
    reason: Optional[str] = None
    correlation_id: Optional[str] = None
    session_id: Optional[str] = None
    metadata: Optional[str] = None  # JSON string


class AuditLogger:
    """
    감사 추적 로거

    - 모든 금융 거래 기록을 SQLite에 영구 저장
    - 체크섬으로 데이터 무결성 보장
    - 스레드 안전
    """

    def __init__(self, log_dir: Optional[Path] = None):
        if log_dir is None:
            log_dir = Path(__file__).parent.parent.parent / "logs" / "audit"

        log_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = log_dir / "audit_trail.db"
        self._lock = threading.Lock()
        self._init_db()

        logger.info(f"AuditLogger initialized: {self._db_path}")

    def _init_db(self) -> None:
        """감사 로그 테이블 초기화"""
        with sqlite3.connect(str(self._db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    module TEXT NOT NULL,
                    correlation_id TEXT,
                    session_id TEXT,

                    -- 거래 정보
                    stock_code TEXT,
                    stock_name TEXT,
                    order_id TEXT,
                    side TEXT,
                    quantity INTEGER,
                    price REAL,
                    amount REAL,
                    strategy_name TEXT,

                    -- 상태 정보
                    status TEXT,
                    reason TEXT,

                    -- 메타데이터 (JSON)
                    metadata TEXT,

                    -- 무결성 검증용 해시
                    checksum TEXT NOT NULL
                )
            """)

            # 인덱스 생성
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_audit_stock ON audit_log(stock_code)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_audit_order ON audit_log(order_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_audit_correlation ON audit_log(correlation_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_log(event_type)"
            )

    def _compute_checksum(self, data: Dict[str, Any]) -> str:
        """데이터 무결성 검증용 체크섬 계산 (SHA-256)"""
        content = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()[:32]

    def log_order(
        self,
        event_type: str,
        module: str,
        stock_code: str,
        stock_name: str,
        order_id: Optional[str],
        side: str,
        quantity: int,
        price: float,
        strategy_name: str,
        status: str,
        reason: Optional[str] = None,
        correlation_id: Optional[str] = None,
        session_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> None:
        """
        주문 관련 감사 로그 기록

        Args:
            event_type: ORDER_SUBMIT, ORDER_FILLED, ORDER_CANCELLED, ORDER_REJECTED
            module: 기록 모듈명
            stock_code: 종목코드
            stock_name: 종목명
            order_id: 주문 ID
            side: BUY/SELL
            quantity: 수량
            price: 가격
            strategy_name: 전략명
            status: 상태
            reason: 사유
            correlation_id: 상관관계 ID
            session_id: 세션 ID
            metadata: 추가 메타데이터
        """
        data = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "module": module,
            "correlation_id": correlation_id,
            "session_id": session_id,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "order_id": order_id,
            "side": side,
            "quantity": quantity,
            "price": price,
            "amount": quantity * price if quantity and price else None,
            "strategy_name": strategy_name,
            "status": status,
            "reason": reason,
            "metadata": json.dumps(metadata) if metadata else None,
        }

        checksum = self._compute_checksum(data)

        with self._lock:
            try:
                with sqlite3.connect(str(self._db_path)) as conn:
                    conn.execute("""
                        INSERT INTO audit_log (
                            timestamp, event_type, module, correlation_id, session_id,
                            stock_code, stock_name, order_id, side, quantity, price,
                            amount, strategy_name, status, reason, metadata, checksum
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        data["timestamp"], data["event_type"], data["module"],
                        data["correlation_id"], data["session_id"],
                        data["stock_code"], data["stock_name"], data["order_id"],
                        data["side"], data["quantity"], data["price"], data["amount"],
                        data["strategy_name"], data["status"], data["reason"],
                        data["metadata"], checksum
                    ))
            except Exception as e:
                logger.error(f"Failed to write audit log: {e}")

    def log_position(
        self,
        event_type: str,
        module: str,
        stock_code: str,
        stock_name: str,
        quantity: int,
        avg_price: float,
        current_price: float,
        profit_loss: float,
        profit_rate: float,
        strategy_name: Optional[str] = None,
        correlation_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> None:
        """
        포지션 관련 감사 로그 기록

        Args:
            event_type: POSITION_OPEN, POSITION_UPDATE, POSITION_CLOSE, POSITION_SYNC
            module: 기록 모듈명
            stock_code: 종목코드
            stock_name: 종목명
            quantity: 수량
            avg_price: 평균가
            current_price: 현재가
            profit_loss: 손익
            profit_rate: 수익률
            strategy_name: 전략명
            correlation_id: 상관관계 ID
            session_id: 세션 ID
        """
        metadata = {
            "avg_price": avg_price,
            "current_price": current_price,
            "profit_loss": profit_loss,
            "profit_rate": profit_rate,
        }

        data = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "module": module,
            "correlation_id": correlation_id,
            "session_id": session_id,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "quantity": quantity,
            "price": current_price,
            "strategy_name": strategy_name,
            "status": "ACTIVE" if quantity > 0 else "CLOSED",
            "metadata": json.dumps(metadata),
        }

        checksum = self._compute_checksum(data)

        with self._lock:
            try:
                with sqlite3.connect(str(self._db_path)) as conn:
                    conn.execute("""
                        INSERT INTO audit_log (
                            timestamp, event_type, module, correlation_id, session_id,
                            stock_code, stock_name, quantity, price,
                            strategy_name, status, metadata, checksum
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        data["timestamp"], data["event_type"], data["module"],
                        data["correlation_id"], data["session_id"],
                        data["stock_code"], data["stock_name"], data["quantity"],
                        data["price"], data["strategy_name"], data["status"],
                        data["metadata"], checksum
                    ))
            except Exception as e:
                logger.error(f"Failed to write audit log: {e}")

    def get_order_history(
        self,
        stock_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
    ) -> list:
        """
        주문 이력 조회

        Args:
            stock_code: 종목코드 (None이면 전체)
            start_date: 시작일 (YYYY-MM-DD)
            end_date: 종료일 (YYYY-MM-DD)
            limit: 최대 조회 수

        Returns:
            주문 이력 리스트
        """
        query = """
            SELECT * FROM audit_log
            WHERE event_type LIKE 'ORDER_%'
        """
        params = []

        if stock_code:
            query += " AND stock_code = ?"
            params.append(stock_code)

        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date)

        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date + "T23:59:59")

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        with sqlite3.connect(str(self._db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def verify_integrity(self) -> Dict[str, Any]:
        """
        데이터 무결성 검증

        Returns:
            검증 결과 (total, valid, invalid, errors)
        """
        result = {"total": 0, "valid": 0, "invalid": 0, "errors": []}

        with sqlite3.connect(str(self._db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM audit_log")

            for row in cursor:
                result["total"] += 1

                # 체크섬 검증용 데이터 재구성
                data = {
                    "timestamp": row["timestamp"],
                    "event_type": row["event_type"],
                    "module": row["module"],
                    "correlation_id": row["correlation_id"],
                    "session_id": row["session_id"],
                    "stock_code": row["stock_code"],
                    "stock_name": row["stock_name"],
                    "order_id": row["order_id"],
                    "side": row["side"],
                    "quantity": row["quantity"],
                    "price": row["price"],
                    "amount": row["amount"],
                    "strategy_name": row["strategy_name"],
                    "status": row["status"],
                    "reason": row["reason"],
                    "metadata": row["metadata"],
                }

                expected_checksum = self._compute_checksum(data)
                actual_checksum = row["checksum"]

                if expected_checksum == actual_checksum:
                    result["valid"] += 1
                else:
                    result["invalid"] += 1
                    result["errors"].append({
                        "id": row["id"],
                        "expected": expected_checksum,
                        "actual": actual_checksum,
                    })

        return result


# 싱글톤 인스턴스 (선택적 사용)
_audit_logger_instance: Optional[AuditLogger] = None


def get_audit_logger(log_dir: Optional[Path] = None) -> AuditLogger:
    """감사 로거 싱글톤 인스턴스 가져오기"""
    global _audit_logger_instance
    if _audit_logger_instance is None:
        _audit_logger_instance = AuditLogger(log_dir)
    return _audit_logger_instance
