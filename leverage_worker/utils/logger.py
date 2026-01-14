"""
로깅 유틸리티

파일 + 콘솔 동시 출력
일별 로그 파일 생성
민감정보 마스킹 (API 키, 토큰, 계좌번호 등)
"""

import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple


class SensitiveDataFilter(logging.Filter):
    """
    민감정보 마스킹 필터

    로그 메시지에서 다음 정보를 마스킹:
    - app_key, app_secret: API 키
    - authorization/Bearer 토큰
    - 계좌번호 (8자리-2자리 형식)
    - password 관련 값
    """

    PATTERNS: List[Tuple[re.Pattern, str]] = [
        # app_key, app_secret 마스킹 (JSON, YAML, 할당문 등)
        (
            re.compile(
                r'(app_key|appkey|APP_KEY)["\']?\s*[:=]\s*["\']?([A-Za-z0-9_-]{8,})["\']?',
                re.IGNORECASE
            ),
            r'\1: ***MASKED***'
        ),
        (
            re.compile(
                r'(app_secret|appsecret|APP_SECRET)["\']?\s*[:=]\s*["\']?([A-Za-z0-9_-]{8,})["\']?',
                re.IGNORECASE
            ),
            r'\1: ***MASKED***'
        ),
        # Authorization Bearer 토큰 마스킹
        (
            re.compile(
                r'(authorization|bearer)["\']?\s*[:=]?\s*["\']?Bearer\s+([A-Za-z0-9._-]+)',
                re.IGNORECASE
            ),
            r'\1: Bearer ***TOKEN***'
        ),
        # 계좌번호 마스킹 (8자리-2자리 형식)
        (
            re.compile(r'(\d{8})-(\d{2})'),
            r'********-\2'
        ),
        # password 마스킹
        (
            re.compile(
                r'(password|passwd|pwd)["\']?\s*[:=]\s*["\']?([^"\'\s,}]+)',
                re.IGNORECASE
            ),
            r'\1: ***MASKED***'
        ),
        # 토큰 값 직접 마스킹 (긴 영숫자 문자열이 토큰처럼 보이는 경우)
        (
            re.compile(
                r'(access_token|token)["\']?\s*[:=]\s*["\']?([A-Za-z0-9._-]{20,})["\']?',
                re.IGNORECASE
            ),
            r'\1: ***TOKEN***'
        ),
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        """로그 레코드에서 민감정보를 마스킹"""
        # 메시지 마스킹
        if hasattr(record, 'msg') and isinstance(record.msg, str):
            record.msg = self._mask_sensitive_data(record.msg)

        # args 마스킹 (% 포맷팅 사용 시)
        if hasattr(record, 'args') and record.args:
            masked_args = []
            for arg in record.args:
                if isinstance(arg, str):
                    masked_args.append(self._mask_sensitive_data(arg))
                else:
                    masked_args.append(arg)
            record.args = tuple(masked_args)

        return True

    def _mask_sensitive_data(self, text: str) -> str:
        """텍스트에서 민감정보를 마스킹"""
        for pattern, replacement in self.PATTERNS:
            text = pattern.sub(replacement, text)
        return text


def setup_logger(
    name: str = "leverage_worker",
    log_dir: Optional[Path] = None,
    level: int = logging.INFO,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
) -> logging.Logger:
    """
    로거 설정

    Args:
        name: 로거 이름
        log_dir: 로그 파일 저장 디렉토리 (None이면 logs/)
        level: 기본 로그 레벨
        console_level: 콘솔 출력 레벨
        file_level: 파일 출력 레벨

    Returns:
        설정된 Logger 객체
    """
    logger = logging.getLogger(name)

    # 이미 핸들러가 있으면 스킵 (중복 방지)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = False  # 부모 로거로 전파 방지

    # 민감정보 마스킹 필터 추가
    sensitive_filter = SensitiveDataFilter()
    logger.addFilter(sensitive_filter)

    # 포맷터 설정
    console_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # 파일 핸들러
    if log_dir is None:
        log_dir = Path(__file__).parent.parent.parent / "logs"

    log_dir.mkdir(parents=True, exist_ok=True)

    log_filename = f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
    log_path = log_dir / log_filename

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "leverage_worker") -> logging.Logger:
    """
    기존 로거 가져오기

    Args:
        name: 로거 이름

    Returns:
        Logger 객체 (없으면 기본 설정으로 생성)
    """
    logger = logging.getLogger(name)

    # 핸들러가 없으면 기본 설정으로 생성
    if not logger.handlers:
        return setup_logger(name)

    return logger


class TradeLogger:
    """매매 전용 로거 (별도 파일)"""

    def __init__(self, log_dir: Optional[Path] = None):
        if log_dir is None:
            log_dir = Path(__file__).parent.parent.parent / "logs"

        log_dir.mkdir(parents=True, exist_ok=True)

        self._log_dir = log_dir
        self._logger = logging.getLogger("trade_log")
        self._logger.setLevel(logging.INFO)

        # 민감정보 마스킹 필터 추가
        self._logger.addFilter(SensitiveDataFilter())

        # 중복 방지
        if not self._logger.handlers:
            log_filename = f"trades_{datetime.now().strftime('%Y%m%d')}.log"
            log_path = log_dir / log_filename

            handler = logging.FileHandler(log_path, encoding="utf-8")
            handler.setFormatter(logging.Formatter(
                fmt="%(asctime)s|%(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            ))
            self._logger.addHandler(handler)

    def log_order(
        self,
        side: str,
        stock_code: str,
        stock_name: str,
        quantity: int,
        price: float,
        strategy: str,
        order_id: Optional[str] = None,
    ) -> None:
        """주문 로그"""
        self._logger.info(
            f"ORDER|{side}|{stock_code}|{stock_name}|{quantity}|{price}|{strategy}|{order_id or 'N/A'}"
        )

    def log_fill(
        self,
        side: str,
        stock_code: str,
        stock_name: str,
        quantity: int,
        price: float,
        strategy: str,
        order_id: str,
    ) -> None:
        """체결 로그"""
        self._logger.info(
            f"FILL|{side}|{stock_code}|{stock_name}|{quantity}|{price}|{strategy}|{order_id}"
        )

    def log_cancel(
        self,
        stock_code: str,
        order_id: str,
        reason: str,
    ) -> None:
        """취소 로그"""
        self._logger.info(
            f"CANCEL|{stock_code}|{order_id}|{reason}"
        )
