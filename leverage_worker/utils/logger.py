"""
로깅 유틸리티

파일 + 콘솔 동시 출력
일별 로그 파일 생성
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


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

    # 포맷터 설정
    console_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S"
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
