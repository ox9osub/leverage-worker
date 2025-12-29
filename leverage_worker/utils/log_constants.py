"""
로그 이벤트 상수 정의

모듈별 시작/수행/종료 로그를 위한 표준 이벤트 타입
"""

from enum import Enum


class LogEventType(str, Enum):
    """로그 이벤트 타입"""

    # 생명주기 이벤트
    MODULE_INIT = "MODULE_INIT"
    MODULE_START = "MODULE_START"
    MODULE_STOP = "MODULE_STOP"
    MODULE_ERROR = "MODULE_ERROR"

    # 거래 이벤트
    ORDER_SUBMIT = "ORDER_SUBMIT"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_PARTIAL = "ORDER_PARTIAL"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    ORDER_REJECTED = "ORDER_REJECTED"

    # 포지션 이벤트
    POSITION_OPEN = "POSITION_OPEN"
    POSITION_UPDATE = "POSITION_UPDATE"
    POSITION_CLOSE = "POSITION_CLOSE"
    POSITION_SYNC = "POSITION_SYNC"

    # 전략 이벤트
    STRATEGY_LOAD = "STRATEGY_LOAD"
    SIGNAL_GENERATE = "SIGNAL_GENERATE"
    SIGNAL_EXECUTE = "SIGNAL_EXECUTE"

    # API 이벤트
    API_REQUEST = "API_REQUEST"
    API_RESPONSE = "API_RESPONSE"
    API_ERROR = "API_ERROR"
    API_RETRY = "API_RETRY"

    # 인증 이벤트
    AUTH_SUCCESS = "AUTH_SUCCESS"
    AUTH_FAILURE = "AUTH_FAILURE"
    TOKEN_REFRESH = "TOKEN_REFRESH"

    # 스케줄러 이벤트
    MARKET_OPEN = "MARKET_OPEN"
    MARKET_CLOSE = "MARKET_CLOSE"
    TICK_PROCESS = "TICK_PROCESS"

    # 시스템 이벤트
    HEALTH_CHECK = "HEALTH_CHECK"
    EMERGENCY_STOP = "EMERGENCY_STOP"
    RECOVERY_START = "RECOVERY_START"


class LogCategory(str, Enum):
    """로그 카테고리"""

    LIFECYCLE = "lifecycle"
    TRADING = "trading"
    POSITION = "position"
    STRATEGY = "strategy"
    API = "api"
    AUTH = "auth"
    SCHEDULER = "scheduler"
    SYSTEM = "system"


# 이벤트 타입 -> 카테고리 매핑
EVENT_CATEGORY_MAP = {
    # 생명주기
    LogEventType.MODULE_INIT: LogCategory.LIFECYCLE,
    LogEventType.MODULE_START: LogCategory.LIFECYCLE,
    LogEventType.MODULE_STOP: LogCategory.LIFECYCLE,
    LogEventType.MODULE_ERROR: LogCategory.LIFECYCLE,

    # 거래
    LogEventType.ORDER_SUBMIT: LogCategory.TRADING,
    LogEventType.ORDER_FILLED: LogCategory.TRADING,
    LogEventType.ORDER_PARTIAL: LogCategory.TRADING,
    LogEventType.ORDER_CANCELLED: LogCategory.TRADING,
    LogEventType.ORDER_REJECTED: LogCategory.TRADING,

    # 포지션
    LogEventType.POSITION_OPEN: LogCategory.POSITION,
    LogEventType.POSITION_UPDATE: LogCategory.POSITION,
    LogEventType.POSITION_CLOSE: LogCategory.POSITION,
    LogEventType.POSITION_SYNC: LogCategory.POSITION,

    # 전략
    LogEventType.STRATEGY_LOAD: LogCategory.STRATEGY,
    LogEventType.SIGNAL_GENERATE: LogCategory.STRATEGY,
    LogEventType.SIGNAL_EXECUTE: LogCategory.STRATEGY,

    # API
    LogEventType.API_REQUEST: LogCategory.API,
    LogEventType.API_RESPONSE: LogCategory.API,
    LogEventType.API_ERROR: LogCategory.API,
    LogEventType.API_RETRY: LogCategory.API,

    # 인증
    LogEventType.AUTH_SUCCESS: LogCategory.AUTH,
    LogEventType.AUTH_FAILURE: LogCategory.AUTH,
    LogEventType.TOKEN_REFRESH: LogCategory.AUTH,

    # 스케줄러
    LogEventType.MARKET_OPEN: LogCategory.SCHEDULER,
    LogEventType.MARKET_CLOSE: LogCategory.SCHEDULER,
    LogEventType.TICK_PROCESS: LogCategory.SCHEDULER,

    # 시스템
    LogEventType.HEALTH_CHECK: LogCategory.SYSTEM,
    LogEventType.EMERGENCY_STOP: LogCategory.SYSTEM,
    LogEventType.RECOVERY_START: LogCategory.SYSTEM,
}


def get_category(event_type: LogEventType) -> LogCategory:
    """이벤트 타입에 해당하는 카테고리 반환"""
    return EVENT_CATEGORY_MAP.get(event_type, LogCategory.SYSTEM)
