"""
전략 레지스트리 모듈

전략 등록 및 조회
- 전략 클래스 등록
- 이름으로 전략 인스턴스 생성
"""

from typing import Any, Dict, Optional, Type

from leverage_worker.strategy.base import BaseStrategy
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


class StrategyRegistry:
    """
    전략 레지스트리

    전략 클래스를 등록하고 이름으로 인스턴스를 생성
    """

    _strategies: Dict[str, Type[BaseStrategy]] = {}

    @classmethod
    def register(cls, name: str, strategy_class: Type[BaseStrategy]) -> None:
        """
        전략 클래스 등록

        Args:
            name: 전략 이름 (고유 식별자)
            strategy_class: BaseStrategy 상속 클래스
        """
        if not issubclass(strategy_class, BaseStrategy):
            raise TypeError(f"{strategy_class} is not a subclass of BaseStrategy")

        if name in cls._strategies:
            logger.warning(f"Strategy '{name}' already registered. Overwriting.")

        cls._strategies[name] = strategy_class
        logger.debug(f"Strategy registered: {name} -> {strategy_class.__name__}")

    @classmethod
    def get(
        cls,
        name: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[BaseStrategy]:
        """
        전략 인스턴스 생성

        Args:
            name: 전략 이름
            params: 전략 파라미터

        Returns:
            BaseStrategy 인스턴스 또는 None
        """
        strategy_class = cls._strategies.get(name)

        if strategy_class is None:
            logger.error(f"Strategy '{name}' not found in registry")
            return None

        try:
            return strategy_class(name=name, params=params)
        except Exception as e:
            logger.error(f"Failed to create strategy '{name}': {e}")
            return None

    @classmethod
    def list_strategies(cls) -> list:
        """등록된 전략 이름 목록"""
        return list(cls._strategies.keys())

    @classmethod
    def has(cls, name: str) -> bool:
        """전략 등록 여부 확인"""
        return name in cls._strategies

    @classmethod
    def clear(cls) -> None:
        """모든 전략 등록 해제 (테스트용)"""
        cls._strategies.clear()


def register_strategy(name: str):
    """
    전략 등록 데코레이터

    Example:
        @register_strategy("my_strategy")
        class MyStrategy(BaseStrategy):
            ...
    """
    def decorator(strategy_class: Type[BaseStrategy]) -> Type[BaseStrategy]:
        StrategyRegistry.register(name, strategy_class)
        return strategy_class

    return decorator
