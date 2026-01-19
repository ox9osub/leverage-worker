# -*- coding: utf-8 -*-
"""
ML 모델 기본 인터페이스

kis-trader에서 이식
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import numpy as np
import pandas as pd
import pickle
from pathlib import Path


class BaseModel(ABC):
    """
    모든 ML 모델의 기본 클래스

    모든 모델은 이 인터페이스를 구현해야 합니다.
    """

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        self.name = name
        self.params = params or {}
        self.model = None
        self.is_fitted = False
        self.feature_names = None
        self.feature_importance_ = None

    @abstractmethod
    def fit(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
    ) -> 'BaseModel':
        """
        모델 학습

        Args:
            X_train: 학습 피처
            y_train: 학습 라벨
            X_val: 검증 피처 (선택)
            y_val: 검증 라벨 (선택)

        Returns:
            self
        """
        pass

    @abstractmethod
    def predict(self, X: np.ndarray) -> np.ndarray:
        """
        예측

        Args:
            X: 피처

        Returns:
            예측 라벨 (0 또는 1)
        """
        pass

    @abstractmethod
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """
        예측 확률

        Args:
            X: 피처

        Returns:
            각 클래스의 확률 (shape: [n_samples, 2])
        """
        pass

    def save(self, path: str) -> None:
        """
        모델 저장

        Args:
            path: 저장 경로
        """
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        save_data = {
            'name': self.name,
            'params': self.params,
            'model': self.model,
            'is_fitted': self.is_fitted,
            'feature_names': self.feature_names,
            'feature_importance_': self.feature_importance_,
        }

        with open(path, 'wb') as f:
            pickle.dump(save_data, f)

    def load(self, path: str) -> 'BaseModel':
        """
        모델 로드

        Args:
            path: 저장된 모델 경로

        Returns:
            self
        """
        with open(path, 'rb') as f:
            save_data = pickle.load(f)

        self.name = save_data['name']
        self.params = save_data['params']
        self.model = save_data['model']
        self.is_fitted = save_data['is_fitted']
        self.feature_names = save_data['feature_names']
        self.feature_importance_ = save_data.get('feature_importance_')

        return self

    def get_feature_importance(self, feature_names: Optional[list] = None) -> pd.DataFrame:
        """
        피처 중요도 반환

        Args:
            feature_names: 피처 이름 리스트

        Returns:
            피처 중요도 DataFrame
        """
        if self.feature_importance_ is None:
            return pd.DataFrame()

        names = feature_names or self.feature_names or [f"feature_{i}" for i in range(len(self.feature_importance_))]

        df = pd.DataFrame({
            'feature': names,
            'importance': self.feature_importance_
        })
        df = df.sort_values('importance', ascending=False).reset_index(drop=True)
        return df

    def __str__(self) -> str:
        return f"{self.name}(fitted={self.is_fitted}, params={self.params})"
