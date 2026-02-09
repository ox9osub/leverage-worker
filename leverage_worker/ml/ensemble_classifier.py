"""
앙상블 분류기 래퍼

LightGBM, XGBoost, CatBoost, RandomForest 모델의 predict_proba 평균 계산
main_beam_1 전략 (90_1 지정가 매수)에서 사용
"""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import joblib
import numpy as np

logger = logging.getLogger(__name__)


class EnsembleClassifier:
    """
    앙상블 분류기 래퍼

    여러 모델의 predict_proba 결과를 평균하여 최종 확률 반환
    """

    def __init__(self, models: Optional[Dict[str, Any]] = None):
        """
        Args:
            models: 분류 모델 딕셔너리 {'lightgbm': model, 'xgboost': model, ...}
                    각 모델은 predict_proba 메서드 필요
        """
        self.models = models or {}
        self.feature_cols: Optional[List[str]] = None
        self.threshold: float = 0.60
        self.max_hold: int = 1
        self.config: Dict[str, Any] = {}

    @classmethod
    def load(cls, model_path: Union[str, Path]) -> "EnsembleClassifier":
        """
        저장된 모델 로드

        Args:
            model_path: .joblib 파일 경로

        Returns:
            EnsembleClassifier 인스턴스

        모델 파일 형식 (joblib):
            {
                'model_type': 'ensemble' | 'random_forest',
                'model': dict | sklearn model,
                'threshold': float,
                'feature_cols': List[str],
                'config': dict,
                'max_hold': int,
            }
        """
        model_path = Path(model_path)
        if not model_path.exists():
            raise FileNotFoundError(f"모델 파일 없음: {model_path}")

        data = joblib.load(model_path)

        instance = cls()

        # 모델 타입에 따라 처리
        model_type = data.get("model_type", "ensemble")

        if model_type == "ensemble":
            # 앙상블 모델: dict 형태 {'lightgbm': model, ...}
            instance.models = data.get("model", {})
        else:
            # 단일 모델 (random_forest 등): 직접 저장
            instance.models = {"single": data.get("model")}

        # 메타데이터 로드
        instance.feature_cols = data.get("feature_cols", [])
        instance.threshold = data.get("threshold", 0.60)
        instance.max_hold = data.get("max_hold", 1)
        instance.config = data.get("config", {})

        logger.info(
            f"앙상블 모델 로드 완료: {model_path.name} | "
            f"모델 {len(instance.models)}개 | "
            f"피처 {len(instance.feature_cols)}개 | "
            f"threshold={instance.threshold}"
        )

        return instance

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """
        확률 예측 (앙상블 평균)

        Args:
            X: 입력 피처 배열 (n_samples, n_features)

        Returns:
            확률 배열 (n_samples, 2) - [P(class=0), P(class=1)]
        """
        if not self.models:
            raise RuntimeError("모델이 로드되지 않았습니다.")

        probas = []
        for name, model in self.models.items():
            try:
                proba = model.predict_proba(X)
                # 이진 분류: class 1의 확률만 추출
                if proba.ndim == 2 and proba.shape[1] >= 2:
                    probas.append(proba[:, 1])
                else:
                    probas.append(proba.flatten())
            except Exception as e:
                logger.warning(f"모델 '{name}' 예측 실패: {e}")
                continue

        if not probas:
            raise RuntimeError("모든 모델 예측 실패")

        # 확률 평균
        avg_proba = np.mean(probas, axis=0)

        # (n_samples, 2) 형태로 반환
        return np.column_stack([1 - avg_proba, avg_proba])

    def predict(self, X: np.ndarray, threshold: Optional[float] = None) -> np.ndarray:
        """
        클래스 예측

        Args:
            X: 입력 피처 배열
            threshold: 분류 임계값 (None이면 self.threshold 사용)

        Returns:
            예측 클래스 배열 (0 또는 1)
        """
        if threshold is None:
            threshold = self.threshold

        proba = self.predict_proba(X)
        return (proba[:, 1] >= threshold).astype(int)

    def get_signal_probability(self, X: np.ndarray) -> float:
        """
        단일 샘플의 신호 확률 반환

        Args:
            X: 입력 피처 배열 (1, n_features)

        Returns:
            class 1 (신호 발생)의 확률
        """
        proba = self.predict_proba(X)
        return float(proba[0, 1])

    @property
    def n_models(self) -> int:
        """앙상블 모델 개수"""
        return len(self.models)

    @property
    def model_names(self) -> List[str]:
        """앙상블 모델 이름 목록"""
        return list(self.models.keys())
