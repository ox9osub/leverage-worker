"""
2단계 필터링 분류기 (main_beam_4 전용)

Stage 1: 기존 앙상블 모델 (LightGBM, XGBoost, CatBoost, RandomForest)
Stage 2: 메타 학습 모델 (RandomForest)

Look-Ahead Bias가 수정된 main_beam_4 전략에서 사용
"""
import logging
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import joblib
import numpy as np

# sklearn feature names 경고 억제
warnings.filterwarnings("ignore", message="X does not have valid feature names")

logger = logging.getLogger(__name__)


class TwoStageClassifier:
    """
    2단계 필터링 분류기

    Stage 1: 기존 앙상블 모델 (old_model) - threshold 0.75
    Stage 2: 메타 학습 모델 (new_model) - threshold 0.65

    두 단계를 모두 통과해야 시그널 발생
    """

    def __init__(
        self,
        old_model: Optional[Dict[str, Any]] = None,
        new_model: Optional[Any] = None,
        old_model_type: str = "ensemble",
        new_model_type: str = "random_forest",
    ):
        """
        Args:
            old_model: Stage 1 모델 (앙상블 또는 단일 모델)
            new_model: Stage 2 모델 (메타 학습 모델)
            old_model_type: Stage 1 모델 타입 ('ensemble' 또는 'random_forest')
            new_model_type: Stage 2 모델 타입
        """
        self._old_model_raw = old_model or {}
        self._new_model = new_model
        self._old_model_type = old_model_type
        self._new_model_type = new_model_type

        self.feature_cols: Optional[List[str]] = None
        self.old_threshold: float = 0.75
        self.new_threshold: float = 0.65
        self.max_hold: int = 4
        self.config: Dict[str, Any] = {}

    @classmethod
    def load(cls, model_path: Union[str, Path]) -> "TwoStageClassifier":
        """
        저장된 main_beam_4 모델 로드

        Args:
            model_path: main_beam_4.joblib 파일 경로

        Returns:
            TwoStageClassifier 인스턴스

        모델 파일 형식 (joblib):
            {
                'old_model': dict | sklearn model,
                'old_model_type': 'ensemble' | 'random_forest',
                'old_threshold': float,
                'new_model': sklearn model,
                'new_model_type': 'random_forest',
                'new_threshold': float,
                'feature_cols': List[str],
                'max_hold': int,
                'config': dict,
            }
        """
        model_path = Path(model_path)
        if not model_path.exists():
            raise FileNotFoundError(f"모델 파일 없음: {model_path}")

        data = joblib.load(model_path)

        instance = cls(
            old_model=data.get("old_model"),
            new_model=data.get("new_model"),
            old_model_type=data.get("old_model_type", "ensemble"),
            new_model_type=data.get("new_model_type", "random_forest"),
        )

        # 메타데이터 로드
        instance.feature_cols = data.get("feature_cols", [])
        instance.old_threshold = data.get("old_threshold", 0.75)
        instance.new_threshold = data.get("new_threshold", 0.65)
        instance.max_hold = data.get("max_hold", 4)
        instance.config = data.get("config", {})

        # 모델 개수 확인
        old_model_count = (
            len(instance._old_model_raw)
            if isinstance(instance._old_model_raw, dict)
            else 1
        )

        logger.info(
            f"2단계 모델 로드 완료: {model_path.name} | "
            f"Stage1: {instance._old_model_type} ({old_model_count}개) @ {instance.old_threshold} | "
            f"Stage2: {instance._new_model_type} @ {instance.new_threshold} | "
            f"피처 {len(instance.feature_cols)}개"
        )

        return instance

    def _predict_proba_old(self, X: np.ndarray) -> np.ndarray:
        """
        Stage 1 (old_model) 확률 예측

        앙상블인 경우 평균 확률 반환
        """
        if self._old_model_type == "ensemble" and isinstance(
            self._old_model_raw, dict
        ):
            probas = []
            for name, model in self._old_model_raw.items():
                try:
                    proba = model.predict_proba(X)
                    if proba.ndim == 2 and proba.shape[1] >= 2:
                        probas.append(proba[:, 1])
                    else:
                        probas.append(proba.flatten())
                except Exception as e:
                    logger.warning(f"Stage1 모델 '{name}' 예측 실패: {e}")
                    continue

            if not probas:
                raise RuntimeError("Stage1: 모든 모델 예측 실패")

            avg_proba = np.mean(probas, axis=0)
            return np.column_stack([1 - avg_proba, avg_proba])
        else:
            # 단일 모델
            return self._old_model_raw.predict_proba(X)

    def _predict_proba_new(self, X: np.ndarray) -> np.ndarray:
        """Stage 2 (new_model) 확률 예측"""
        if self._new_model is None:
            raise RuntimeError("Stage2 모델이 로드되지 않았습니다.")
        return self._new_model.predict_proba(X)

    def get_two_stage_probability(
        self, X: np.ndarray
    ) -> Tuple[float, float, bool]:
        """
        2단계 필터링 수행

        Args:
            X: 입력 피처 배열 (1, n_features)

        Returns:
            Tuple[old_proba, new_proba, signal]
            - old_proba: Stage 1 확률
            - new_proba: Stage 2 확률 (Stage 1 통과 시에만 유효)
            - signal: 최종 시그널 여부
        """
        # Stage 1
        old_proba = float(self._predict_proba_old(X)[0, 1])
        if old_proba < self.old_threshold:
            return old_proba, 0.0, False

        # Stage 2
        new_proba = float(self._predict_proba_new(X)[0, 1])
        signal = new_proba >= self.new_threshold

        return old_proba, new_proba, signal

    def get_signal_probability(self, X: np.ndarray) -> float:
        """
        EnsembleClassifier 호환 인터페이스

        2단계 통과 시 new_proba 반환, 아니면 0.0
        """
        old_proba, new_proba, signal = self.get_two_stage_probability(X)
        if signal:
            return new_proba
        return 0.0

    @property
    def threshold(self) -> float:
        """EnsembleClassifier 호환: new_threshold 반환"""
        return self.new_threshold

    @property
    def n_models(self) -> int:
        """Stage 1 앙상블 모델 개수"""
        if isinstance(self._old_model_raw, dict):
            return len(self._old_model_raw)
        return 1
