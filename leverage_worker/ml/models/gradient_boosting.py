# -*- coding: utf-8 -*-
"""
그래디언트 부스팅 모델들

LightGBM, XGBoost, CatBoost, RandomForest 구현
kis-trader에서 이식
"""

from typing import Dict, Any, Optional
import warnings
import numpy as np
from .base_model import BaseModel


class LightGBMModel(BaseModel):
    """LightGBM 모델"""

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        default_params = {
            'objective': 'binary',
            'metric': 'binary_logloss',
            'boosting_type': 'gbdt',
            'n_estimators': 200,
            'max_depth': 8,
            'learning_rate': 0.05,
            'num_leaves': 31,
            'min_child_samples': 50,
            'reg_alpha': 0.1,
            'reg_lambda': 0.1,
            'random_state': 42,
            'verbose': -1,
            'n_jobs': -1,
        }
        if params:
            default_params.update(params)

        super().__init__(name="LightGBM", params=default_params)

    def fit(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
    ) -> 'LightGBMModel':
        try:
            import lightgbm as lgb
        except ImportError:
            raise ImportError("lightgbm이 설치되어 있지 않습니다. pip install lightgbm")

        callbacks = []
        eval_set = None

        if X_val is not None and y_val is not None:
            eval_set = [(X_val, y_val)]
            callbacks.append(lgb.early_stopping(stopping_rounds=50, verbose=False))

        self.model = lgb.LGBMClassifier(**self.params)
        self.model.fit(
            X_train, y_train,
            eval_set=eval_set,
            callbacks=callbacks if callbacks else None,
        )

        self.is_fitted = True
        self.feature_importance_ = self.model.feature_importances_
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            return self.model.predict(X)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            return self.model.predict_proba(X)


class XGBoostModel(BaseModel):
    """XGBoost 모델"""

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        default_params = {
            'objective': 'binary:logistic',
            'eval_metric': 'logloss',
            'n_estimators': 200,
            'max_depth': 6,
            'learning_rate': 0.05,
            'min_child_weight': 5,
            'reg_alpha': 0.1,
            'reg_lambda': 0.1,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'random_state': 42,
            'n_jobs': -1,
            'verbosity': 0,
        }
        if params:
            default_params.update(params)

        super().__init__(name="XGBoost", params=default_params)

    def fit(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
    ) -> 'XGBoostModel':
        try:
            import xgboost as xgb
        except ImportError:
            raise ImportError("xgboost가 설치되어 있지 않습니다. pip install xgboost")

        eval_set = None
        early_stopping_rounds = None

        if X_val is not None and y_val is not None:
            eval_set = [(X_val, y_val)]
            early_stopping_rounds = 50

        self.model = xgb.XGBClassifier(**self.params)
        self.model.fit(
            X_train, y_train,
            eval_set=eval_set,
            early_stopping_rounds=early_stopping_rounds,
            verbose=False,
        )

        self.is_fitted = True
        self.feature_importance_ = self.model.feature_importances_
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.model.predict(X)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        return self.model.predict_proba(X)


class CatBoostModel(BaseModel):
    """CatBoost 모델"""

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        default_params = {
            'iterations': 200,
            'depth': 6,
            'learning_rate': 0.05,
            'l2_leaf_reg': 3,
            'random_seed': 42,
            'verbose': False,
            'thread_count': -1,
        }
        if params:
            default_params.update(params)

        super().__init__(name="CatBoost", params=default_params)

    def fit(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
    ) -> 'CatBoostModel':
        try:
            from catboost import CatBoostClassifier
        except ImportError:
            raise ImportError("catboost가 설치되어 있지 않습니다. pip install catboost")

        eval_set = None
        early_stopping_rounds = None

        if X_val is not None and y_val is not None:
            eval_set = (X_val, y_val)
            early_stopping_rounds = 50

        self.model = CatBoostClassifier(**self.params)
        self.model.fit(
            X_train, y_train,
            eval_set=eval_set,
            early_stopping_rounds=early_stopping_rounds,
        )

        self.is_fitted = True
        self.feature_importance_ = self.model.feature_importances_
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.model.predict(X).flatten()

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        return self.model.predict_proba(X)


class RandomForestModel(BaseModel):
    """RandomForest 모델"""

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        default_params = {
            'n_estimators': 200,
            'max_depth': 10,
            'min_samples_split': 20,
            'min_samples_leaf': 10,
            'max_features': 'sqrt',
            'random_state': 42,
            'n_jobs': -1,
        }
        if params:
            default_params.update(params)

        super().__init__(name="RandomForest", params=default_params)

    def fit(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
    ) -> 'RandomForestModel':
        from sklearn.ensemble import RandomForestClassifier

        self.model = RandomForestClassifier(**self.params)
        self.model.fit(X_train, y_train)

        self.is_fitted = True
        self.feature_importance_ = self.model.feature_importances_
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.model.predict(X)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        return self.model.predict_proba(X)


def create_model(model_type: str, params: Optional[Dict[str, Any]] = None) -> BaseModel:
    """
    모델 생성 팩토리 함수

    Args:
        model_type: 모델 타입 ('lgb', 'xgb', 'cat', 'rf')
        params: 모델 파라미터

    Returns:
        BaseModel 인스턴스
    """
    models = {
        'lgb': LightGBMModel,
        'lightgbm': LightGBMModel,
        'xgb': XGBoostModel,
        'xgboost': XGBoostModel,
        'cat': CatBoostModel,
        'catboost': CatBoostModel,
        'rf': RandomForestModel,
        'randomforest': RandomForestModel,
    }

    model_type = model_type.lower()
    if model_type not in models:
        raise ValueError(f"Unknown model type: {model_type}. Available: {list(models.keys())}")

    return models[model_type](params)
