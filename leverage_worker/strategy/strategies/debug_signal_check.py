"""
백테스트 vs 실시간 시그널 불일치 디버깅 스크립트

DB에서 분봉 데이터를 로드하여 피처 값과 ML 예측 결과를 확인합니다.
백테스트에서 시그널이 발생한 시점의 데이터와 비교하여 차이점을 파악합니다.
"""

import pickle
from datetime import datetime
from pathlib import Path

import pandas as pd

# 프로젝트 루트 경로 설정
project_root = Path(__file__).parent.parent.parent.parent
import sys
sys.path.insert(0, str(project_root))

from leverage_worker.data.database import MarketDataDB
from leverage_worker.data.minute_candle_repository import MinuteCandleRepository
from leverage_worker.ml.data_utils import candles_to_dataframe
from leverage_worker.ml.features import TradingFeatureEngineer
from leverage_worker.ml.signal_generator import _ModuleRemappingUnpickler


def check_signal_data(stock_code: str = "122630", target_date: str = "2026-01-27"):
    """
    DB 분봉 데이터로 시그널 조건 확인

    Args:
        stock_code: 종목코드
        target_date: 확인할 날짜 (YYYY-MM-DD)
    """
    print("=" * 80)
    print(f"시그널 디버깅: {stock_code} - {target_date}")
    print("=" * 80)

    # 1. DB에서 분봉 데이터 로드
    print("\n[1] DB 분봉 데이터 로드")
    db = MarketDataDB()
    repo = MinuteCandleRepository(db)
    candles = repo.get_recent_prices(stock_code, count=500)

    print(f"  - 로드된 분봉 개수: {len(candles)}")
    if candles:
        print(f"  - 첫 분봉: {candles[0].candle_datetime}")
        print(f"  - 마지막 분봉: {candles[-1].candle_datetime}")
    else:
        print("  - ERROR: 분봉 데이터 없음!")
        return

    # 2. DataFrame 변환
    print("\n[2] DataFrame 변환")
    df = candles_to_dataframe(candles)
    print(f"  - DataFrame 크기: {df.shape}")

    # 3. 피처 엔지니어링
    print("\n[3] 피처 엔지니어링")
    engineer = TradingFeatureEngineer()
    df = engineer.engineer_features(df)
    print(f"  - 피처 포함 DataFrame 크기: {df.shape}")

    # 4. 특정 날짜 데이터 필터링 (당일 포함 2일)
    print(f"\n[4] {target_date} 포함 최근 2일 데이터 필터링")
    df['date'] = df['timestamp'].dt.date.astype(str)
    unique_dates = sorted(df['date'].unique())

    if target_date in unique_dates:
        target_idx = unique_dates.index(target_date)
        filter_dates = unique_dates[max(0, target_idx - 1):target_idx + 1]
    else:
        filter_dates = [target_date]

    target_df = df[df['date'].isin(filter_dates)].copy()
    print(f"  - 필터링 날짜: {filter_dates}")
    print(f"  - 필터링된 분봉 개수: {len(target_df)}")

    # 5. momentum_5, momentum_10 값 확인
    print("\n[5] 모멘텀 값 확인")
    print("-" * 80)
    print(f"{'시간':^12} {'종가':>10} {'momentum_5':>12} {'momentum_10':>12} {'시그널':^8}")
    print("-" * 80)

    # 백테스트에서 시그널 발생 시점들 (27일 기준)
    signal_times = ["10:02", "10:05", "10:46", "11:34", "11:37", "11:41", "12:00", "12:04", "12:41", "13:10"]

    for idx, row in target_df.iterrows():
        time_str = row['timestamp'].strftime('%H:%M')
        mom5 = row.get('momentum_5', 0)
        mom10 = row.get('momentum_10', 0)
        close = row.get('close', 0)

        # LONG/SHORT 조건 판단
        if mom5 > 0 and mom10 > 0:
            signal_type = "LONG"
        elif mom5 < 0 and mom10 < 0:
            signal_type = "SHORT"
        else:
            signal_type = ""

        # 시그널 시점만 출력
        if time_str in signal_times or signal_type:
            marker = "◀ 시그널" if time_str in signal_times else ""
            print(f"{time_str:^12} {close:>10,.0f} {mom5:>+12.0f} {mom10:>+12.0f} {signal_type:^8} {marker}")

    print("-" * 80)

    # 6. ML 모델 예측 확인
    print("\n[6] ML 모델 예측 확인")
    model_path = project_root / "data" / "ml_models" / "volatility_direction_price_position_20260118.pkl"

    if model_path.exists():
        with open(model_path, 'rb') as f:
            save_data = _ModuleRemappingUnpickler(f).load()

        model = save_data['model']
        feature_columns = save_data['feature_columns']

        print(f"  - 모델 피처 수: {len(feature_columns)}")

        # 피처 존재 여부 확인
        missing_features = [col for col in feature_columns if col not in df.columns]
        if missing_features:
            print(f"  - 누락된 피처: {missing_features[:5]}...")
        else:
            print("  - 모든 피처 존재 OK")

            # vol_prob 예측
            target_features = target_df[feature_columns].values
            vol_probs = model.predict_proba(target_features)[:, 1]

            print("\n  변동성 예측 결과 (시그널 시점):")
            print("-" * 60)
            print(f"  {'시간':^12} {'vol_prob':>10} {'임계값(0.85)':>12} {'통과':^6}")
            print("-" * 60)

            for i, (idx, row) in enumerate(target_df.iterrows()):
                time_str = row['timestamp'].strftime('%H:%M')
                if time_str in signal_times:
                    vp = vol_probs[i]
                    passed = "OK" if vp >= 0.85 else ""
                    print(f"  {time_str:^12} {vp:>10.1%} {0.85:>12.0%} {passed:^6}")

            print("-" * 60)
    else:
        print(f"  - ERROR: 모델 파일 없음: {model_path}")

    # 7. 결론
    print("\n[7] 결론")
    print("-" * 80)

    # LONG 조건 만족 시점 카운트
    long_count = ((target_df['momentum_5'] > 0) & (target_df['momentum_10'] > 0)).sum()
    print(f"  - LONG 조건 (mom5 > 0 AND mom10 > 0) 만족: {long_count}개 시점")

    if long_count == 0:
        print("\n  ⚠️ LONG 조건을 만족하는 시점이 없습니다!")
        print("     → momentum 값 계산에 문제가 있을 수 있습니다.")
        print("     → 전일 데이터가 충분한지 확인하세요.")

    print("=" * 80)


def compare_with_backtest(stock_code: str = "122630"):
    """
    백테스트 데이터와 실시간 DB 데이터 비교
    """
    print("\n" + "=" * 80)
    print("백테스트 vs 실시간 DB 데이터 비교")
    print("=" * 80)

    # 백테스트 데이터 경로 (kis-trader 프로젝트)
    backtest_data_path = Path(r"C:\Users\suble\Desktop\work\project\kis-trader\data\ohlcv\122630\minute_1.parquet")

    if not backtest_data_path.exists():
        print(f"  - 백테스트 데이터 파일 없음: {backtest_data_path}")
        return

    # 백테스트 데이터 로드
    backtest_df = pd.read_parquet(backtest_data_path)
    print(f"  - 백테스트 데이터 크기: {backtest_df.shape}")

    # DB 데이터 로드
    db = MarketDataDB()
    repo = MinuteCandleRepository(db)
    candles = repo.get_recent_prices(stock_code, count=1000)
    db_df = candles_to_dataframe(candles)
    print(f"  - DB 데이터 크기: {db_df.shape}")

    # 공통 시간대 비교
    print("\n  10:02 시점 데이터 비교:")

    # 백테스트 10:02
    bt_row = backtest_df[backtest_df['timestamp'].astype(str).str.contains('2026-01-27 10:02')]
    if not bt_row.empty:
        print(f"    백테스트: close={bt_row['close'].values[0]}")

    # DB 10:02
    db_row = db_df[db_df['timestamp'].astype(str).str.contains('2026-01-27 10:02')]
    if not db_row.empty:
        print(f"    DB:       close={db_row['close'].values[0]}")


if __name__ == "__main__":
    # 메인 실행
    check_signal_data("122630", "2026-01-27")
    compare_with_backtest("122630")
