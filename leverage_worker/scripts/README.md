# Scripts

데이터 수집 및 관리용 스크립트 모음

## 실행 방법

```bash
cd c:\Users\suble\Desktop\work\project\leverage-worker
uv run python leverage_worker/scripts/<스크립트명>.py [옵션]
```

---

## collect_historical_data.py

1년치 분봉/일봉 데이터를 수집하여 `market_data.db`에 저장합니다.

### 사용법

```bash
python leverage_worker/scripts/collect_historical_data.py
```

### 수집 대상

| 항목 | 값 |
|------|-----|
| 종목 | 122630 (KODEX 레버리지), 233740 (KODEX 코스닥150 레버리지) |
| 기간 | 과거 1년 (약 250 거래일) |
| 데이터 | 분봉 (1분), 일봉 |

### 출력 파일

- `market_data.db` - 분봉/일봉 데이터 저장
- `incomplete_dates.log` - 381개 미만 분봉 수집된 날짜 목록

### 예상 소요시간

약 15-20분 (Rate Limit: 초당 4건)

---

## refill_incomplete_data.py

불완전한 분봉 데이터를 재수집합니다.

### 사용법

```bash
# 특정 종목/날짜 재수집
python leverage_worker/scripts/refill_incomplete_data.py 122630 20260122

# 여러 날짜 재수집 (콤마로 구분)
python leverage_worker/scripts/refill_incomplete_data.py 122630 20260122,20260121,20260120

# 로그 파일에서 전체 재수집
python leverage_worker/scripts/refill_incomplete_data.py --from-log incomplete_dates.log
```

### 옵션

| 인자 | 설명 | 예시 |
|------|------|------|
| `symbol` | 종목코드 | `122630` |
| `dates` | 날짜 (YYYYMMDD, 콤마 구분 가능) | `20260122,20260121` |
| `--from-log` | 로그 파일에서 재수집 | `--from-log incomplete_dates.log` |

### 출력 파일

- `still_incomplete_dates.log` - 재수집 후에도 불완전한 날짜 목록 (없으면 생성 안됨)

---

## 데이터 검증

수집된 데이터 확인:

```python
from leverage_worker.data.database import MarketDataDB
from leverage_worker.data.minute_candle_repository import MinuteCandleRepository
from leverage_worker.data.daily_candle_repository import DailyCandleRepository

db = MarketDataDB()
minute_repo = MinuteCandleRepository(db)
daily_repo = DailyCandleRepository(db)

for symbol in ['122630', '233740']:
    print(f"\n=== {symbol} ===")

    # 분봉
    minute_range = minute_repo.get_date_range(symbol)
    minute_count = minute_repo.get_count(symbol)
    print(f"분봉: {minute_range} ({minute_count:,}건)")

    # 일봉
    daily_range = daily_repo.get_date_range(symbol)
    daily_count = daily_repo.get_count(symbol)
    print(f"일봉: {daily_range} ({daily_count:,}건)")
```

---

## API 제약사항

| 항목 | 모의투자 (VPS) | 실전투자 |
|------|---------------|---------|
| Rate Limit | 초당 4건 | 초당 18건 |
| 분봉 조회 | 최대 120건/호출 | 최대 120건/호출 |
| 일봉 조회 | 최대 100건/호출 | 최대 100건/호출 |
| 조회 가능 기간 | 과거 1년 | 과거 1년 |
