"""
1년치 분봉/일봉 데이터 수집 스크립트

market_data.db에 122630, 233740 종목의 과거 데이터를 채웁니다.
- 분봉: 약 250 거래일 x 381개 = 95,250개/종목
- 일봉: 약 250 거래일/종목

실행:
    cd c:/Users/suble/Desktop/work/project/leverage-worker
    python leverage_worker/scripts/collect_historical_data.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "kis-trader"))

import time
import random
from datetime import datetime, timedelta
from typing import List, Callable, TypeVar, Any

from src.api.kis_api import KISApi

# leverage-worker imports
from leverage_worker.data.database import MarketDataDB
from leverage_worker.data.daily_candle_repository import DailyCandle, DailyCandleRepository
from leverage_worker.data.minute_candle_repository import MinuteCandle, MinuteCandleRepository

SYMBOLS = ["122630", "233740"]
YEAR_DAYS = 365  # 실제 거래일은 약 250일, 휴일 포함해서 넉넉히
EXPECTED_MINUTE_BARS = 381  # 09:00 ~ 15:30 = 390분, 실제 약 381개

# Rate limit 관련 설정
API_DELAY = 0.5  # 기본 API 호출 간 대기 시간 (초)
MAX_RETRIES = 5  # 최대 재시도 횟수
INITIAL_BACKOFF = 1.0  # 초기 backoff 시간 (초)
MAX_BACKOFF = 60.0  # 최대 backoff 시간 (초)

T = TypeVar("T")


def retry_with_backoff(
    func: Callable[..., T],
    *args: Any,
    max_retries: int = MAX_RETRIES,
    initial_backoff: float = INITIAL_BACKOFF,
    max_backoff: float = MAX_BACKOFF,
    **kwargs: Any,
) -> T:
    """
    Exponential backoff으로 함수 재시도

    Rate limit 에러 발생 시 점진적으로 대기 시간을 늘려가며 재시도합니다.
    """
    last_exception: Exception | None = None
    backoff = initial_backoff

    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            error_msg = str(e).lower()

            # Rate limit 관련 에러인지 확인
            is_rate_limit = any(
                keyword in error_msg
                for keyword in ["rate", "limit", "too many", "429", "exceeded"]
            )

            if attempt == max_retries:
                break

            # Rate limit이면 더 길게 대기
            if is_rate_limit:
                wait_time = min(backoff * (2**attempt), max_backoff)
                # Jitter 추가 (0.5 ~ 1.5 배)
                wait_time *= 0.5 + random.random()
                print(f"    Rate limit 감지, {wait_time:.1f}초 대기 후 재시도 ({attempt + 1}/{max_retries})...")
            else:
                wait_time = backoff
                print(f"    에러 발생, {wait_time:.1f}초 대기 후 재시도 ({attempt + 1}/{max_retries}): {e}")

            time.sleep(wait_time)

    raise last_exception  # type: ignore


def get_trading_dates(days: int = 365) -> List[str]:
    """과거 N일간의 날짜 리스트 (YYYYMMDD), 주말 제외, 오늘 포함"""
    dates = []
    today = datetime.now()
    for i in range(days):
        date = today - timedelta(days=i)  # 오늘부터 시작
        # 주말 제외 (월=0, 일=6)
        if date.weekday() < 5:
            dates.append(date.strftime("%Y%m%d"))
    return dates


def collect_minute_data(
    api: KISApi,
    repo: MinuteCandleRepository,
    symbol: str,
    date_str: str,
    incomplete_log: List[dict],
) -> int:
    """
    특정 날짜의 분봉 데이터 수집

    API는 한 번에 최대 120건을 반환하므로
    여러 시간대에서 호출하여 전체 분봉을 수집합니다.
    """
    time_points = ["093000", "113000", "133000", "153000", "160000"]

    # 오늘 날짜인 경우 현재 시간 이전의 time_points만 사용
    now = datetime.now()
    today_str = now.strftime("%Y%m%d")
    if date_str == today_str:
        current_time = now.strftime("%H%M%S")
        time_points = [tp for tp in time_points if tp <= current_time]
        if not time_points:
            return 0  # 장 시작 전이면 수집하지 않음

    all_candles = []

    for tp in time_points:
        try:
            ohlcv_list = retry_with_backoff(
                api.get_historical_minute_data, symbol, date_str, tp
            )
            for ohlcv in ohlcv_list:
                candle = MinuteCandle(
                    stock_code=symbol,
                    candle_datetime=ohlcv.timestamp.strftime("%Y-%m-%d %H:%M"),
                    trade_date=ohlcv.timestamp.strftime("%Y%m%d"),
                    open_price=ohlcv.open,
                    high_price=ohlcv.high,
                    low_price=ohlcv.low,
                    close_price=ohlcv.close,
                    volume=ohlcv.volume,
                )
                all_candles.append(candle)
            time.sleep(API_DELAY)
        except Exception as e:
            print(f"  [{tp}] Error (재시도 실패): {e}")

    # 중복 제거 (candle_datetime 기준)
    seen = set()
    unique = []
    for c in all_candles:
        if c.candle_datetime not in seen:
            seen.add(c.candle_datetime)
            unique.append(c)

    # 381개 미만이면 로그 기록
    if 0 < len(unique) < EXPECTED_MINUTE_BARS:
        incomplete_log.append(
            {
                "symbol": symbol,
                "date": date_str,
                "count": len(unique),
                "missing": EXPECTED_MINUTE_BARS - len(unique),
            }
        )

    if unique:
        repo.upsert_batch(unique)

    return len(unique)


def collect_daily_data(
    api: KISApi,
    repo: DailyCandleRepository,
    symbol: str,
    start_date: str,
    end_date: str,
) -> int:
    """일봉 데이터 수집 (최대 100건씩)"""
    try:
        ohlcv_list = retry_with_backoff(
            api.get_daily_data, symbol, start_date, end_date
        )
    except Exception as e:
        print(f"  일봉 조회 실패 ({start_date}~{end_date}, 재시도 실패): {e}")
        return 0

    candles = []
    for ohlcv in ohlcv_list:
        candle = DailyCandle(
            stock_code=symbol,
            trade_date=ohlcv.timestamp.strftime("%Y%m%d"),
            open_price=ohlcv.open,
            high_price=ohlcv.high,
            low_price=ohlcv.low,
            close_price=ohlcv.close,
            volume=ohlcv.volume,
        )
        candles.append(candle)

    if candles:
        repo.upsert_batch(candles)

    return len(candles)


def save_incomplete_log(
    incomplete_log: List[dict], filepath: str = "incomplete_dates.log"
) -> None:
    """불완전한 날짜 로그 저장"""
    if not incomplete_log:
        print("\n모든 거래일의 분봉이 완전히 수집되었습니다.")
        return

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"# 불완전한 분봉 데이터 목록 (기준: {EXPECTED_MINUTE_BARS}개)\n")
        f.write(f"# 생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        for entry in incomplete_log:
            f.write(
                f"{entry['symbol']}\t{entry['date']}\t"
                f"{entry['count']}개\t(누락: {entry['missing']}개)\n"
            )

    print(f"\n[경고] {len(incomplete_log)}개 거래일의 분봉이 불완전합니다.")
    print(f"       상세 내용: {filepath}")


def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("1년치 분봉/일봉 데이터 수집 시작")
    print("=" * 60)

    # API 인증
    api = KISApi()
    api.authenticate(svr="vps", product="01")

    # Repository 초기화
    db = MarketDataDB()  # 기본 경로: leverage_worker/data/market_data.db
    minute_repo = MinuteCandleRepository(db)
    daily_repo = DailyCandleRepository(db)

    dates = get_trading_dates(YEAR_DAYS)
    print(f"수집 대상: {len(dates)} 거래일")
    print(f"종목: {', '.join(SYMBOLS)}")

    # 불완전한 날짜 로그
    incomplete_log: List[dict] = []

    for symbol in SYMBOLS:
        print(f"\n{'='*60}")
        print(f"{symbol} 데이터 수집")
        print("=" * 60)

        # 1. 일봉 수집 (100건씩 분할)
        print("\n[일봉 수집]")
        daily_total = 0
        for i in range(0, len(dates), 100):
            chunk = dates[i : i + 100]
            end_date = chunk[0]  # 최신 날짜
            start_date = chunk[-1]  # 과거 날짜
            count = collect_daily_data(api, daily_repo, symbol, start_date, end_date)
            daily_total += count
            print(f"  {start_date} ~ {end_date}: {count}건")
            time.sleep(API_DELAY)
        print(f"  => 일봉 총 {daily_total}건 저장")

        # 2. 분봉 수집
        print("\n[분봉 수집]")
        minute_total = 0
        start_time = time.time()

        for i, date_str in enumerate(dates):
            count = collect_minute_data(
                api, minute_repo, symbol, date_str, incomplete_log
            )
            minute_total += count

            # 데이터가 있는 날만 출력
            if count > 0:
                status = "OK" if count >= EXPECTED_MINUTE_BARS else f"부족({count})"
                print(f"  [{i+1}/{len(dates)}] {date_str}: {count}건 [{status}]")

            # 10일마다 진행률 출력
            if (i + 1) % 10 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                remaining = (len(dates) - i - 1) / rate if rate > 0 else 0
                print(
                    f"  => 진행: {i+1}/{len(dates)} ({(i+1)/len(dates)*100:.1f}%) "
                    f"| 남은시간: {remaining/60:.1f}분"
                )

        print(f"  => 분봉 총 {minute_total}건 저장")

    # 불완전한 날짜 로그 저장
    save_incomplete_log(incomplete_log)

    # 최종 통계
    print("\n" + "=" * 60)
    print("수집 완료! 최종 통계:")
    print("=" * 60)

    for symbol in SYMBOLS:
        minute_range = minute_repo.get_date_range(symbol)
        minute_count = minute_repo.get_count(symbol)
        daily_range = daily_repo.get_date_range(symbol)
        daily_count = daily_repo.get_count(symbol)

        print(f"\n{symbol}:")
        print(f"  분봉: {minute_range} ({minute_count:,}건)")
        print(f"  일봉: {daily_range} ({daily_count:,}건)")


if __name__ == "__main__":
    main()
