"""
불완전한 분봉 데이터 재수집 스크립트

사용법:
    # 특정 종목, 날짜 지정
    python refill_incomplete_data.py 122630 20260122

    # 특정 종목, 여러 날짜
    python refill_incomplete_data.py 122630 20260122,20260121,20260120

    # 로그 파일에서 전체 재수집
    python refill_incomplete_data.py --from-log incomplete_dates.log
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "kis-trader"))

import time
from datetime import datetime
from typing import Dict, List, Tuple

from src.api.kis_api import KISApi

from leverage_worker.data.database import MarketDataDB
from leverage_worker.data.minute_candle_repository import MinuteCandle, MinuteCandleRepository

EXPECTED_MINUTE_BARS = 381
# 더 많은 시간대로 조회 (7개)
TIME_POINTS = ["090500", "100000", "110000", "120000", "140000", "150000", "160000"]
API_DELAY = 0.5  # Rate limit 대기 시간 (초)
MAX_RETRIES = 3


def parse_incomplete_log(filepath: str) -> List[Tuple[str, str, int]]:
    """incomplete_dates.log 파일 파싱"""
    entries = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 3:
                symbol = parts[0]
                date_str = parts[1]
                count = int(parts[2].replace("개", ""))
                entries.append((symbol, date_str, count))
    return entries


def collect_minute_data(
    api: KISApi,
    repo: MinuteCandleRepository,
    symbol: str,
    date_str: str,
) -> int:
    """
    분봉 데이터 수집 (재시도 로직 포함)

    Returns:
        수집된 분봉 개수
    """
    all_candles: Dict[str, MinuteCandle] = {}

    for tp in TIME_POINTS:
        for retry in range(MAX_RETRIES):
            try:
                ohlcv_list = api.get_historical_minute_data(symbol, date_str, tp)
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
                    all_candles[candle.candle_datetime] = candle
                time.sleep(API_DELAY)
                break
            except Exception as e:
                if retry < MAX_RETRIES - 1:
                    print(f"    [{tp}] 재시도 {retry + 1}/{MAX_RETRIES}...")
                    time.sleep(1.0)
                else:
                    print(f"    [{tp}] 실패: {e}")

    unique_candles = list(all_candles.values())

    if unique_candles:
        repo.upsert_batch(unique_candles)

    return len(unique_candles)


def refill_single(api: KISApi, repo: MinuteCandleRepository, symbol: str, date_str: str):
    """단일 날짜 재수집"""
    print(f"\n{symbol} / {date_str} 수집 중...")

    count = collect_minute_data(api, repo, symbol, date_str)

    status = "OK" if count >= EXPECTED_MINUTE_BARS else f"부족({count}/{EXPECTED_MINUTE_BARS})"
    print(f"  => {count}개 저장 [{status}]")

    return count


def refill_from_log(api: KISApi, repo: MinuteCandleRepository, log_file: str):
    """로그 파일에서 전체 재수집"""
    entries = parse_incomplete_log(log_file)
    print(f"재수집 대상: {len(entries)}개 날짜")

    still_incomplete: List[Tuple[str, str, int]] = []
    fixed_count = 0

    for i, (symbol, date_str, _) in enumerate(entries):
        print(f"[{i+1}/{len(entries)}] {symbol} / {date_str}", end=" ")

        count = collect_minute_data(api, repo, symbol, date_str)

        if count >= EXPECTED_MINUTE_BARS:
            print(f"=> {count}개 [OK]")
            fixed_count += 1
        else:
            print(f"=> {count}개 [부족]")
            still_incomplete.append((symbol, date_str, count))

    # 결과
    print("\n" + "=" * 50)
    print(f"완료! 수정됨: {fixed_count}개, 여전히 불완전: {len(still_incomplete)}개")

    if still_incomplete:
        output_file = "still_incomplete_dates.log"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"# 여전히 불완전한 분봉 (기준: {EXPECTED_MINUTE_BARS}개)\n")
            f.write(f"# {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for sym, dt, cnt in still_incomplete:
                f.write(f"{sym}\t{dt}\t{cnt}개\t(누락: {EXPECTED_MINUTE_BARS - cnt}개)\n")
        print(f"상세: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="불완전한 분봉 데이터 재수집")
    parser.add_argument("symbol", nargs="?", help="종목코드 (예: 122630)")
    parser.add_argument("dates", nargs="?", help="날짜 (예: 20260122 또는 20260122,20260121)")
    parser.add_argument("--from-log", dest="log_file", help="로그 파일에서 재수집")

    args = parser.parse_args()

    # API 인증
    api = KISApi()
    api.authenticate(svr="vps", product="01")

    # Repository 초기화
    db = MarketDataDB()
    repo = MinuteCandleRepository(db)

    print("=" * 50)
    print("불완전한 분봉 데이터 재수집")
    print("=" * 50)

    if args.log_file:
        # 로그 파일에서 재수집
        refill_from_log(api, repo, args.log_file)
    elif args.symbol and args.dates:
        # 특정 종목/날짜 재수집
        dates = args.dates.split(",")
        for date_str in dates:
            refill_single(api, repo, args.symbol, date_str.strip())
    else:
        parser.print_help()
        print("\n예시:")
        print("  python refill_incomplete_data.py 122630 20260122")
        print("  python refill_incomplete_data.py 122630 20260122,20260121")
        print("  python refill_incomplete_data.py --from-log incomplete_dates.log")


if __name__ == "__main__":
    main()
