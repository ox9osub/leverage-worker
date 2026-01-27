"""
캔들 데이터 무결성 체크 스크립트

market_data.db의 일봉/분봉 데이터 누락 여부를 검증합니다.
1. 종목 리스트업
2. 각 종목의 첫날/마지막날 확인
3. 빠진 일봉 확인 (휴일 제외)
4. 분봉 개수가 381개가 아닌 날 확인
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Set, Tuple

# 프로젝트 루트를 path에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from leverage_worker.data.database import MarketDataDB
from leverage_worker.data.daily_candle_repository import DailyCandleRepository
from leverage_worker.data.minute_candle_repository import MinuteCandleRepository


# 정규 장 분봉 개수 (09:00 ~ 15:30 = 6시간 30분 = 390분, 하지만 09:00 봉부터 15:30 봉까지)
# 09:00, 09:01, ..., 15:30 = 381개
EXPECTED_MINUTE_CANDLES_PER_DAY = 381


def get_weekdays_between(start_date: str, end_date: str) -> Set[str]:
    """
    시작일과 종료일 사이의 평일(월~금) 목록 반환

    Args:
        start_date: 시작일 (YYYYMMDD)
        end_date: 종료일 (YYYYMMDD)

    Returns:
        평일 날짜 집합 (YYYYMMDD)
    """
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    weekdays = set()
    current = start

    while current <= end:
        # 0=월, 1=화, ..., 4=금, 5=토, 6=일
        if current.weekday() < 5:
            weekdays.add(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return weekdays


def check_daily_candle_integrity(
    daily_repo: DailyCandleRepository,
    stock_code: str,
    start_date: str,
    end_date: str,
    all_trading_days: Set[str],
) -> Tuple[List[str], int]:
    """
    일봉 데이터 무결성 체크

    Args:
        daily_repo: 일봉 저장소
        stock_code: 종목코드
        start_date: 시작일
        end_date: 종료일
        all_trading_days: 전체 거래일 집합 (공휴일 제외된)

    Returns:
        (빠진 날짜 리스트, 존재하는 데이터 개수)
    """
    available_dates = set(daily_repo.get_available_dates(stock_code))

    # 해당 종목의 기간 내 거래일만 필터링
    expected_dates = {d for d in all_trading_days if start_date <= d <= end_date}

    missing_dates = sorted(expected_dates - available_dates)

    return missing_dates, len(available_dates)


def check_minute_candle_integrity(
    minute_repo: MinuteCandleRepository,
    stock_code: str,
    start_date: str,
    end_date: str,
) -> List[Tuple[str, int]]:
    """
    분봉 데이터 무결성 체크

    Args:
        minute_repo: 분봉 저장소
        stock_code: 종목코드
        start_date: 시작일
        end_date: 종료일

    Returns:
        (날짜, 분봉개수) 튜플 리스트 (381개가 아닌 날만)
    """
    db = minute_repo._db

    # 날짜별 분봉 개수 조회
    query = """
        SELECT trade_date, COUNT(*) as cnt
        FROM minute_candles
        WHERE stock_code = ? AND trade_date BETWEEN ? AND ?
        GROUP BY trade_date
        ORDER BY trade_date
    """
    rows = db.fetch_all(query, (stock_code, start_date, end_date))

    incomplete_days = []
    for row in rows:
        if row["cnt"] != EXPECTED_MINUTE_CANDLES_PER_DAY:
            incomplete_days.append((row["trade_date"], row["cnt"]))

    return incomplete_days


def get_common_trading_days(daily_repo: DailyCandleRepository) -> Set[str]:
    """
    모든 종목에서 공통적으로 데이터가 있는 거래일 추출
    (공휴일은 어떤 종목에도 데이터가 없음)

    Returns:
        거래일 집합
    """
    stock_codes = daily_repo.get_stock_codes()
    if not stock_codes:
        return set()

    # 첫 번째 종목의 거래일로 시작
    trading_days = set(daily_repo.get_available_dates(stock_codes[0]))

    # 모든 종목의 거래일 합집합 (최대한 많은 거래일 수집)
    for stock_code in stock_codes[1:]:
        dates = set(daily_repo.get_available_dates(stock_code))
        trading_days = trading_days.union(dates)

    return trading_days


def main():
    print("=" * 70)
    print("캔들 데이터 무결성 체크")
    print("=" * 70)
    print()

    # DB 연결
    db = MarketDataDB()
    daily_repo = DailyCandleRepository(db)
    minute_repo = MinuteCandleRepository(db)

    # 1. 종목 리스트업
    print("[1] 종목 리스트 확인")
    print("-" * 50)

    daily_stocks = set(daily_repo.get_stock_codes())
    minute_stocks = set(minute_repo.get_stored_stock_codes())
    all_stocks = daily_stocks.union(minute_stocks)

    print(f"  일봉 데이터 종목 수: {len(daily_stocks)}")
    print(f"  분봉 데이터 종목 수: {len(minute_stocks)}")
    print(f"  전체 종목: {sorted(all_stocks)}")
    print()

    # 일봉만 있고 분봉 없는 종목
    daily_only = daily_stocks - minute_stocks
    if daily_only:
        print(f"  [WARN] 일봉만 있는 종목: {sorted(daily_only)}")

    # 분봉만 있고 일봉 없는 종목
    minute_only = minute_stocks - daily_stocks
    if minute_only:
        print(f"  [WARN] 분봉만 있는 종목: {sorted(minute_only)}")
    print()

    # 실제 거래일 추정 (모든 종목의 일봉 데이터에서 거래일 추출)
    trading_days = get_common_trading_days(daily_repo)
    print(f"  추정 거래일 수: {len(trading_days)}")
    print()

    # 2. 각 종목별 데이터 기간 및 무결성 체크
    print("[2] 종목별 데이터 기간 확인")
    print("-" * 50)

    for stock_code in sorted(all_stocks):
        print(f"\n  [STOCK] {stock_code}")

        # 일봉 기간
        daily_range = daily_repo.get_date_range(stock_code)
        if daily_range:
            start_date, end_date = daily_range
            print(f"     일봉: {start_date} ~ {end_date}")
        else:
            start_date, end_date = None, None
            print(f"     일봉: 데이터 없음")

        # 분봉 기간
        minute_range = minute_repo.get_date_range(stock_code)
        if minute_range:
            m_start, m_end = minute_range
            print(f"     분봉: {m_start} ~ {m_end}")
            if start_date is None:
                start_date, end_date = m_start, m_end
        else:
            print(f"     분봉: 데이터 없음")

        if start_date is None:
            continue

        # 3. 일봉 누락 체크 (휴일 제외)
        if daily_range:
            # 평일 중에서 실제 거래일만 필터링
            weekdays = get_weekdays_between(start_date, end_date)
            actual_trading_days = weekdays.intersection(trading_days)

            missing_daily, daily_count = check_daily_candle_integrity(
                daily_repo, stock_code, start_date, end_date, actual_trading_days
            )

            if missing_daily:
                print(f"     [WARN] 일봉 누락 ({len(missing_daily)}일):")
                # 최대 10개까지만 표시
                display_dates = missing_daily[:10]
                for date in display_dates:
                    formatted = f"{date[:4]}-{date[4:6]}-{date[6:]}"
                    print(f"        - {formatted}")
                if len(missing_daily) > 10:
                    print(f"        ... 외 {len(missing_daily) - 10}일")
            else:
                print(f"     [OK] 일봉: 누락 없음 ({daily_count}개)")

        # 4. 분봉 개수 체크 (381개 아닌 날)
        if minute_range:
            m_start, m_end = minute_range
            incomplete_minute = check_minute_candle_integrity(
                minute_repo, stock_code, m_start, m_end
            )

            if incomplete_minute:
                print(f"     [WARN] 분봉 개수 이상 ({len(incomplete_minute)}일):")
                # 최대 10개까지만 표시
                display_items = incomplete_minute[:10]
                for date, cnt in display_items:
                    formatted = f"{date[:4]}-{date[4:6]}-{date[6:]}"
                    diff = cnt - EXPECTED_MINUTE_CANDLES_PER_DAY
                    sign = "+" if diff > 0 else ""
                    print(f"        - {formatted}: {cnt}개 ({sign}{diff})")
                if len(incomplete_minute) > 10:
                    print(f"        ... 외 {len(incomplete_minute) - 10}일")
            else:
                # 분봉 있는 날 수 확인
                query = """
                    SELECT COUNT(DISTINCT trade_date) as day_cnt
                    FROM minute_candles
                    WHERE stock_code = ?
                """
                row = minute_repo._db.fetch_one(query, (stock_code,))
                day_count = row["day_cnt"] if row else 0
                print(f"     [OK] 분봉: 모든 거래일 {EXPECTED_MINUTE_CANDLES_PER_DAY}개 ({day_count}일)")

    print()
    print("=" * 70)
    print("체크 완료")
    print("=" * 70)

    # DB 연결 종료
    db.close()


if __name__ == "__main__":
    main()
