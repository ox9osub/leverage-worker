"""
메인 진입점

자동매매 프로그램 실행
- CLI 인터페이스
- 모드 선택 (paper/live)
"""

import argparse
import sys
from pathlib import Path

# 모듈 경로 설정
sys.path.insert(0, str(Path(__file__).parent.parent))

from leverage_worker.config.settings import Settings, TradingMode
from leverage_worker.core.trading_engine import TradingEngine
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """CLI 인자 파싱"""
    parser = argparse.ArgumentParser(
        description="주식 자동매매 프로그램",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python main.py --mode paper    모의투자 모드로 실행
  python main.py --mode live     실전투자 모드로 실행
        """,
    )

    parser.add_argument(
        "--mode",
        type=str,
        choices=["paper", "live"],
        default="paper",
        help="실행 모드 (paper: 모의투자, live: 실전투자)",
    )

    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="설정 파일 경로 (기본: config/trading_config.yaml)",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="디버그 모드 활성화",
    )

    return parser.parse_args()


def main() -> int:
    """메인 함수"""
    args = parse_args()

    # 모드 설정
    mode = TradingMode.PAPER if args.mode == "paper" else TradingMode.LIVE

    logger.info("=" * 60)
    logger.info(f"Starting Auto Trading System")
    logger.info(f"Mode: {mode.value}")
    logger.info("=" * 60)

    try:
        # 설정 로드
        config_path = Path(args.config) if args.config else None
        settings = Settings(mode=mode, config_path=config_path)

        logger.info(f"Account: {settings.account_number}")
        logger.info(f"Managed stocks: {len(settings.stocks)}")

        for stock_code, stock_config in settings.stocks.items():
            name = stock_config.get("name", stock_code)
            strategies = stock_config.get("strategies", [])
            strategy_names = [s.get("name", "?") for s in strategies]
            logger.info(f"  - {name} ({stock_code}): {strategy_names or 'No strategy'}")

        # 엔진 시작
        engine = TradingEngine(settings)
        engine.start()

        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
