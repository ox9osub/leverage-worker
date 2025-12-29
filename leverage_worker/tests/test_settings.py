"""
Settings 모듈 테스트
"""

import pytest
import tempfile
from pathlib import Path

import yaml


class TestSettingsLoad:
    """Settings 로드 테스트"""

    def setup_method(self):
        """테스트용 임시 설정 파일 생성"""
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = Path(self.temp_dir)

        # credentials 폴더 생성
        (self.config_path / "credentials").mkdir(parents=True, exist_ok=True)

        # kis_paper.yaml 생성
        paper_creds = {
            "app_key": "TEST_APP_KEY",
            "app_secret": "TEST_APP_SECRET",
            "account_number": "12345678",
            "account_product_code": "01",
            "hts_id": "TEST_HTS_ID",
        }
        with open(self.config_path / "credentials" / "kis_paper.yaml", "w") as f:
            yaml.dump(paper_creds, f)

        # trading_config.yaml 생성
        trading_config = {
            "schedule": {
                "trading_start": "09:00",
                "trading_end": "15:30",
                "default_interval_seconds": 5,
                "default_offset_seconds": 0,
            },
            "session": {
                "token_refresh_hours_before": 8,
            },
            "stocks": {
                "005930": {
                    "name": "삼성전자",
                    "interval_seconds": 3,
                    "strategies": [
                        {
                            "name": "example_strategy",
                            "params": {"stop_loss_pct": 0.03},
                        }
                    ],
                },
                "000660": {
                    "name": "SK하이닉스",
                    "strategies": [],
                },
            },
            "notification": {
                "slack_webhook_url": "https://hooks.slack.com/test",
            },
        }
        with open(self.config_path / "trading_config.yaml", "w") as f:
            yaml.dump(trading_config, f)

    def test_settings_load_success(self):
        """설정 로드 성공 테스트"""
        from leverage_worker.config.settings import Settings, TradingMode

        settings = Settings(mode=TradingMode.PAPER, config_path=self.config_path)

        assert settings.app_key == "TEST_APP_KEY"
        assert settings.account_number == "12345678"
        assert settings.schedule.trading_start == "09:00"
        assert len(settings.stocks) == 2

    def test_stock_config_access(self):
        """StockConfig 속성 접근 테스트"""
        from leverage_worker.config.settings import Settings, TradingMode

        settings = Settings(mode=TradingMode.PAPER, config_path=self.config_path)

        samsung = settings.stocks.get("005930")
        assert samsung is not None
        assert samsung.name == "삼성전자"
        assert samsung.interval_seconds == 3
        assert len(samsung.strategies) == 1
        assert samsung.strategies[0]["name"] == "example_strategy"

    def test_get_stock_interval(self):
        """종목별 interval 조회 테스트"""
        from leverage_worker.config.settings import Settings, TradingMode

        settings = Settings(mode=TradingMode.PAPER, config_path=self.config_path)

        # 개별 설정이 있는 종목
        assert settings.get_stock_interval("005930") == 3

        # 개별 설정이 없는 종목 (기본값 사용)
        assert settings.get_stock_interval("000660") == 5

    def test_server_url(self):
        """서버 URL 테스트"""
        from leverage_worker.config.settings import Settings, TradingMode

        paper_settings = Settings(mode=TradingMode.PAPER, config_path=self.config_path)
        assert "vts" in paper_settings.get_server_url()

        # live용 설정 파일 생성
        with open(self.config_path / "credentials" / "kis_prod.yaml", "w") as f:
            yaml.dump({
                "app_key": "LIVE_KEY",
                "app_secret": "LIVE_SECRET",
                "account_number": "87654321",
                "account_product_code": "01",
            }, f)

        live_settings = Settings(mode=TradingMode.LIVE, config_path=self.config_path)
        assert "vts" not in live_settings.get_server_url()


class TestTimeUtils:
    """time_utils 테스트"""

    def test_parse_time_string(self):
        """시간 문자열 파싱 테스트"""
        from leverage_worker.utils.time_utils import parse_time_string
        from datetime import time as dt_time

        result = parse_time_string("09:30")
        assert result == dt_time(9, 30)

        result = parse_time_string("15:00")
        assert result == dt_time(15, 0)

    def test_is_trading_hours(self):
        """매매 시간 체크 테스트"""
        from leverage_worker.utils.time_utils import is_trading_hours
        from datetime import datetime

        # 장중
        now = datetime(2024, 1, 15, 10, 0, 0)  # 10:00
        assert is_trading_hours(now, "09:00", "15:30") is True

        # 장 전
        now = datetime(2024, 1, 15, 8, 0, 0)  # 08:00
        assert is_trading_hours(now, "09:00", "15:30") is False

        # 장 후
        now = datetime(2024, 1, 15, 16, 0, 0)  # 16:00
        assert is_trading_hours(now, "09:00", "15:30") is False

    def test_should_execute_stock(self):
        """종목 실행 시점 체크 테스트"""
        from leverage_worker.utils.time_utils import should_execute_stock
        from datetime import datetime

        # interval=5, offset=0 → 00초, 05초, 10초, ...
        now = datetime(2024, 1, 15, 10, 0, 0)  # :00초
        assert should_execute_stock(now, 5, 0) is True

        now = datetime(2024, 1, 15, 10, 0, 5)  # :05초
        assert should_execute_stock(now, 5, 0) is True

        now = datetime(2024, 1, 15, 10, 0, 3)  # :03초
        assert should_execute_stock(now, 5, 0) is False

        # interval=5, offset=2 → 02초, 07초, 12초, ...
        now = datetime(2024, 1, 15, 10, 0, 2)  # :02초
        assert should_execute_stock(now, 5, 2) is True

        now = datetime(2024, 1, 15, 10, 0, 7)  # :07초
        assert should_execute_stock(now, 5, 2) is True

    def test_is_weekday(self):
        """평일 체크 테스트"""
        from leverage_worker.utils.time_utils import is_weekday
        from datetime import datetime

        # 월요일
        monday = datetime(2024, 1, 15, 10, 0, 0)
        assert is_weekday(monday) is True

        # 토요일
        saturday = datetime(2024, 1, 13, 10, 0, 0)
        assert is_weekday(saturday) is False


class TestStrategy:
    """전략 시스템 테스트"""

    def test_strategy_registry(self):
        """전략 레지스트리 테스트"""
        from leverage_worker.strategy import StrategyRegistry

        # 예시 전략이 등록되어 있는지 확인
        assert StrategyRegistry.has("example_strategy")
        assert StrategyRegistry.has("simple_momentum")

    def test_strategy_instance_creation(self):
        """전략 인스턴스 생성 테스트"""
        from leverage_worker.strategy import StrategyRegistry

        strategy = StrategyRegistry.get(
            "example_strategy",
            params={"stop_loss_pct": 0.05}
        )

        assert strategy is not None
        assert strategy.name == "example_strategy"
        assert strategy.get_param("stop_loss_pct") == 0.05

    def test_trading_signal_creation(self):
        """TradingSignal 생성 테스트"""
        from leverage_worker.strategy import TradingSignal, SignalType

        # HOLD 시그널
        hold = TradingSignal.hold("005930", "Waiting")
        assert hold.is_hold is True
        assert hold.stock_code == "005930"

        # BUY 시그널
        buy = TradingSignal.buy("005930", 10, "Golden cross")
        assert buy.is_buy is True
        assert buy.quantity == 10

        # SELL 시그널
        sell = TradingSignal.sell("005930", 5, "Stop loss")
        assert sell.is_sell is True
        assert sell.quantity == 5


class TestDatabase:
    """데이터베이스 테스트"""

    def test_market_data_db_init(self):
        """시세 DB 초기화 테스트"""
        import tempfile
        from pathlib import Path
        from leverage_worker.data.database import MarketDataDB

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "market_data.db"
            db = MarketDataDB(db_path)

            # 시세 테이블 생성 확인
            stats = db.get_table_stats()
            assert "stocks" in stats
            assert "daily_candles" in stats
            assert "minute_candles" in stats
            assert "price_history" in stats

            db.close()

    def test_trading_db_init(self):
        """매매 DB 초기화 테스트"""
        import tempfile
        from pathlib import Path
        from leverage_worker.data.database import TradingDB

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "trading.db"
            db = TradingDB(db_path)

            # 매매 테이블 생성 확인
            stats = db.get_table_stats()
            assert "orders" in stats
            assert "positions" in stats
            assert "daily_summary" in stats

            db.close()

    def test_price_repository(self):
        """가격 저장소 테스트"""
        import tempfile
        from pathlib import Path
        from leverage_worker.data.database import MarketDataDB
        from leverage_worker.data.minute_candle_repository import MinuteCandleRepository as PriceRepository

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "market_data.db"
            db = MarketDataDB(db_path)
            repo = PriceRepository(db)

            # 가격 upsert
            repo.upsert_price(
                stock_code="005930",
                open_price=70000,
                high_price=71000,
                low_price=69000,
                close_price=70500,
                volume=1000000,
                minute_key="20240115_1000",
            )

            # 조회
            price = repo.get_price("005930", "20240115_1000")
            assert price is not None
            assert price.close_price == 70500
            assert price.volume == 1000000

            # upsert (업데이트)
            repo.upsert_price(
                stock_code="005930",
                open_price=70000,
                high_price=72000,  # 고가 갱신
                low_price=69000,
                close_price=71000,  # 종가 갱신
                volume=1500000,
                minute_key="20240115_1000",
            )

            price = repo.get_price("005930", "20240115_1000")
            assert price.high_price == 72000
            assert price.close_price == 71000

            db.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
