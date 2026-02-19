# Leverage Worker 아키텍처

> 주식 자동매매 시스템의 컴포넌트 단위 아키텍처 문서

## 1. 시스템 전체 구조

```mermaid
flowchart TB
    subgraph Entry["진입점"]
        main["main.py<br/>parse_args()<br/>main()"]
    end

    subgraph Core["Core 컴포넌트"]
        direction TB
        HC["HealthChecker<br/>• register_check()<br/>• check_now()<br/>• start_background_check()"]
        ES["EmergencyStop<br/>• trigger()<br/>• is_stopped()"]
        RM["RecoveryManager<br/>• SessionState<br/>• recover_session()"]
        DL["DailyLiquidationManager<br/>• LiquidationResult<br/>• liquidate_all()"]
    end

    subgraph Trading["Trading 컴포넌트"]
        direction TB
        KB["KISBroker<br/>─────────────<br/>• get_current_price()<br/>• get_balance()<br/>• place_market_order()<br/>• place_limit_order()<br/>• cancel_order()<br/>• get_minute_candles()"]
        OM["OrderManager<br/>─────────────<br/>• place_buy_order()<br/>• place_buy_order_with_chase()<br/>• place_sell_order()<br/>• place_sell_order_with_fallback()<br/>• check_fills()<br/>• cancel_all_pending()"]
        PM["PositionManager<br/>─────────────<br/>• sync_with_broker()<br/>• add_position()<br/>• remove_position()<br/>• get_strategy_positions()"]
    end

    subgraph Strategy["Strategy 컴포넌트"]
        direction TB
        SR["StrategyRegistry<br/>• register()<br/>• get()<br/>• list_strategies()"]

        subgraph Strategies["전략들"]
            MB4["main_beam_4"]
            BB["bollinger_band"]
            BH["breakout_high"]
            HM["hybrid_momentum"]
            KD["kosdaq_donchian"]
            KB2["kosdaq_bb_conservative"]
            FO["fee_optimized"]
            FL["fibonacci_lucky"]
        end
    end

    subgraph Data["Data 컴포넌트"]
        direction TB
        DB["Database<br/>• MarketDataDB<br/>• TradingDB"]

        subgraph Repositories["레포지토리"]
            StockR["StockRepository<br/>• Stock 모델"]
            PriceR["PriceRepository<br/>• OHLCV 모델"]
            MCR["MinuteCandleRepository<br/>• MinuteCandle 모델"]
            DCR["DailyCandleRepository<br/>• DailyCandle 모델"]
        end
    end

    subgraph WebSocket["WebSocket 컴포넌트"]
        direction TB
        TH["TickHandler<br/>• TickData 모델<br/>• parse()"]
        EM["ExitMonitor<br/>• ExitMonitorConfig<br/>• check_exit_condition()"]
    end

    subgraph ML["ML 컴포넌트"]
        direction TB
        SG["SignalGenerator<br/>• VolatilityDirectionSignalGenerator<br/>• generate_signal()"]
        FE["TradingFeatureEngineer<br/>• FeatureConfig<br/>• compute_features()"]

        subgraph Models["모델"]
            BM["BaseModel"]
            GB["GradientBoosting"]
        end
    end

    subgraph Notification["Notification 컴포넌트"]
        DRG["DailyReportGenerator<br/>• TradeRecord<br/>• PositionRecord<br/>• DailyReport<br/>• generate()"]
    end

    subgraph Utils["Utils 컴포넌트"]
        direction TB
        LOG["Logger / StructuredLogger"]
        AL["AuditLogger"]
        TU["TimeUtils"]
        MU["MathUtils"]
    end

    subgraph Config["Config"]
        YAML["trading_config.yaml"]
        CRED["credentials/<br/>• kis_prod__.yaml<br/>• kis_paper__.yaml"]
    end

    subgraph External["외부 시스템"]
        KIS["KIS Open API<br/>(한국투자증권)"]
        SLACK["Slack API"]
        SQLite["SQLite DB"]
    end

    %% 연결 관계
    main --> Core
    main --> Trading
    main --> Strategy

    Core --> Trading
    HC --> ES

    Trading --> Data
    KB --> KIS
    OM --> KB
    OM --> PM
    PM --> KB

    Strategy --> Data
    Strategy --> ML
    SR --> Strategies

    Data --> SQLite
    DB --> Repositories

    WebSocket --> Trading
    TH --> OM
    EM --> PM

    ML --> Data
    SG --> FE
    FE --> Models

    Notification --> SLACK
    DRG --> Data

    Config --> main
    Utils --> Core
    Utils --> Trading
    Utils --> Strategy
```

## 2. 데이터 흐름도

```mermaid
flowchart LR
    subgraph Input["입력"]
        KIS_REST["KIS REST API"]
        KIS_WS["KIS WebSocket"]
        CONFIG["Config YAML"]
    end

    subgraph Processing["처리"]
        direction TB
        BROKER["KISBroker"]
        TICK["TickHandler"]
        STRATEGY["Strategy<br/>Execution"]
        ML_SIGNAL["ML Signal<br/>Generator"]
        ORDER["OrderManager"]
        POSITION["PositionManager"]
    end

    subgraph Storage["저장"]
        direction TB
        MARKET_DB["MarketDataDB<br/>(분봉, 일봉, 가격)"]
        TRADING_DB["TradingDB<br/>(주문, 포지션)"]
    end

    subgraph Output["출력"]
        SLACK_OUT["Slack 알림"]
        LOGS["로그 파일"]
        ORDER_OUT["주문 실행"]
    end

    KIS_REST --> BROKER
    KIS_WS --> TICK
    CONFIG --> STRATEGY

    BROKER --> MARKET_DB
    BROKER --> ORDER
    TICK --> STRATEGY

    ML_SIGNAL --> STRATEGY
    MARKET_DB --> ML_SIGNAL

    STRATEGY --> ORDER
    ORDER --> POSITION
    ORDER --> ORDER_OUT
    POSITION --> TRADING_DB

    STRATEGY --> SLACK_OUT
    ORDER --> LOGS
```

## 3. 클래스 관계도

```mermaid
classDiagram
    direction TB

    %% Core Classes
    class HealthChecker {
        -_checks: Dict
        -_last_health: SystemHealth
        +register_check()
        +check_now()
        +start_background_check()
        +stop_background_check()
    }

    class EmergencyStop {
        -_stopped: bool
        +trigger()
        +is_stopped()
    }

    class RecoveryManager {
        -_session_state: SessionState
        +recover_session()
        +save_state()
    }

    class DailyLiquidationManager {
        +liquidate_all()
        +get_liquidation_result()
    }

    %% Trading Classes
    class KISBroker {
        -_session: Session
        -_account_no: str
        +get_current_price()
        +get_balance()
        +place_market_order()
        +place_limit_order()
        +cancel_order()
        +get_minute_candles()
        +get_daily_candles()
    }

    class OrderManager {
        -_broker: KISBroker
        -_position_manager: PositionManager
        -_active_orders: Dict
        +place_buy_order()
        +place_buy_order_with_chase()
        +place_sell_order()
        +place_sell_order_with_fallback()
        +check_fills()
        +cancel_all_pending()
    }

    class PositionManager {
        -_broker: KISBroker
        -_positions: Dict
        +sync_with_broker()
        +add_position()
        +remove_position()
        +get_position()
        +get_strategy_positions()
    }

    %% Strategy Classes
    class StrategyRegistry {
        -_strategies: Dict
        +register()
        +get()
        +list_strategies()
        +has()
    }

    class BaseStrategy {
        <<abstract>>
        +execute()
        +should_buy()
        +should_sell()
    }

    %% Data Classes
    class Database {
        -_db_path: str
        +get_cursor()
        +execute()
        +fetch_one()
        +fetch_all()
    }

    class MinuteCandleRepository {
        -_db: Database
        +save()
        +get_candles()
        +get_latest()
    }

    class PriceRepository {
        -_db: Database
        +save_price()
        +get_prices()
    }

    %% WebSocket Classes
    class TickHandler {
        +COL_PRICE: int
        +COL_VOLUME: int
        +parse()
    }

    class ExitMonitor {
        -_config: ExitMonitorConfig
        +check_exit_condition()
    }

    %% ML Classes
    class SignalGenerator {
        -_model: BaseModel
        +generate_signal()
    }

    class TradingFeatureEngineer {
        -_config: FeatureConfig
        +compute_features()
    }

    %% Relationships
    OrderManager --> KISBroker : uses
    OrderManager --> PositionManager : manages
    PositionManager --> KISBroker : syncs with

    StrategyRegistry --> BaseStrategy : registers

    DailyLiquidationManager --> OrderManager : uses
    DailyLiquidationManager --> PositionManager : uses

    ExitMonitor --> PositionManager : monitors

    SignalGenerator --> TradingFeatureEngineer : uses

    MinuteCandleRepository --> Database : uses
    PriceRepository --> Database : uses
```

## 4. 시퀀스 다이어그램 - 매수 플로우

```mermaid
sequenceDiagram
    participant Main as main.py
    participant Strategy as Strategy
    participant ML as SignalGenerator
    participant OM as OrderManager
    participant Broker as KISBroker
    participant PM as PositionManager
    participant DB as Database
    participant KIS as KIS API

    Main->>Strategy: execute()
    Strategy->>ML: generate_signal()
    ML-->>Strategy: BUY signal (confidence)

    alt Signal is BUY
        Strategy->>OM: place_buy_order()
        OM->>Broker: get_buyable_quantity()
        Broker->>KIS: API 호출
        KIS-->>Broker: 매수가능수량
        Broker-->>OM: quantity

        OM->>Broker: place_limit_order()
        Broker->>KIS: 지정가 주문
        KIS-->>Broker: 주문번호
        Broker-->>OM: OrderResult

        OM->>DB: save_order()

        loop 체결 확인
            OM->>Broker: get_order_status()
            Broker->>KIS: 체결 조회
            KIS-->>Broker: 체결 상태
        end

        OM->>PM: add_position()
        PM->>DB: save_position()
        PM-->>OM: success
        OM-->>Strategy: OrderResult
    end

    Strategy-->>Main: execution result
```

## 5. 실행 모드별 흐름

```mermaid
flowchart TB
    subgraph Modes["실행 모드"]
        direction LR
        SCALPING["Scalping Mode"]
        STANDARD["Standard Mode"]
        WEBSOCKET["WebSocket Mode"]
    end

    subgraph ScalpingFlow["Scalping 플로우"]
        S1["ML 신호 생성"]
        S2["지정가 매수"]
        S3["WebSocket 실시간 모니터링"]
        S4["TP/SL 도달 시 즉시 매도"]
        S1 --> S2 --> S3 --> S4
    end

    subgraph StandardFlow["Standard 플로우"]
        T1["주기적 전략 실행<br/>(interval_seconds)"]
        T2["기술적 지표 계산"]
        T3["매수/매도 결정"]
        T4["주문 실행"]
        T1 --> T2 --> T3 --> T4
    end

    subgraph WebSocketFlow["WebSocket 플로우"]
        W1["실시간 틱 수신"]
        W2["틱 데이터 파싱"]
        W3["실시간 전략 판단"]
        W4["즉시 주문"]
        W1 --> W2 --> W3 --> W4
    end

    SCALPING --> ScalpingFlow
    STANDARD --> StandardFlow
    WEBSOCKET --> WebSocketFlow
```

## 6. 폴더 구조

```
leverage_worker/
├── main.py                      # 진입점
├── config/
│   ├── trading_config.yaml      # 매매 설정
│   └── credentials/             # KIS 인증 정보
│       ├── kis_prod__.yaml
│       └── kis_paper__.yaml
├── core/
│   ├── health_checker.py        # 상태 모니터링
│   ├── emergency.py             # 긴급 정지
│   ├── recovery_manager.py      # 세션 복구
│   └── daily_liquidation.py     # 당일 청산
├── trading/
│   ├── broker.py                # KIS API 통신 (1,183 lines)
│   ├── order_manager.py         # 주문 관리 (1,503 lines)
│   └── position_manager.py      # 포지션 관리 (606 lines)
├── strategy/
│   ├── registry.py              # 전략 레지스트리
│   └── strategies/
│       ├── main_beam_4.py       # ML 기반 (활성)
│       ├── bollinger_band.py
│       ├── breakout_high.py
│       ├── hybrid_momentum.py
│       └── ...
├── data/
│   ├── database.py              # SQLite 연결
│   ├── minute_candle_repository.py
│   ├── daily_candle_repository.py
│   ├── price_repository.py
│   └── stock_repository.py
├── websocket/
│   ├── tick_handler.py          # 실시간 틱
│   └── exit_monitor.py          # TP/SL 모니터
├── ml/
│   ├── signal_generator.py      # 신호 생성
│   ├── features.py              # 피처 엔지니어링
│   └── models/
│       ├── base_model.py
│       └── gradient_boosting.py
├── notification/
│   └── daily_report.py          # Slack 리포트
└── utils/
    ├── logger.py
    ├── structured_logger.py
    ├── audit_logger.py
    ├── time_utils.py
    └── math_utils.py
```

## 7. 컴포넌트 상세 설명

### Core 컴포넌트
| 클래스 | 파일 | 주요 메서드 | 역할 |
|--------|------|-------------|------|
| `HealthChecker` | [health_checker.py](../leverage_worker/core/health_checker.py) | `register_check()`, `check_now()`, `start_background_check()` | API, DB, Scheduler 상태 모니터링 |
| `EmergencyStop` | [emergency.py](../leverage_worker/core/emergency.py) | `trigger()`, `is_stopped()` | 긴급 정지 핸들러 |
| `RecoveryManager` | [recovery_manager.py](../leverage_worker/core/recovery_manager.py) | `recover_session()`, `save_state()` | 세션 복구/재연결 |
| `DailyLiquidationManager` | [daily_liquidation.py](../leverage_worker/core/daily_liquidation.py) | `liquidate_all()` | 당일 청산 처리 |

### Trading 컴포넌트
| 클래스 | 파일 | 주요 메서드 | 역할 |
|--------|------|-------------|------|
| `KISBroker` | [broker.py](../leverage_worker/trading/broker.py) | `get_current_price()`, `place_limit_order()`, `cancel_order()` | KIS API 통신 추상화 (1,000+ lines) |
| `OrderManager` | [order_manager.py](../leverage_worker/trading/order_manager.py) | `place_buy_order_with_chase()`, `place_sell_order_with_fallback()` | 주문 생명주기 관리 (1,500+ lines) |
| `PositionManager` | [position_manager.py](../leverage_worker/trading/position_manager.py) | `sync_with_broker()`, `add_position()`, `get_strategy_positions()` | 포지션 상태 관리 |

### Strategy 컴포넌트
| 클래스 | 파일 | 역할 |
|--------|------|------|
| `StrategyRegistry` | [registry.py](../leverage_worker/strategy/registry.py) | 전략 등록/조회 (싱글톤 패턴) |
| `main_beam_4` | strategies/ | ML 기반 4분 보유 전략 (승률 86.78%) |
| `hybrid_momentum` | strategies/ | 하이브리드 모멘텀 전략 |
| `bollinger_band` | strategies/ | 볼린저밴드 전략 |
| `breakout_high` | strategies/ | 고점 돌파 전략 |
| `kosdaq_*` | strategies/ | 코스닥 전용 전략들 |

### Data 컴포넌트
| 클래스 | 파일 | 데이터 | 역할 |
|--------|------|--------|------|
| `Database` | [database.py](../leverage_worker/data/database.py) | SQLite | 기본 DB 연결/쿼리 |
| `MarketDataDB` | [database.py](../leverage_worker/data/database.py) | 시장 데이터 | 가격/캔들 저장 |
| `TradingDB` | [database.py](../leverage_worker/data/database.py) | 거래 데이터 | 주문/포지션 저장 |
| `MinuteCandleRepository` | [minute_candle_repository.py](../leverage_worker/data/minute_candle_repository.py) | 분봉 | OHLCV 분봉 데이터 |
| `DailyCandleRepository` | [daily_candle_repository.py](../leverage_worker/data/daily_candle_repository.py) | 일봉 | OHLCV 일봉 데이터 |
| `PriceRepository` | [price_repository.py](../leverage_worker/data/price_repository.py) | 현재가 | 실시간 가격 |

### WebSocket 컴포넌트
| 클래스 | 파일 | 주요 메서드 | 역할 |
|--------|------|-------------|------|
| `TickHandler` | [tick_handler.py](../leverage_worker/websocket/tick_handler.py) | `parse()` | 실시간 틱 데이터 파싱 |
| `ExitMonitor` | [exit_monitor.py](../leverage_worker/websocket/exit_monitor.py) | `check_exit_condition()` | 실시간 TP/SL 모니터링 |

### ML 컴포넌트
| 클래스 | 파일 | 주요 메서드 | 역할 |
|--------|------|-------------|------|
| `SignalGenerator` | [signal_generator.py](../leverage_worker/ml/signal_generator.py) | `generate_signal()` | 변동성 방향 신호 생성 |
| `TradingFeatureEngineer` | [features.py](../leverage_worker/ml/features.py) | `compute_features()` | 피처 엔지니어링 |
| `GradientBoosting` | models/ | `predict()` | 그래디언트 부스팅 모델 |

### Notification 컴포넌트
| 클래스 | 파일 | 주요 메서드 | 역할 |
|--------|------|-------------|------|
| `DailyReportGenerator` | [daily_report.py](../leverage_worker/notification/daily_report.py) | `generate()` | Slack 일일 리포트 |

### Utils 컴포넌트
| 클래스 | 파일 | 역할 |
|--------|------|------|
| `Logger` | [logger.py](../leverage_worker/utils/logger.py) | 기본 로깅 |
| `StructuredLogger` | [structured_logger.py](../leverage_worker/utils/structured_logger.py) | 구조화된 로깅 |
| `AuditLogger` | [audit_logger.py](../leverage_worker/utils/audit_logger.py) | 감사 로깅 (주문/포지션) |
| `TimeUtils` | [time_utils.py](../leverage_worker/utils/time_utils.py) | 시간 관련 유틸리티 |
| `MathUtils` | [math_utils.py](../leverage_worker/utils/math_utils.py) | 수학 계산 유틸리티 |

## 8. 실행 모드 상세

| 모드 | 전략 예시 | 특징 |
|------|----------|------|
| **Scalping** | `main_beam_4`, `ml_momentum` | ML 신호 기반, WebSocket 실시간 TP/SL, 1-4분 보유 |
| **Standard** | `bollinger_band`, `breakout_high` | 주기적 실행 (60초), 기술적 지표 기반 |
| **WebSocket** | `dip_buy` | 틱 단위 실시간 전략, 급락 매수 |
