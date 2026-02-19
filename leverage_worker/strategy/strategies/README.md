# Trading Strategies

이 폴더에는 KODEX 레버리지 ETF 자동매매를 위한 다양한 전략이 구현되어 있습니다.

## 전략 개요

- **총 전략 수**: 17개
- **대상 종목**: KODEX 레버리지(122630), 코스닥150레버리지(233740)
- **데이터 타입**: ML 기반 분봉, 일봉 기반, 분봉 기반

---

## 전략 분류

### 1. ML 기반 스캘핑 (Main Beam 시리즈)

앙상블 ML 모델(LightGBM+XGBoost+CatBoost+RF)을 사용한 지정가 스캘핑 전략입니다.

| 전략명 | 파일 | 타임아웃 | 특징 | 백테스트 성과 |
|--------|------|----------|------|---------------|
| `main_beam_1` | [main_beam_1.py](main_beam_1.py) | 1분 | 앙상블 모델, 129개 피처 | 승률 86.7%, 수익 79.84% (2,062회) |
| `main_beam_2` | [main_beam_2.py](main_beam_2.py) | 2분 | 앙상블 모델, 129개 피처 | 승률 88.9%, 수익 65.01% (2,143회) |
| `main_beam_4` | [main_beam_4.py](main_beam_4.py) | 4분 | 2단계 필터링 (앙상블 0.75 → 메타RF 0.65) | 승률 86.78%, 수익 56.76% (3,539회) |

**매매 로직**:
1. 매분 1초에 데이터 조회
2. n-1분 봉의 피처 계산 (129개)
3. ML 모델로 성공 확률 예측
4. 이전 봉 종가 * 0.999 지정가 매수
5. 매수 체결 시 매수가 * 1.001 지정가 매도
6. 타임아웃 경과 시 시장가 손절

---

### 2. ML 변동성 예측 전략

2단계 의사결정 시스템: 변동성 예측 (ML) → 방향 판단

| 전략명 | 파일 | 방향 판단 | TP/SL | 백테스트 성과 |
|--------|------|-----------|-------|---------------|
| `ml_momentum` | [ml_momentum.py](ml_momentum.py) | 모멘텀 5분/10분 방향 일치 | TP 0.2%, SL 0.7% | 승률 68.22%, 수익 64.69%, PF 1.22 |
| `ml_price_position` | [ml_price_position.py](ml_price_position.py) | 일일 가격 범위 하위 30% | TP 0.3%, SL 1.0% | 승률 79.67%, 수익 99.60%, PF 2.02 |

---

### 3. 일봉 기반 전략 (KODEX 레버리지 122630)

| 전략명 | 파일 | 유형 | 진입 조건 | TP/SL/Time | 백테스트 성과 |
|--------|------|------|-----------|------------|---------------|
| `bollinger_band` | [bollinger_band.py](bollinger_band.py) | 평균회귀 | BB 하단 돌파 (15일, 1.5σ) | +3%/-1.5%/10일 | 수익 34.3%, 승률 85.4%, MDD 1.6% |
| `breakout_high5` | [breakout_high.py](breakout_high.py) | 추세추종 | 5일 신고가 + 거래량 | +5%/-2%/10일 | 수익 86.1%, 승률 78.7%, MDD 2.1% |
| `fee_optimized` | [fee_optimized.py](fee_optimized.py) | 추세추종 | SMA20 상향 + ATR>0.2% | +4%/-2%/10일 | 수익 98.2%, 승률 55.6%, MDD 13.6% |
| `fibonacci_lucky` | [fibonacci_lucky.py](fibonacci_lucky.py) | 추세추종 | 5일 상승 + 거래량 증가 | +4%/-2%/5일 | 수익 92.7%, 승률 63.2%, MDD 4.7% |
| `hybrid_momentum` | [hybrid_momentum.py](hybrid_momentum.py) | 하이브리드 | 10일 모멘텀 3%+ + Z-Score>-1 | +3.5%/-2%/10일 | 수익 113.3%, 승률 61.8%, MDD 4.1% |

---

### 4. 일봉 기반 전략 (코스닥150레버리지 233740)

| 전략명 | 파일 | 유형 | 진입 조건 | TP/SL/Time | 백테스트 성과 |
|--------|------|------|-----------|------------|---------------|
| `kosdaq_donchian` | [kosdaq_donchian.py](kosdaq_donchian.py) | 추세추종 | 30일 돈치안 채널 돌파 | +3%/-2.5%/15일 | 연환산 34%, 승률 100% (4회) |
| `kosdaq_bb_conservative` | [kosdaq_bb_conservative.py](kosdaq_bb_conservative.py) | 평균회귀 | BB 하단 터치 (25일, 1.5σ) | +3%/-1.5%/10일 | 연환산 46%, 승률 70%, 평균 1일 |
| `kosdaq_mdd_target` | [kosdaq_mdd_target.py](kosdaq_mdd_target.py) | MDD관리 | ATR 0.2~4% + 5일 상승 | +3%/-3%/8일 | 연환산 14%, 승률 60%, MDD 6.25% |

---

### 5. 분봉 기반 전략

| 전략명 | 파일 | 설명 | 청산 방식 |
|--------|------|------|-----------|
| `dip_buy` | [dip_buy.py](dip_buy.py) | N분봉 시가 대비 하락 시 매수 | TP/SL/시간 청산 |
| `scalping_range` | [scalping_range.py](scalping_range.py) | N분봉 분석 후 매수 시그널 생성 | ScalpingExecutor 처리 |

---

### 6. 예제/테스트 전략

| 전략명 | 파일 | 설명 |
|--------|------|------|
| `example_strategy` | [example_strategy.py](example_strategy.py) | 단순 이동평균 크로스 예제 |
| `simple_momentum` | [example_strategy.py](example_strategy.py) | 단순 모멘텀 예제 |
| `test_strategy` | [test_strategy.py](test_strategy.py) | 테스트용 |
| `debug_signal_check` | [debug_signal_check.py](debug_signal_check.py) | 디버깅용 |

---

## 성과 비교표

### 수익률 순위 (백테스트 기준)

| 순위 | 전략명 | 수익률 | 승률 | MDD | 거래수 |
|------|--------|--------|------|-----|--------|
| 1 | hybrid_momentum | 113.3% | 61.8% | 4.1% | - |
| 2 | ml_price_position | 99.6% | 79.7% | - | 600 |
| 3 | fee_optimized | 98.2% | 55.6% | 13.6% | - |
| 4 | fibonacci_lucky | 92.7% | 63.2% | 4.7% | - |
| 5 | breakout_high5 | 86.1% | 78.7% | 2.1% | - |
| 6 | main_beam_1 | 79.8% | 86.7% | - | 2,062 |
| 7 | main_beam_2 | 65.0% | 88.9% | - | 2,143 |
| 8 | ml_momentum | 64.7% | 68.2% | - | 1,614 |
| 9 | main_beam_4 | 56.8% | 86.8% | - | 3,539 |
| 10 | bollinger_band | 34.3% | 85.4% | 1.6% | - |

### 승률 순위

| 순위 | 전략명 | 승률 | 비고 |
|------|--------|------|------|
| 1 | kosdaq_donchian | 100.0% | 4회 거래 |
| 2 | main_beam_2 | 88.9% | ML 스캘핑 |
| 3 | main_beam_1 | 86.7% | ML 스캘핑 |
| 4 | main_beam_4 | 86.8% | ML 스캘핑 |
| 5 | bollinger_band | 85.4% | 평균회귀 |

---

## 사용 방법

### 전략 등록

모든 전략은 `@register_strategy("strategy_name")` 데코레이터로 자동 등록됩니다.

```python
from leverage_worker.strategy.registry import register_strategy
from leverage_worker.strategy.base import BaseStrategy

@register_strategy("my_strategy")
class MyStrategy(BaseStrategy):
    def generate_signal(self, context):
        ...
```

### 전략 로드

```python
from leverage_worker.strategy.registry import get_strategy_class

StrategyClass = get_strategy_class("main_beam_4")
strategy = StrategyClass(name="main_beam_4", params={"threshold": 0.70})
```

### 전략 실행 (설정 예시)

```yaml
# config.yaml
strategy:
  name: main_beam_4
  params:
    old_threshold: 0.75
    new_threshold: 0.65
    timeout_seconds: 240
```

---

## 전략 개발 가이드

1. `BaseStrategy` 클래스를 상속
2. `generate_signal()` 메서드 구현 (필수)
3. `can_generate_signal()` 메서드 구현 (선택)
4. `on_entry()`, `on_exit()` 콜백 구현 (선택)
5. `@register_strategy()` 데코레이터로 등록
