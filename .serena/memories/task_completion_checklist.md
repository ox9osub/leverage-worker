# Task Completion Checklist

## Before Committing Code

### 1. Code Quality
- [ ] 코드가 Python 전문가 수준의 품질을 유지하는지 확인
- [ ] 타입 힌트가 함수 파라미터, 반환값, 변수에 적용되었는지 확인
- [ ] 네이밍 컨벤션 준수 (snake_case, PascalCase, UPPER_SNAKE_CASE)
- [ ] Docstring 작성 완료 (목적, 인자, 반환값, 예제)

### 2. Import & Structure
- [ ] Import 순서 확인 (표준 → 서드파티 → 로컬)
- [ ] Wildcard import 사용 안함
- [ ] 함수/클래스가 단일 책임 원칙 준수

### 3. Error Handling
- [ ] try-except 블록에 구체적인 예외 타입 명시
- [ ] logging을 사용한 에러/이벤트 기록

### 4. Testing
- [ ] 모의투자(`svr="vps"`)에서 충분히 테스트
- [ ] chk_*.py 테스트 파일 실행 확인

```bash
# 테스트 실행 예시
python examples_llm/domestic_stock/inquire_price/chk_inquire_price.py
```

### 5. Configuration
- [ ] API 키, 경로 등 민감 정보가 코드에 하드코딩되지 않았는지 확인
- [ ] kis_devlp.yaml에 설정 분리

## Development Guidelines

### 실행 방법
- bat 파일 사용 금지
- Python 직접 실행: `python 파일명.py`
- 실행 파라미터는 `파일명.txt`에 별도 저장

### 매매 로직 주의사항
- 반드시 모의투자에서 충분히 테스트 후 실전 적용
- 코드 작성 전 계획을 세우고, 에러 없이 수행되도록 철저히 검토

## Common Issues

### 토큰 오류
```python
import kis_auth as ka
ka.auth(svr="prod")  # 또는 "vps", 1분당 1회 발급
```

### 의존성 오류
```bash
uv sync --reinstall
```
