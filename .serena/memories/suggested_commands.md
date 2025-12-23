# Suggested Commands

## Package Management (uv)

```bash
# uv 패키지 매니저 설치 (Windows PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# 의존성 설치
uv sync

# 의존성 재설치 (문제 발생 시)
uv sync --reinstall
```

## Running Examples

### examples_user (통합 예제)
```bash
# 국내주식 REST API 예제
python examples_user/domestic_stock/domestic_stock_examples.py

# 국내주식 WebSocket 예제
python examples_user/domestic_stock/domestic_stock_examples_ws.py
```

### examples_llm (단일 API 테스트)
```bash
# 특정 API 테스트 (예: 주식현재가 시세)
python examples_llm/domestic_stock/inquire_price/chk_inquire_price.py
```

## Windows System Commands
- `dir` - 디렉토리 목록 (ls 대체)
- `type` - 파일 내용 출력 (cat 대체)
- `findstr` - 텍스트 검색 (grep 대체)
- `where` - 파일 위치 찾기 (which 대체)
- `copy`, `move`, `del` - 파일 작업

## Git Commands
```bash
git status
git add .
git commit -m "message"
git push
git pull
```

## Important Notes
- bat 파일 사용 금지 (한글 깨짐, 가독성 저하)
- Python 직접 실행: `python 파일명.py`
- 실행 파라미터는 `파일명.txt`에 별도 저장
