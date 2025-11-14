# News Analysis Package

뉴스 데이터 및 비정형 데이터에 대한 (전)처리와 분석을 제공한다.

![다이어그램](./docs/news_analysis_diagram.png)

**[주요 기능]**

1. 네이버 API로 부터 온 불순물 정보 제거
2. Client가 즉시 사용 가능한 형태로 데이터 형변환/가공
3. 제공된 뉴스 링크를 통해 본문 파싱/스크래핑

**목적: FAST API에서 서비스 레이어에서 즉시 사용 가능한 패키지 구현**

## 개발

### 패키지 설치

```bash
# cd ./news_analysis
pip install -e .
```

### 테스트

`pyproject.toml`이 있는 패키지 최상위에서 pytest -s 수행시 특정 쿼리로 테스트 스크래핑까지 동작할 수 있습니다.

```bash
cd ./packages/news_analysis
pytest
pytest -s	# 출력 확인
```

### 패키지 빌드

```bash
python -m build

# (예시) 외부 패키지/시스템에서 아래와 같은 형식으로 설치
pip install --upgrade ../../packages/news_analysis/dist/news_analysis-0.1.1-py3-none-any.whl
```

### Github CI/CD 예시

```yaml

- name: Build wheel
  run: |
    cd packages/news_analysis
    pip install build
    python -m build
    cp dist/*.whl ../../apps/azure/
```
