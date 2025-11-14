"""
# News Analysis Package

뉴스 수집 (네이버) API 호출과 전처리, 본문 스크랩 (BeatifulSoup4)을 동시에 처리할 수 있습니다.

`from news_analysis import NewsDataPipelineAPI`를 사용하여 즉시 호출하세요.

반드시 `.env`파일을 구성해야 합니다. 

```
# filename: .env
# 네이버 API 클라이언트 ID 및 SECRET
NAVER_API_CLIENT_ID=
NAVER_API_CLIENT_SECRET=

```

**TODO:**  
    - 비동기 메서드 구현
    - 본문 스크랩 코드 안정화
"""
from .core import NewsDataPipelineAPI