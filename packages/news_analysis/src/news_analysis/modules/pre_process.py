"""

뉴스 데이터에 대한 파서 기능을 구현한다.

```json
{
    "lastBuildDate": "Tue, 11 Nov 2025 11:01:35 +0900",
    "total": 4073196,
    "start": 1,
    "display": 99,
    "items": [
        {
            "title": "&quot;하반기에도 플래그십 흥행&quot;...<b>삼성전자</b>, 폴드7·플립7·엣지로 실적 견...",
            "originallink": "https://www.asiaa.co.kr/news/articleView.html?idxno=229782",
            "link": "https://www.asiaa.co.kr/news/articleView.html?idxno=229782",
            "description": "<b>삼성전자</b>가 올해 선보인 갤럭시 플래그십 스마트폰이 상반기에 이어 하반기에도 연속 흥행을 이어가며 연중 내내 실적 호조세를 기록하고 있다. 11일 <b>삼성전자</b>에 따르면 역대 폴더블폰 최다 사전 판매량인 104만대... ",
            "pubDate": "Tue, 11 Nov 2025 11:00:00 +0900"
        },
        ...
}
```
"""
from bs4 import BeautifulSoup
import html
import logging
from datetime import datetime
import requests
import time, os

__all__ = (
    'TextTagCleaner',
    'pubdate_to_datetime',
    'to_unicode_escape',
    'get_naver_news_contents'
)


class TextTagCleaner:
    def __init__(
            self, 
            remove_html_tag: bool=True,
            remove_html_tag_unescape_letters: bool=True,
            remove_html_tag_unescape_letters_replace_to: str=''
    ) -> None:
        """
        불필요한 문자, HTML 태그 등을 제거하도록 도와주는 유틸리티 클래스.

        >>> text = "<b>삼성전자</b>가 올해 선보인 갤럭시 플래그십 스마트폰이 상반기에 이어 하반기에도 연속 흥행을 이어가며 연중 내내 실적 호조세를 기록하고 있다."
        >>> cleaner = TextTagCleaner()
        >>> print(cleaner(text))

        :param remove_html_tag: (bool) HTML 태그 제거 여부
        :param remove_html_tag_unescape_letters: (bool) HTML 엔티티(이스케이프 문자) 복원 여부 

        >>> print(cleaner("<b>삼성전자</b> &amp; 갤럭시"))
        >>> 삼성전자 & 갤럭시

        ```
        """
        self.__remove_html_tag = remove_html_tag
        self.__remove_html_tag_unescape_letters = remove_html_tag_unescape_letters
        self.__remove_html_tag_unescape_letters_replace_to = remove_html_tag_unescape_letters_replace_to

    def _remove_html_tag(self, text: str, strip: bool=False):
        """
        Raises:
            ValueError: if text is none
        Returns:
            str: cleaned text or plain text
        """
        if not text:
            raise ValueError("text is none, expected str")
        soup = BeautifulSoup(text, "html.parser")
        cleaned = soup.get_text(
            separator=self.__remove_html_tag_unescape_letters_replace_to, 
            strip=strip
        )
        if self.__remove_html_tag_unescape_letters:
            cleaned = html.unescape(cleaned)
        return cleaned

    def __call__(
            self,
            text: str,
    ) -> str | None:
        """
        Returns:
            str: cleaned text or none
        """
        cleaned = None
        if self.__remove_html_tag:
            try:
                cleaned = self._remove_html_tag(text)
            except ValueError as e:
                logging.error(e)
        if not cleaned:
            return text
        return cleaned



def pubdate_to_datetime(text: str | datetime):
    """입력 형식: "Tue, 11 Nov 2025 11:00:00 +0900"""
    if isinstance(text, datetime):
        return text
    return datetime.strptime(text, "%a, %d %b %Y %H:%M:%S %z")

def to_unicode_escape(text):
    return text.encode('unicode_escape').decode('utf-8')
    
def scrape_naver_news_content(url: str):
    """
    주어진 네이버 뉴스 URL에서 제목과 본문 내용을 스크랩합니다.
    일반 뉴스, 연예, 스포츠 뉴스 등 다양한 섹션의 구조를 처리합니다.

    Args:
        url (str): 스크랩할 네이버 뉴스 기사의 URL

    Returns:
        tuple[str, str] | None: (기사 제목, 본문 내용) 튜플. 실패 시 None을 반환합니다.
    """
    headers = {
        # 일부 언론사는 헤더를 검사하므로 일반적인 브라우저 헤더를 추가합니다.
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        # HTTP 에러 발생 시 예외 처리
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    title = ""
    content = None

    # 1. 기사 제목 찾기 (일반, 연예, 스포츠 공통 시도)
    title_tag = soup.select_one('h2#title_area span, h4.title, h2.end_tit')
    if title_tag:
        title = title_tag.get_text(strip=True)

    # 2. 기사 본문 찾기 (여러 선택자를 순서대로 시도)
    #   - 일반 뉴스: #dic_area
    #   - 연예 뉴스: #articeBody
    #   - 스포츠 뉴스: #newsEndContents
    content_selectors = ['#dic_area', '#articeBody', '#newsEndContents']
    for selector in content_selectors:
        content = soup.select_one(selector)
        if content:
            break
    
    if not content:
        print(f"본문 내용을 찾을 수 없습니다: {url}")
        return title, "본문 내용을 찾을 수 없습니다."

    # 3. 본문 내용에서 불필요한 요소 제거 (광고, 기자 정보 등)
    #    - decompose() 메서드는 해당 태그를 파싱 트리에서 완전히 제거합니다.
    unnecessary_tags = [
        'script', 'style', 'div.byline', 'div.reporter_area', 
        'span.end_photo_org', 'p.source', 'div.highlight-editor',
        'div.ad_body_2020x150'
    ]
    for tag_selector in unnecessary_tags:
        for tag in content.select(tag_selector):
            tag.decompose()

    # 4. 텍스트 추출 및 정리
    #    - get_text()를 사용하여 텍스트만 추출합니다.
    #    - separator='\n' : 태그와 태그 사이를 줄바꿈으로 연결합니다.
    #    - strip=True : 텍스트 앞뒤의 공백을 제거합니다.
    body_text = content.get_text(separator='\n', strip=True)
    
    return title, body_text

# --- 네이버 뉴스 검색 API 호출 함수 ---
def fetch_news_urls_from_naver_api(query: str, display_count: int = 100):
    """
    네이버 뉴스 검색 API를 호출하여 뉴스 기사 URL 리스트를 가져옵니다.
    API 호출에는 Client ID와 Client Secret이 필요합니다.
    환경 변수 또는 직접 코드에 설정해야 합니다.
    """
    # 네이버 개발자 센터에서 발급받은 Client ID와 Client Secret을 설정하세요.
    # 보안을 위해 환경 변수로 관리하는 것을 권장합니다.
    client_id = os.getenv("NAVER_CLIENT_ID", "tvcpOUvwQKCsxxm4lrnJ") # 여기에 실제 Client ID를 입력하세요
    client_secret = os.getenv("NAVER_CLIENT_SECRET", "ozpiQ5IAmO") # 여기에 실제 Client Secret을 입력하세요

    if client_id == "" or client_secret == "":
        print("경고: NAVER_CLIENT_ID 또는 NAVER_CLIENT_SECRET이 설정되지 않았습니다.")
        print("네이버 개발자 센터에서 발급받아 환경 변수로 설정하거나 코드에 직접 입력해주세요.")
        return []

    api_url = "https://openapi.naver.com/v1/search/news"
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret
    }
    params = {
        "query": query,
        "display": display_count, # 최대 100
        "start": 1,
        "sort": "date" # sim (정확도순), date (날짜순)
    }

    news_urls = []
    # 디버깅을 위해 실제 전송되는 헤더를 출력합니다.
    print("--- 실제 전송되는 헤더 정보 ---")
    print(headers)
    print("--------------------------")

    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if 'items' in data:
            for item in data['items']:
                # 네이버 뉴스 API는 'link' 필드에 기사 원문 URL을 제공합니다.
                # 하지만 가끔 외부 링크나 다른 형태의 링크가 있을 수 있으므로,
                # 'n.news.naver.com' 또는 'sports.news.naver.com'으로 시작하는 링크만 필터링하는 것이 좋습니다.
                if item['link'].startswith('https://n.news.naver.com') or \
                item['link'].startswith('https://sports.news.naver.com'):
                    news_urls.append(item['link'])
                else:
                    # 다른 형태의 링크는 스크래핑 대상에서 제외하거나 추가 처리 로직 필요
                    print(f"경고: 네이버 뉴스 도메인이 아닌 링크 발견 - {item['link']}")
        else:
            print(f"API 응답에 'items'가 없습니다: {data}")

    except requests.exceptions.RequestException as e:
        print(f"네이버 뉴스 API 호출 중 오류 발생: {e}")
    except ValueError as e:
        print(f"API 응답 JSON 파싱 오류: {e}")
    
    return news_urls

if __name__ == '__main__':
    text = "<b>삼성전자</b>가 올해 선보인 갤럭시 플래그십 스마트폰이 상반기에 이어 하반기에도 연속 흥행을 이어가며 연중 내내 실적 호조세를 기록하고 있다. 11일 <b>삼성전자</b>에 따르면 역대 폴더블폰 최다 사전 판매량인 104만대... "
    print(to_unicode_escape(text))

    cleaner = TextTagCleaner()
    print(cleaner(text))

    text_2 = "&quot;하반기에도 플래그십 흥행&quot;...<b>삼성전자</b>, 폴드7·플립7·엣지로 실적 견.."
    print(cleaner(text_2))

    print(f"{pubdate_to_datetime('Tue, 11 Nov 2025 11:00:00 +0900')!r}")

def get_naver_news_contents(query: str):
    news_url_list = fetch_news_urls_from_naver_api(query)

    scraped_results = []

    if not news_url_list:
        print("가져올 뉴스 URL이 없습니다. API 설정 또는 네트워크를 확인하세요.")
    else:
        print(f"총 {len(news_url_list)}개의 뉴스 URL을 가져왔습니다. 본문 스크래핑을 시작합니다...")

        # scraped_results = []

        for i, url in enumerate(news_url_list):
            print(f"[{i+1}/{len(news_url_list)}] 스크래핑 중: {url}")
            
            result = scrape_naver_news_content(url)
            
            if result:
                scraped_results.append({
                    "url": url,
                    "title": result[0],
                    "content": result[1]
                })
            
            # 서버에 과도한 부하를 주지 않기 위해 각 요청 사이에 지연 시간(delay)을 둡니다.
            # 실제 100건을 스크래핑할 경우, 이 지연 시간을 적절히 조절해야 합니다.
            time.sleep(1)

    return scraped_results

