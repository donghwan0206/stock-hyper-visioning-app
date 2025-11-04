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

__all__ = (
    'TextTagCleaner',
    'pubdate_to_datetime',
    'to_unicode_escape'
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
    

if __name__ == '__main__':
    text = "<b>삼성전자</b>가 올해 선보인 갤럭시 플래그십 스마트폰이 상반기에 이어 하반기에도 연속 흥행을 이어가며 연중 내내 실적 호조세를 기록하고 있다. 11일 <b>삼성전자</b>에 따르면 역대 폴더블폰 최다 사전 판매량인 104만대... "
    print(to_unicode_escape(text))

    cleaner = TextTagCleaner()
    print(cleaner(text))

    text_2 = "&quot;하반기에도 플래그십 흥행&quot;...<b>삼성전자</b>, 폴드7·플립7·엣지로 실적 견.."
    print(cleaner(text_2))

    print(f"{pubdate_to_datetime('Tue, 11 Nov 2025 11:00:00 +0900')!r}")
