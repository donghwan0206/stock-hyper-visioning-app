"""
(API 서비스 구현) 네이버 뉴스 데이터 출력을 위한 모듈

"""
import heapq, logging
from datetime import datetime
from typing import TypedDict, Literal, Union, Iterable
from ..modules import (
    TextTagCleaner, 
    pubdate_to_datetime
)

__all__ = (
    'NaverNewsDataResponseService',
    'NaverNewsApiResultTDict',
    'TSort'
)

TSort = Literal['ascending', 'descending']

class NaverNewsApiResultTDict(TypedDict):
    """News API의 결괏값에서 'items'키에 담긴 배열 데이터에 대한 형식
    >>> [
        {
            "title": "<b>삼성전자</b>, 임직원 특허 보상금 최대 2배 인상",
            "originallink": "https://www.etnews.com/20251110000159",
            "link": "https://n.news.naver.com/mnews/article/030/0003368429?sid=105",
            "description": "<b>삼성전자</b>가 임직원 특허 보상금을 최대 2배 올렸다. 2017년 이후 약 10년 만의 큰 폭 인상으로 기술 경쟁력 강화와 신기술 개발 장려를 위한 조처로 풀이된다. 이번 인상안은 2027년 9월까지 2년간 적용된다. 해외... ",
            "pubDate": "Mon, 10 Nov 2025 11:13:00 +0900"
        },
        ...
    ]
    """
    title: str
    originallink: str
    link: str
    description: str
    pubDate: str | datetime


class NaverNewsDataResponseService:
    """네이버 뉴스 데이터에 대해 클라이언트에서 바로 사용가능한 형태로 데이터를 제공하는 클래스."""

    def __init__(self) -> None:
        self._text_tag_cleaner = TextTagCleaner(
            remove_html_tag=True,
            remove_html_tag_unescape_letters=True,
        )

    def clean_news_items(
            self, 
            items: Iterable[NaverNewsApiResultTDict]
    ):
        """
        네이버 뉴스 API 결과를 받아 정제된 뉴스 데이터 반환.

        >>> items = [
            {
                "title": "<b>삼성전자</b>, 임직원 특허 보상금 최대 2배 인상",
                "originallink": "https://www.etnews.com/20251110000159",
                "link": "https://n.news.naver.com/mnews/article/030/0003368429?sid=105",
                "description": "<b>삼성전자</b>가 임직원 특허 보상금을 최대 2배 올렸다. 2017년 이후 약 10년 만의 큰 폭 인상으로 기술 경쟁력 강화와 신기술 개발 장려를 위한 조처로 풀이된다. 이번 인상안은 2027년 9월까지 2년간 적용된다. 해외... ",
                "pubDate": "Mon, 10 Nov 2025 11:13:00 +0900"
            },
            ...
        ]
        >>> parse_news_item(items)

        >>> [
            {
                "title": "삼성전자, 임직원 특허 보상금 최대 2배 인상,
                "originallink": "https://www.etnews.com/20251110000159",
                "link": "https://n.news.naver.com/mnews/article/030/0003368429?sid=105",
                "description": "삼성전자가 임직원 특허 보상금을 최대 2배 올렸다. 2017년 이후 약 10년 만의 큰 폭 인상으로 기술 경쟁력 강화와 신기술 개발 장려를 위한 조처로 풀이된다. 이번 인상안은 2027년 9월까지 2년간 적용된다. 해외... 
                "pubDate": "Mon, 10 Nov 2025 11:13:00 +0900"
            },
            ...
        ]
        """
        result = []
        try:
            for item in items:
                d = {
                    "title": self._text_tag_cleaner(item['title']),
                    "originallink": item['originallink'],
                    "link": item["link"],
                    "description": self._text_tag_cleaner(item["description"]),
                    "pubDate": item['pubDate']
                }
                result.append(d)
        except KeyError as e:
            logging.error(e)
        return result

    def select_top_k_by_date_from(
            self, 
            data: list[NaverNewsApiResultTDict],
            k: int,
            sort: TSort='descending',
            sort_item_key: str='pubDate'
    ):
        """(기본값: pubDate)특정 키를 기준으로 k개의 항목을 정렬하여 선택합니다.

        Raises:
            ValueError: data가 none인 경우

        :param data: (list[NaverNewsApiResultTDict]) 뉴스 항목 리스트
        :param k: (int) 선택할 항목 개수
        :param sort: (str) 정렬 방향 ('descending' | 'ascending')

        Examples:

        >>> data = [
            {
                "title": "<b>삼성전자</b>, 차세대 저전력 D램 공개…전력 소모 21%↓",
                "originallink": "https://www.newsis.com/view/NISX20251110_0003397262",
                "link": "https://n.news.naver.com/mnews/article/003/0013592191?sid=101",
                "description": "전력 부족이 인공지능(AI) 산업의 핵심 병목으로 떠오르는 가운데, <b>삼성전자</b>가 전력 효율을 높인 차세대 저전력 D램(LPDDR6)를 내년 출시한다. 11일 업계에 따르면 <b>삼성전자</b>가 내년 초 열리는 세계 최대 IT·가전... ",
                "pubDate": "Tue, 11 Nov 2025 07:00:00 +0900"
            },
            {
                "title": "고창 '<b>삼성전자</b> 스마트허브단지' 착공",
                "originallink": "https://www.yna.co.kr/view/PYH20251110078800055?input=1196m",
                "link": "https://n.news.naver.com/mnews/article/001/0015733289?sid=101",
                "description": "10일 전북 고창군 고수면 신활력산업단지에서 열린 '<b>삼성전자</b> 스마트허브단지(물류센터) 착공식'에서 김관영 전북도지사와 박순철 <b>삼성전자</b> 부사장, 심덕섭 고창군수(왼쪽부터)가 기념 세리머니를 하고 있다. 2025.11.10",
                "pubDate": "Mon, 10 Nov 2025 12:16:00 +0900"
            },
            ...
        ]
        >>> service = NewsDataResponseService()
        >>> result = service.select_top_k_from(data, 5, 'descending')
        >>> print(result)
        >>> [
            {
                "title": "고창 '<b>삼성전자</b> 스마트허브단지' 착공",
                "originallink": "https://www.yna.co.kr/view/PYH20251110078800055?input=1196m",
                "link": "https://n.news.naver.com/mnews/article/001/0015733289?sid=101",
                "description": "10일 전북 고창군 고수면 신활력산업단지에서 열린 '<b>삼성전자</b> 스마트허브단지(물류센터) 착공식'에서 김관영 전북도지사와 박순철 <b>삼성전자</b> 부사장, 심덕섭 고창군수(왼쪽부터)가 기념 세리머니를 하고 있다. 2025.11.10",
                "pubDate": "datetime()"
            }
        ]
        """
        if data is None:
            raise ValueError("Data is none. expected list[NaverNewsApiResultTDict]")
        elif sort not in ("descending", "ascending"):
            raise ValueError(f"Invalid sort arg: {sort}")
        elif not data:
            return []
        heap_func = heapq.nlargest if sort == "descending" else heapq.nsmallest
        default_date = datetime(1970, 1, 1) if sort == "descending" else datetime(2099, 1, 1)
        top_k = heap_func(
            k, data, 
            key=lambda item: pubdate_to_datetime(item.get(sort_item_key, default_date))
        )
        return top_k



