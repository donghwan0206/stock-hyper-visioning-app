from .modules import *
from .service import *

class NewsDataPipelineAPI:
    """뉴스 데이터를 전처리하고 집계/가공할 수 있는 기능을 제공하는 API 클래스."""

    def fetch_news_from_naver_api(
        self,
        query: str,
        web_scrap_body: bool=True,
        preprocess: bool=True,
    ):
        """네이버 API로 부터 뉴스 데이터를 Fetch하고, 옵션에 따라 `전처리` 후 Return한다.

        **전처리 내용:**  
            1. 불필요 HTML 태그, 이스케이프 문자 등
        
        :param query: (str) 검색 문자열
        :param web_scrap_body: (str) 뉴스 본문 스크랩
        :param preprocess: (str) 문자열 전처리 여부. 
        """
        # data = service.clean_news_items(filtered_items)
        pass

    def select_top_k_by_date(
        self,
        data: list[NaverNewsApiResultTDict],
        k: int,
        sort: TSort
    ):
        """해당 데이터를 날짜 순으로 정렬하되, 상위 k개를 반환시킵니다.
        
        :param data: (str) 뉴스 데이터 리스트

        """
        service = NaverNewsDataResponseService()
        return service.select_top_k_by_date_from(
            data, k, sort=sort
        )

