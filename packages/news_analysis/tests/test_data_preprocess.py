import pytest
from pprint import pprint
from src.news_analysis.service import NaverNewsDataResponseService
from src.news_analysis.modules.handlers import JSONLoader

def test_data_response():
    # mock_api_res = 'tests/news.json'

    # items = list(JSONLoader()(mock_api_res))

    # assert isinstance(items, list) and isinstance(items[0], dict)

    # filtered_items = [e for e in items
    #                   if 'items' in e][0]['items']
    
    # assert isinstance(filtered_items[0], dict)
    # assert len(set(filtered_items[0].keys()).difference(
    #     {'title', 'originallink', 'description', 'pubDate', 'link'}
    # )) == 0

    # service = NaverNewsDataResponseService()

    # # 전처리
    # data = service.clean_news_items(filtered_items)
    # assert isinstance(data[0]['pubDate'], str)

    # # 정렬 (날짜)
    # data = service.select_top_k_by_date_from(
    #     data, 5, sort='descending'
    # )
    # pprint(data)
    pprint("test_data_response activated")

