import pytest
from pprint import pprint
from src.news_analysis import NewsDataPipelineAPI
from src.news_analysis.modules.handlers import JSONLoader

def test_integration():
    # mock_api_res = './tests/news.json'
    # items = list(JSONLoader()(mock_api_res))
    # filtered_items = [e for e in items
    #                   if 'items' in e][0]['items']
    
    api = NewsDataPipelineAPI()
    # selected = api.select_top_k_by_date(filtered_items, 5, 'descending')
    selected = api.fetch_news_from_naver_api(query="인공지능")
    pprint(selected)
