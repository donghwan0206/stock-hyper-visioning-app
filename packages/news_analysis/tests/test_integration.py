import pytest
from pprint import pprint
from news_analysis import NewsDataPipelineAPI
from news_analysis.modules.handlers import JSONLoader

def test_integration():
    mock_api_res = './tests/news.json'
    items = list(JSONLoader()(mock_api_res))
    filtered_items = [e for e in items
                      if 'items' in e][0]['items']
    
    api = NewsDataPipelineAPI()
    selected = api.select_top_k_by_date(filtered_items, 5, 'descending')
    pprint(selected)
