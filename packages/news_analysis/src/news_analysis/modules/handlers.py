import json
from typing import Generator, Any

__all__ = (
    'JSONLoader',
)

class JSONLoader:
    """제너레이터를 반환하는 JSONLoader.
    
    ```python
    # Example
    loader = JSONLoader()
    for item in loader("data.json"):
        print(item)
    ```
    """
    def __init__(self):
        self._data = None
    
    def __call__(self, path: str) -> Generator[Any, None, None]:
        with open(path, 'r', encoding='utf-8') as f:
            self._data = json.load(f)
        
        if isinstance(self._data, list):
            for item in self._data:
                yield item
        elif isinstance(self._data, dict):
            for key, value in self._data.items():
                yield {
                    key: value
                }
                # yield key, value
        else:
            yield self._data

