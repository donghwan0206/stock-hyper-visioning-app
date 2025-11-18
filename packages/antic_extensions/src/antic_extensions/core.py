import logging
def set_logger():
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

__all__ = (
    'RedisService',
    'PsqlDBClient',
    'USE_LOGGER'
)
USE_LOGGER = True
if USE_LOGGER:
    set_logger()

from .service import RedisService
from .modules.database import PsqlDBClient


