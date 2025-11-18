import pytest
import os
from dotenv import load_dotenv
load_dotenv()

def test_psql_connection():
    from src.antic_extensions.modules.database import PsqlDBClient
    client = PsqlDBClient(
        os.getenv('SQL_HOST'),
        os.getenv('SQL_USER'),
        os.getenv('SQL_PASSWORD'),
        os.getenv('SQL_DATABASE')
    )
    with client.cursor() as cur:
        cur.execute('SELECT 1')
        r = cur.fetchone()
        print("Psql Test:", 'Success' if r else 'Failed')
