import pytest
import os
from data_utils.data_loader import DataLoader
from data_utils.data_scraper import BinanceDataScraper

@pytest.fixture
def data_file():
    scraper = BinanceDataScraper(path='tests/test_data')
    filename = scraper.fetch_klines_15m(symbol='BTCUSDT', days_back=1)
    yield filename
    if os.path.exists(filename):        
       os.remove(filename)
    
def test_data_structure(data_file):
    loader = DataLoader(path=data_file)
    loader.load_data()
    df = loader.get_data()
    
    assert not df.empty, "Dataframe should not be empty"
    assert 'Session' in df.columns, "Missing 'Session' column"
    assert 'Day' in df.columns, "Missing 'Day' column"
    assert not df.empty

def test_get_session_data(data_file):
    loader = DataLoader(path=data_file)
    loader.load_data()
    
    asia_data = loader.get_session_data('Asia')
    europe_data = loader.get_session_data('Europe')
    ny_data = loader.get_session_data('New York')
    
    assert not asia_data.empty, "Asia session data should not be empty"
    assert not europe_data.empty, "Europe session data should not be empty"
    assert not ny_data.empty, "New York session data should not be empty"
    
    assert all(asia_data['Session'] == 'Asia'), "All rows should belong to Asia session"
    assert all(europe_data['Session'] == 'Europe'), "All rows should belong to Europe session"
    assert all(ny_data['Session'] == 'New York'), "All rows should belong to New York session"
    