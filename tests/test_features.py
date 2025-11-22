import pytest
import os
from data_utils.features import Features
from data_utils.data_scraper import BinanceDataScraper
from data_utils.data_loader import DataLoader

@pytest.fixture
def data_file():
    scraper = BinanceDataScraper(path='tests/test_data')
    filename = scraper.fetch_klines_15m(symbol='BTCUSDT', days_back=20)
    yield filename
    if os.path.exists(filename):        
       os.remove(filename)

def test_add_returns(data_file):
    loader = DataLoader(path=data_file)
    df = loader.get_data()
    
    feature_extractor = Features(data=df, config={})
    feature_extractor._add_returns()
    
    assert 'ret_1' in df.columns, "Missing 'ret_1' column"
    assert 'ret_4' in df.columns, "Missing 'ret_4' column"
    assert 'ret_16' in df.columns, "Missing 'ret_16' column"

def test_add_moving_average(data_file):
    loader = DataLoader(path=data_file)
    df = loader.get_data()
    
    feature_extractor = Features(data=df, config={})
    feature_extractor._add_moving_average(window=14)
    
    assert 'MA_14' in df.columns, "Missing 'MA_14' column"

    # ensure no NaN values in MA_14
    assert df['MA_14'].isnull().sum() == 13, "'MA_14' column contains NaN values beyond expected initial NaNs"


def test_add_volatility(data_file):
    loader = DataLoader(path=data_file)
    df = loader.get_data()
    
    feature_extractor = Features(data=df, config={})
    feature_extractor._add_volatility(window=14)
    
    assert 'vol_14' in df.columns, "Missing 'vol_14' column"

    # ensure no NaN values in vol_14
    assert df['vol_14'].isnull().sum() == 14, "'vol_14' column contains NaN values beyond expected initial NaNs"

def test_add_rsi(data_file):
    loader = DataLoader(path=data_file)
    df = loader.get_data()
    
    feature_extractor = Features(data=df, config={})
    feature_extractor._add_rsi(window=14)
    
    assert 'RSI_14' in df.columns, "Missing 'RSI_14' column"

    # ensure no NaN values in RSI_14
    assert df['RSI_14'].isnull().sum() == 13, "'RSI_14' column contains NaN values beyond expected initial NaNs"


