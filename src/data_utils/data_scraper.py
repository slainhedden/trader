import pandas as pd
from binance.client import Client
from binance.enums import HistoricalKlinesType
from datetime import datetime, timedelta

# 1. Initialize Client (No API keys needed for historical data)
class BinanceDataScraper:
    def __init__(self, path: str):
        self.client = Client()
        self.path = path # path to save data
    
    def _save_data(self, df: pd.DataFrame, days_back: int, symbol: str) -> str:
        filename = f"{self.path}/{symbol}_15m_data_{days_back}d.parquet"
        df.to_parquet(filename, index=False)
        print(f"Data saved to {filename}")
        return filename
    
    def fetch_klines_15m(self, symbol: str, days_back: int) -> str:
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%d %b, %Y")
        klines = self.client.get_historical_klines(
            symbol=symbol,
            interval=Client.KLINE_INTERVAL_15MINUTE,
            start_str=start_date,
            end_str=None,
            klines_type=HistoricalKlinesType.FUTURES
        )
        data = pd.DataFrame(klines)
        data = data.iloc[:, :6]  # Keep only the first 6 columns (Date, Open, High, Low, Close, Vol)
        data.columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        data['Open Time'] = pd.to_datetime(data['Open Time'], unit='ms')

        # Save to CSV
        filename = self._save_data(data, days_back, symbol)

        return filename

