import pandas as pd
import numpy as np

class Features:
    def __init__(self, data: pd.DataFrame, config: dict):
        self.data = data
        self.config = config
    
    def _add_returns(self):
        # Return percentage changes over 1, 4, and 16 periods
        for period in [1, 4, 16]:
            ret_column = f"ret_{period}"
            self.data[ret_column] = np.log(self.data['Close'] / self.data['Close'].shift(period))

    def _add_moving_average(self, window: int):
        ma_column = f"MA_{window}"
        self.data[ma_column] = self.data['Close'].rolling(window=window).mean()
    
    def _add_volatility(self, window: int):
        vol_column = f"vol_{window}"
        if 'ret_1' not in self.data.columns:
            self._add_returns()
        self.data[vol_column] = self.data['ret_1'].rolling(window=window).std()
    
    def _add_rsi(self, window: int):
        delta = self.data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi_column = f"RSI_{window}"
        self.data[rsi_column] = 100 - (100 / (1 + rs))