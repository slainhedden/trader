import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np


class DataLoader:
    def __init__(self, path: str):
        self.path = path
        self.data = None

    def _convert_types(self):
        self.data['Open'] = self.data['Open'].astype(float)
        self.data['High'] = self.data['High'].astype(float)
        self.data['Low'] = self.data['Low'].astype(float)
        self.data['Close'] = self.data['Close'].astype(float)
        self.data['Volume'] = self.data['Volume'].astype(float)

    def _add_sessions(self):
        def get_session(hour):
            if 0 <= hour < 8: return 'Asia'
            elif 8 <= hour < 13: return 'Europe'
            elif 13 <= hour < 21: return 'New York'
            else: return 'Other'
        
        self.data['Session'] = self.data['Open Time'].dt.hour.apply(get_session)
        
        # Add boolean flags for session open/close
        self.data['is_session_open'] = self.data['Session'] != self.data['Session'].shift(1)
        self.data['is_session_close'] = self.data['Session'] != self.data['Session'].shift(-1)
    
    def _add_day_of_week(self):
        self.data['Day'] = self.data['Open Time'].dt.day_name()
    
    def load_data(self):
        self.data = pd.read_parquet(self.path)
        self._convert_types()
        self._add_sessions()
        self._add_day_of_week()
    
    def get_data(self) -> pd.DataFrame:
        if self.data is None:
            self.load_data()
        return self.data
    
    def get_session_data(self, session: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        if self.data is None:
            self.load_data()
        
        valid_sessions = ['Asia', 'Europe', 'New York', 'Other']
        if session not in valid_sessions:
            raise ValueError(f"Invalid session. Choose from {valid_sessions}")
        
        session_data = self.data[self.data['Session'] == session].copy()
        
        if start_date is not None or end_date is not None:
            if start_date is not None:
                session_data = session_data[session_data['Open Time'] >= pd.to_datetime(start_date)]
            if end_date is not None:
                session_data = session_data[session_data['Open Time'] <= pd.to_datetime(end_date)]
        
        return session_data
