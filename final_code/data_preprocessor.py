import pandas as pd

class DataPreprocessor:
    def __init__(self):
        pass

    def market_filter(self, df: pd.DataFrame, market: str) -> pd.DataFrame:
        """특정 마켓 데이터만 반환"""
        df = df.copy()
        return df[df["market"] == market].reset_index(drop=True)

    def time_transform(self, df: pd.DataFrame, tz: str = "Asia/Seoul") -> pd.DataFrame:
        """timestamp 컬럼을 ms epoch → tz 변환 → naive datetime"""
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert(tz)
        df["timestamp"] = df["timestamp"].dt.tz_localize(None)
        return df
    def deduplicate(self, df: pd.DataFrame, how: str = "last") -> pd.DataFrame:
        """timestamp 기준 중복 제거"""
        df = df.copy().set_index("timestamp")
        if how == "last":
            df = df[~df.index.duplicated(keep="last")]
        elif how == "first":
            df = df[~df.index.duplicated(keep="first")]
        elif how == "mean":
            df = df.groupby(df.index).mean(numeric_only=True)
        return df.reset_index()