import pandas as pd
import base64
import json
import logging


class Transformer:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def transform_data(self, records):
        rows = []

        for record in records:
            payload = base64.b64decode(record['kinesis']['data'])
            data = json.loads(payload)
            data = json.loads(payload)
            if isinstance(data, str):
                data = json.loads(data)  # parse again
            rows.append(data)

        df = pd.DataFrame(rows)
        df.head(10)
        df = self.normalize_data(df)
        return df

    def normalize_data(self, df):
        column_map = {
            "e": "event",
            "E": "event_time",
            "s": "symbol",
            "p": "price_change",
            "P": "price_change_percent",
            "w": "weighted_avg_price",
            "x": "prev_close_price",       # match Athena schema
            "c": "last_price",
            "Q": "last_qty",               # match Athena schema
            "b": "best_bid_price",
            "B": "best_bid_qty",           # match Athena schema
            "a": "best_ask_price",
            "A": "best_ask_qty",           # match Athena schema
            "o": "open_price",
            "h": "high_price",
            "l": "low_price",
            "v": "base_volume",
            "q": "quote_volume",
            "O": "open_time",
            "C": "close_time",
            "F": "first_trade_id",
            "L": "last_trade_id",
            "n": "trade_count"
        }

        # Rename columns
        df.rename(columns=column_map, inplace=True)

        # Drop rows missing required columns
        required_cols = list(column_map.values())
        existing_required_cols = [
            col for col in required_cols if col in df.columns]
        before_rows = len(df)
        df = df.dropna(subset=existing_required_cols)
        after_rows = len(df)
        self.logger.info(
            f"❌ ✅ Removed {before_rows - after_rows} rows due to missing required columns")

        # Float columns
        float_columns = [
            "price_change", "price_change_percent", "weighted_avg_price",
            "prev_close_price", "last_price", "last_qty",
            "best_bid_price", "best_bid_qty", "best_ask_price",
            "best_ask_qty", "open_price", "high_price", "low_price",
            "base_volume", "quote_volume"
        ]
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Bigint columns
        bigint_columns = ["first_trade_id", "last_trade_id", "trade_count"]
        for col in bigint_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col], errors="coerce").fillna(0).astype("int64")

        # Timestamp columns
        timestamp_columns = ["event_time", "open_time", "close_time"]
        for col in timestamp_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col], unit="ms", errors="coerce").astype("datetime64[ms]")

        return df
