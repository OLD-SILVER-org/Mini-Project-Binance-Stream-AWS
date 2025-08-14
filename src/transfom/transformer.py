import pandas as pd
import base64
import json
import logging


class Transformer:
    def __init__(self):

        self.logger = logging.getLogger(self.__class__.__name__)
        pass

    def transform_data(self, records):
        rows = []

        for record in records:
            payload = base64.b64decode(record['kinesis']['data'])
            data = json.loads(payload)
            self.logger.info(f"✅ Received data: {data}")
            rows.append(data)
            pass

        df = pd.DataFrame(rows)
        df.head(10)
        # df = self.normalize_data(df)
        return df

    def normalize_data(self, df):
        column_map = {
            "e": "event",
            "E": "event_time",
            "s": "symbol",
            "p": "price_change",
            "P": "price_change_percent",
            "w": "weighted_avg_price",
            "x": "previous_close_price",
            "c": "last_price",
            "Q": "last_quantity",
            "b": "best_bid_price",
            "B": "best_bid_quantity",
            "a": "best_ask_price",
            "A": "best_ask_quantity",
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
        df.rename(columns=column_map, inplace=True)

        # Cast numeric columns
        float_columns = [
            "price_change", "price_change_percent", "weighted_avg_price",
            "previous_close_price", "last_price", "last_quantity",
            "best_bid_price", "best_bid_quantity", "best_ask_price",
            "best_ask_quantity", "open_price", "high_price", "low_price",
            "base_volume", "quote_volume"
        ]

        for col in float_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        integer_columns = ["first_trade_id", "last_trade_id", "trade_count"]
        for col in integer_columns:
            df[col] = pd.to_numeric(
                df[col], downcast='integer', errors='coerce')

        # Convert timestamps from milliseconds to datetime
        timestamp_columns = ["event_time", "open_time", "close_time"]
        for col in timestamp_columns:
            df[col] = pd.to_datetime(df[col], unit='ms', errors='coerce')

        return df
