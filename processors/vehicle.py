import re

from .base_processor import BaseProcessor
import pandas as pd

class VehicleProcessor(BaseProcessor):
    def process(self,df, input_key):
        # df = super().clean_data(df, input_key)
        try:
            df = super().clean_data(df, input_key)
        except Exception as e:
            print(f"clean_data() failed: {e}")
            return None

        # Updated function to return uptime in milliseconds
        def parse_uptime_to_milliseconds(value):
            match = re.match(r"T#(?:(\d+)s)?(?:(\d+)ms)?", str(value))
            if not match:
                return 0
            seconds = int(match.group(1)) if match.group(1) else 0
            milliseconds = int(match.group(2)) if match.group(2) else 0
            return seconds * 1000 + milliseconds

        # Apply the function to create a new column
        df['uptime_ms'] = df['uptime'].apply(parse_uptime_to_milliseconds)
        df['downtime_ms'] = df['downtime'].apply(parse_uptime_to_milliseconds)
        df['gearchngetime_ms'] = df['gearchngetime'].apply(parse_uptime_to_milliseconds)

        df['uptime'] = pd.to_numeric(df['uptime']).astype('int64')
        df['downtime'] = pd.to_numeric(df['downtime']).astype('int64')
        df['gearchngetime'] = pd.to_numeric(df['gearchngetime']).astype('int64')

        df['uptime_ms'] = pd.to_numeric(df['uptime_ms']).astype('int64')
        df['downtime_ms'] = pd.to_numeric(df['downtime_ms']).astype('int64')
        df['gearchngetime_ms'] = pd.to_numeric(df['gearchngetime_ms']).astype('int64')
        df['24volt'] = pd.to_numeric(df['24volt']).astype('int64')
        df['shutdownsafe'] = pd.to_numeric(df['shutdownsafe']).astype('int64')

        # Show examples
        # df[['uptime', 'uptime_ms']].dropna().head(10)

        # print(f"column type:{df.dtypes}")
        for col in df.columns:
                try:
                    # print(f"column type:{df[col].dtypes}")
                    if col == "shutdownsafe" and df[col].dtype == object:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
                except Exception:
                    print(f"Could not convert {col} to float64")

        return df