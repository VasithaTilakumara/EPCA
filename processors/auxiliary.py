from .base_processor import BaseProcessor
import pandas as pd

class AuxiliaryProcessor(BaseProcessor):
    def clean_data(self,df,input_key):
        df = super().clean_data(df,input_key)

        # # Drop row where 'time' is null or zero
        # if 'time' in df.columns:
        #     df = df[~df['time'].isnull() & (df['time'] != '0') & (df['time'] != 0)]

        # # Rename first time/date column to session
        # for col in df.columns:
        #     if 'time' in col or 'date' in col:
        #         df[col] = pd.to_datetime(df[col], errors='coerce')
        #         if df[col].notna().sum() > 0:
        #             df.rename(columns={col: 'session'}, inplace=True)
        #             df.sort_values(by='session', inplace=True)
        #             break

        return df
