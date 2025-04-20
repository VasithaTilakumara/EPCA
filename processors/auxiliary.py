from .base_processor import BaseProcessor
import pandas as pd

class AuxiliaryProcessor(BaseProcessor):
    def process(self):
        super().process()

        # Drop row where 'time' is null or zero
        if 'time' in self.df.columns:
            self.df = self.df[~self.df['time'].isnull() & (self.df['time'] != '0') & (self.df['time'] != 0)]

        # Rename first time/date column to session
        for col in self.df.columns:
            if 'time' in col or 'date' in col:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                if self.df[col].notna().sum() > 0:
                    self.df.rename(columns={col: 'session'}, inplace=True)
                    self.df.sort_values(by='session', inplace=True)
                    break

        return self.df
