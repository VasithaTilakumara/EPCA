from .base_processor import BaseProcessor

class Battery1InputProcessor(BaseProcessor):
    def clean_data(self, df, input_key):
        df = super().clean_data(df, input_key)
        df.drop(columns=[col for col in df.columns if 'unnamed' in col.lower()], inplace=True, errors='ignore')
        return df