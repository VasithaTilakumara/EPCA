from .base_processor import BaseProcessor

class Drive1InputProcessor(BaseProcessor):
    def process(self,df, input_key):
        df = super().clean_data(df, input_key)
        return df