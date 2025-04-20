from .base_processor import BaseProcessor

class Battery1Processor(BaseProcessor):
    def process(self):
        super().process()
        self.df.drop(columns=[col for col in self.df.columns if 'unnamed' in col.lower()], inplace=True, errors='ignore')
        return self.df