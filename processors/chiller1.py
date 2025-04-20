from .base_processor import BaseProcessor

class Chiller1Processor(BaseProcessor):
    def process(self):
        super().process()
        return self.df
