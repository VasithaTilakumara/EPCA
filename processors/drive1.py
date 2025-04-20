from .base_processor import BaseProcessor

class Drive1Processor(BaseProcessor):
    def process(self):
        super().process()
        return self.df
