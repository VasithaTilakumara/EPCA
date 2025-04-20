from .base_processor import BaseProcessor

class Drive2Processor(BaseProcessor):
    def process(self):
        super().process()
        return self.df