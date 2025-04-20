from .base_processor import BaseProcessor

class CCL1Processor(BaseProcessor):
    def process(self):
        super().process()
        return self.df