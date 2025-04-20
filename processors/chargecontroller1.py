from .base_processor import BaseProcessor

class ChargeController1Processor(BaseProcessor):
    def process(self):
        super().process()
        return self.df