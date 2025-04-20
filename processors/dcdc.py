from .base_processor import BaseProcessor

class DCDCProcessor(BaseProcessor):
    def process(self):
        super().process()
        return self.df
