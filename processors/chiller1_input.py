from .base_processor import BaseProcessor

class Chiller1InputProcessor(BaseProcessor):
    def process(self):
        super().process()
        return self.df