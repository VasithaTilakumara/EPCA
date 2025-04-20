from .base_processor import BaseProcessor

class Drive1InputProcessor(BaseProcessor):
    def process(self):
        super().process()
        return self.df