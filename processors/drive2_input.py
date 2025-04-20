from .base_processor import BaseProcessor

class Drive2InputProcessor(BaseProcessor):
    def process(self):
        super().process()
        return self.df