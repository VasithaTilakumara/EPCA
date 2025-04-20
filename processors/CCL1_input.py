from .base_processor import BaseProcessor

class CCL1InputProcessor(BaseProcessor):
    def process(self):
        super().process()
        return self.df