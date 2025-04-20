from .base_processor import BaseProcessor

class VehicleProcessor(BaseProcessor):
    def process(self):
        super().process()
        return self.df