from processors.CCL1 import CCL1Processor
from processors.CCL1_input import CCL1InputProcessor
from processors.auxiliary import AuxiliaryProcessor
from processors.auxiliary_input import AuxiliaryInputProcessor
from processors.battery1_input import Battery1InputProcessor
from processors.chargecontroller1 import ChargeController1Processor
from processors.chiller1 import Chiller1Processor
from processors.chiller1_input import Chiller1InputProcessor
from processors.dcdc import DCDCProcessor
from processors.drive1 import Drive1Processor
from processors.drive1_input import Drive1InputProcessor
from processors.drive2 import Drive2Processor
from processors.drive2_input import Drive2InputProcessor
from processors.vehicle import VehicleProcessor
from processors.battery1 import Battery1Processor

# Maps file name keywords to processor classes
PROCESSOR_MAPPING = {
    'auxiliary': AuxiliaryProcessor,
    'auxiliary_input': AuxiliaryInputProcessor,
    'vehicle': VehicleProcessor,
    'battery1': Battery1Processor,
    'battery1_input': Battery1InputProcessor,
    'ccl1': CCL1Processor,
    'ccl1_input': CCL1InputProcessor,
    'chiller1': Chiller1Processor,
    'chiller1_input': Chiller1InputProcessor,
    'dcdc': DCDCProcessor,
    'drive1': Drive1Processor,
    'drive1_input': Drive1InputProcessor,
    'drive2': Drive2Processor,
    'drive2_input': Drive2InputProcessor,
    'chargecontroller1': ChargeController1Processor
}
