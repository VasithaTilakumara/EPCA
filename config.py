from processors.CCL1 import CCL1Processor
from processors.CCL1_input import CCL1InputProcessor
from processors.auxiliary import AuxiliaryProcessor
from processors.auxiliary_input import AuxiliaryInputProcessor
from processors.baattery1_input import Battery1InputProcessor
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
    'Auxiliary': AuxiliaryProcessor,
    'Auxiliary_Input': AuxiliaryInputProcessor,
    'Vehicle': VehicleProcessor,
    'Battery1': Battery1Processor,
    'Battery1_Input': Battery1InputProcessor,
    'CCL1': CCL1Processor,
    'CCL1_Input': CCL1InputProcessor,
    'Chiller1': Chiller1Processor,
    'Chiller1_Input': Chiller1InputProcessor,
    'DCDC': DCDCProcessor,
    'Drive1': Drive1Processor,
    'Drive1_Input': Drive1InputProcessor,
    'Drive2': Drive2Processor,
    'Drive2_Input': Drive2InputProcessor,
    'ChargeController1': ChargeController1Processor
}
