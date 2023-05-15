"""Constants from publisher and subscribe dummy"""
import os
from datetime import datetime
from test_protocol_pb2 import (EntitySmall, EntityLarge,
                               EntitySetSmall, EntitySetLarge)

PUB_SOCKET_ADDRESS = "tcp://localhost:5555"
SUB_SOCKET_ADDRESS = "tcp://localhost:5556"

# In bytes, from (datetime.now().strftime("%H:%M:%S.%f")).encode().
# Should only be used as reference.
TIMESTAMP_SIZE = 15
TIMESTAMP_FORMAT = '%H:%M:%S.%f'  # Hours:Minutes:Seconds:Microseconds

ENTITIES_IN_SET = 100  # Number of entities in each entity set.

# True if entity size should be 64-bytes, False if 512-bytes.
USE_SMALL_ENTITY = False

# In states (entity sets) per second. NOTE: Must be higher than 1 or else timedeltas get weird (probably).
ENTITY_SET_FREQUENCY = 100

# The increase in frequency for each new iteration. In states per second.
ENTITY_SET_FREQUENCY_STEP = 50

# Number of entity sets per iteration or frequency step.
ITERATION_ENTITY_SETS = 1000

# The ratio between the programmed throughput and the actual
# throughput when the test should end due to congestion.
THROUGHPUT_RATIO_CUTOFF = 0.5

ENTITY_SIZE_SMALL = 64  # In bytes, should only be used as reference.
ENTITY_SIZE_LARGE = 512  # In bytes, should only be used as reference.

TIMEOUT = 5

# If True, print results to stdout. If False, write results to file.
PRINT_RESULTS = False

# Result file constants
__FILENAME = "_TEST_"
__FILEFORMAT = ".csv"
__FOLDERNAME = "./logs/"
__FOLDER_TIME_FORMAT = "%Y-%m-%d"
__FILE_TIME_FORMAT = "%H%M%S"


def get_test_topic(single_topic_test: bool) -> bytes:
    if single_topic_test:
        return b'entity'

    return b'ent.'


def get_entity_set_size(single_topic_test: bool) -> int:
    """Return expected test entity set size in bytes."""
    if single_topic_test:
        if USE_SMALL_ENTITY:
            return len(get_test_topic(single_topic_test)) + ENTITIES_IN_SET * ENTITY_SIZE_SMALL + ENTITIES_IN_SET * 2

        return len(get_test_topic(single_topic_test)) + ENTITIES_IN_SET * ENTITY_SIZE_LARGE + ENTITIES_IN_SET * 3

    sub_topic_size = len(get_test_topic(single_topic_test)) + 2 # 2 bytes for entity id
    if USE_SMALL_ENTITY:
        return (sub_topic_size + ENTITY_SIZE_SMALL) * ENTITIES_IN_SET

    return (sub_topic_size + ENTITY_SIZE_LARGE) * ENTITIES_IN_SET


def __make_path(time) -> str:
    folder_name = __FOLDERNAME + time.strftime(__FOLDER_TIME_FORMAT) + "/"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    return folder_name


def get_test_filename(type_name: str) -> str:
    """Get next logfile name"""
    time = datetime.now()
    return (__make_path(time) + type_name + __FILENAME
            + time.strftime(__FILE_TIME_FORMAT) + __FILEFORMAT)


def get_latency_filename(test_type: str) -> str:
    """Get subscriber latency logfile name"""
    time = datetime.now()
    return (__make_path(time) + "SUB_" + test_type + __FILENAME + "LATENCY_"
            + time.strftime(__FILE_TIME_FORMAT) + __FILEFORMAT)
