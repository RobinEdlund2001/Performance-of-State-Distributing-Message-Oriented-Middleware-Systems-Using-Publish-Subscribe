"""Single-topic publisher for testing."""
from datetime import datetime
from test_protocol_pb2 import (EntitySmall, EntityLarge,
                               EntitySetSmall, EntitySetLarge)
from publisher import Publisher
from settings import *


class SingleTopicPublisher(Publisher):
    """Class for single-topic publisher"""

    def __init__(self):
        super().__init__("SINGLE")

    def publish_entity_set(self) -> None:
        """If constants.USE_SMALL_ENTITY is True, publish a timestamped entity set
        message with small (64-byte) entities. If constants.USE_SMALL_ENTITY is False,
        publish a timestamped entity set message with large (512-byte) entities.
        """
        timestamp = (datetime.now().strftime(TIMESTAMP_FORMAT)).encode()

        if USE_SMALL_ENTITY:
            entity_set = self.serialize_small_entity_set(timestamp)
        else:
            entity_set = self.serialize_large_entity_set(timestamp)

        message = [get_test_topic(self.is_single), entity_set]
        self.pub_socket.send_multipart(message)

    def serialize_small_entity_set(self, timestamp: bytes) -> str:
        """Return a serialized small entity set for single-topic testing."""
        entity_set = EntitySetSmall()

        # Add entities
        for i in range(1, ENTITIES_IN_SET + 1):
            entity = entity_set.entities.add()
            # Set id
            entity.id = i
            # Add timestamp
            entity.timestamp = timestamp
            # Pad entity to reach correct entity size
            entity.padding = bytes(40)

        return entity_set.SerializeToString()

    def serialize_large_entity_set(self, timestamp: bytes) -> str:
        """Return a serialized large entity set for single-topic testing."""
        entity_set = EntitySetLarge()

        # Add entities
        for i in range(1, ENTITIES_IN_SET + 1):
            entity = entity_set.entities.add()
            # Set id
            entity.id = i
            # Add timestamp
            entity.timestamp = timestamp
            # Pad entity to reach correct entity size
            entity.padding = bytes(487)

        return entity_set.SerializeToString()


if __name__ == "__main__":
    SingleTopicPublisher()
