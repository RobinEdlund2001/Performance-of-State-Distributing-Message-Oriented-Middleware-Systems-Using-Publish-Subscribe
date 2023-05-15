"""Sub-topic publisher for testing."""
from datetime import datetime
from test_protocol_pb2 import EntitySmall, EntityLarge
from publisher import Publisher
from settings import *


class SubTopicPublisher(Publisher):
    """Class for sub-topic publisher"""

    def __init__(self):
        super().__init__("SUB")

    def publish_entity_set(self) -> None:
        """If constants.USE_SMALL_ENTITY is True, publish ENTITIES_IN_SET number of
        timestamped 64-byte entities. If constants.USE_SMALL_ENTITY is False,
        publish ENTITIES_IN_SET number of timestamped 512-byte entities.
        """
        # Publish ENTITIES_IN_SET entities
        for i in range(1, ENTITIES_IN_SET + 1):
            timestamp = (datetime.now().strftime(TIMESTAMP_FORMAT)).encode()
            if USE_SMALL_ENTITY:
                entity = self.serialize_small_entity(i, timestamp)
            else:
                entity = self.serialize_large_entity(i, timestamp)

            subtopic = i.to_bytes(2, 'little')
            topic = bytearray(get_test_topic(self.is_single) + subtopic)

            message = [bytes(topic), entity]
            self.pub_socket.send_multipart(message)

    def serialize_small_entity(self, entity_id: int, timestamp: bytes) -> str:
        """Return a serialized small (64-byte) entity for sub-topic testing."""
        entity = EntitySmall()
        # Set id
        entity.id = entity_id
        # Add timestamp
        entity.timestamp = timestamp
        # Pad entity to reach correct entity size
        entity.padding = bytes(40)

        return entity.SerializeToString()

    def serialize_large_entity(self, entity_id: int, timestamp: bytes) -> str:
        """Return a serialized large (512-byte) entity for sub-topic testing."""
        entity = EntityLarge()
        # Set id
        entity.id = entity_id
        # Add timestamp
        entity.timestamp = timestamp
        # Pad entity to reach correct entity size
        entity.padding = bytes(487)

        return entity.SerializeToString()


if __name__ == "__main__":
    SubTopicPublisher()
