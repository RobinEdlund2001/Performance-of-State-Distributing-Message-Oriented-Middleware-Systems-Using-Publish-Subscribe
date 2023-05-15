"""Sub-topic subscriber for testing."""
from datetime import datetime, timedelta
import zmq
from test_protocol_pb2 import EntitySmall, EntityLarge
from subscriber import Subscriber
from settings import *


class SubTopicSubscriber(Subscriber):
    """Class for Sub-topic subscriber"""

    def __init__(self) -> None:
        # Receive and send times for each entity in the set
        self.entity_set_latency_in_milliseconds = []

        super().__init__("SUB")

    def get_entity_set(self) -> int:
        """Attempt to receive an entity set update.
        Return 1 if a entity set was successfully received, 0 if not."""
        entities_received = 0
        last_entity = datetime.now()
        while entities_received < ENTITIES_IN_SET:
            if (datetime.now() - last_entity).seconds > TIMEOUT:
                return 0

            old_entities_recieved = entities_received
            entities_received += self.get_entity()

            if entities_received > old_entities_recieved:
                last_entity = datetime.now()

        delta_time = self.entity_set_latency_in_milliseconds[-1][1] - \
            self.entity_set_latency_in_milliseconds[0][0]
        self.latencies_milliseconds[-1].append(
            self.calculate_latency_in_milliseconds(delta_time))
        self.entity_set_latency_in_milliseconds.clear()

        return 1

    def get_entity(self) -> int:
        """Attempt to receive one entity from the subscriber socket."""
        try:
            message_parts = self.sub_socket.recv_multipart(zmq.NOBLOCK)

            # Timestamp receive time
            receive_time = datetime.now()

            if USE_SMALL_ENTITY:
                entity = EntitySmall()
            else:
                entity = EntityLarge()

            entity.ParseFromString(message_parts[1])
            # Get timestamp from entity.
            timestamp = entity.timestamp.decode()
            send_time = datetime.strptime(timestamp, TIMESTAMP_FORMAT)
            # Calculate latency
            self.entity_set_latency_in_milliseconds.append(
                (send_time, receive_time))

            return 1  # Successfully recieved a new entity.

        except zmq.ZMQError as error:
            if error.errno == zmq.EAGAIN:
                pass
            else:
                print(f"Unexpected ZMQError: {str(error)}")

            return 0  # No message was received.


if __name__ == "__main__":
    SubTopicSubscriber()
