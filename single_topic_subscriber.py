"""Single-topic subscriber for testing."""
from datetime import datetime
import zmq
from test_protocol_pb2 import (EntitySmall, EntityLarge,
                               EntitySetSmall, EntitySetLarge)
from subscriber import Subscriber
from settings import *


class SingleTopicSubscriber(Subscriber):
    """Class for single-topic subscriber"""

    def __init__(self) -> None:
        super().__init__("SINGLE")

    def get_entity_set(self) -> int:
        """Attempt to receive an entity set update.
        Return 1 if a message was successfully received, 0 if not."""
        try:
            message_parts = self.sub_socket.recv_multipart(zmq.NOBLOCK)

            # Timestamp receive time
            receive_time = datetime.now()

            if USE_SMALL_ENTITY:
                entity_set = EntitySetSmall()
            else:
                entity_set = EntitySetLarge()

            entity_set.ParseFromString(message_parts[1])
            # Get timestamp from first entity.
            timestamp = entity_set.entities[0].timestamp.decode()
            send_time = datetime.strptime(timestamp, TIMESTAMP_FORMAT)

            # Calculate latency
            delta_time = receive_time - send_time
            latency_in_milliseconds = self.calculate_latency_in_milliseconds(delta_time)
            self.latencies_milliseconds[-1].append(latency_in_milliseconds)

            return 1  # Successfully recieved a new message.

        except zmq.ZMQError as error:
            if error.errno == zmq.EAGAIN:
                pass
            else:
                print(f"Unexpected ZMQError: {str(error)}")

            return 0  # No message was received.


if __name__ == "__main__":
    SingleTopicSubscriber()
