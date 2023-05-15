""" The Subscriber base class for testing single-topic versus sub-topic publish-subscribe.
Functions are sorted in the order they are first called.
"""
from datetime import datetime, timedelta
import csv
import zmq
from settings import *


class TimeOut(Exception):
    """Occurs when the subscriber times out waiting for topic updates."""


class Subscriber():
    """Base class for Subscriber."""

    def __init__(self, test_type: str) -> None:
        test_start_time = datetime.now()

        print(f"Starting {test_type.lower()}-topic Subscriber")
        self.test_type = test_type
        self.sub_file_path = get_test_filename("SUB_" + self.test_type)
        self.sub_latency_file_path = get_latency_filename(self.test_type)
        self.is_single = test_type == "SINGLE"

        # All iteration results
        self.results = []

        # Recorded latencies
        self.latencies_milliseconds = [[]]  # In microseconds

        # CSV fields
        self.field_names = [
            # Iteration number (to mark which data applies to which iteration)
            'iteration',
            # Average latency in seconds: Sum of all latencies / amount of latencies
            'avg_latency_ms',
            # Entity sets recieved
            'entity_sets_receieved',
            # Throughput entity sets/s =  entity_sets_receieved / (end_time - start_time / 1000000)
            'throughput_sets/s',
            # Throughput kB/s = (entity_sets_receieved * get_entity_set_size()) / (end_time - start_time)
            'throughput_kB/s',
            # end_time - start_time
            'delta_milliseconds'
        ]

        self.latency_field_names = [
            'iteration',
            'entity_set_nr',
            'entity_set_latency_milliseconds'
        ]

        # ZMQ context
        self.context = zmq.Context()

        self.open_subscriber()
        self.listen()
        self.close_subscriber()
        self.record_results()

        test_end_time = datetime.now()
        print(f"Total test time: {test_end_time - test_start_time}")

    def open_subscriber(self) -> None:
        """Start the subscriber socket and subscribe to the test topic."""
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(SUB_SOCKET_ADDRESS)
        self.sub_socket.subscribe(get_test_topic(self.is_single))

    def listen(self) -> None:
        """Listen for incoming topic updates and store test results."""
        # Receive all incoming entity sets
        while True:
            try:
                # Wait until incoming entity set
                _ = self.sub_socket.poll(TIMEOUT * 1000)

                start_time = datetime.now()
                entity_sets_received = self.get_iteration_entity_sets()
                end_time = datetime.now()
            except TimeOut:
                break

            # Calculate latencies
            average_latency_milliseconds = self.calculate_average_latency_milliseconds()
            delta_in_microseconds = self.get_delta_time_in_microseconds(
                start_time,
                end_time
            )
            self.results.append(self.calculate_results(average_latency_milliseconds,
                                                       entity_sets_received,
                                                       delta_in_microseconds
                                                       ))
            self.latencies_milliseconds.append([])

    def get_iteration_entity_sets(self) -> int:
        """Get all entity sets for one iteration."""
        entity_sets_received = 0
        last_entity_set = datetime.now()
        while entity_sets_received < ITERATION_ENTITY_SETS:
            if (datetime.now() - last_entity_set).seconds > TIMEOUT:
                raise TimeOut()

            old_entity_sets_recieved = entity_sets_received
            entity_sets_received += self.get_entity_set()

            if entity_sets_received > old_entity_sets_recieved:
                last_entity_set = datetime.now()
        return entity_sets_received

    def get_entity_set(self) -> int:
        """Get one entity set. Should be overridden by inheriting subclasses."""
        raise NotImplementedError("Not implemented in base class.")

    def calculate_latency_in_milliseconds(self, delta_time: timedelta) -> float:
        """Calculates latency in milliseconds based on given delta time"""
        return (delta_time.seconds * 1000000 + delta_time.microseconds) / 1000

    def calculate_average_latency_milliseconds(self) -> float:
        """Return the latest iterations average entity set latency in milliseconds."""
        if 0 < len(self.latencies_milliseconds[-1]):
            return sum(self.latencies_milliseconds[-1]) / len(self.latencies_milliseconds[-1])

        return 0

    def get_delta_time_in_microseconds(self,
                                       start_time: datetime,
                                       end_time: datetime) -> float:
        """Return the time difference between end_time and start_time in microseconds."""
        delta_time = end_time - start_time
        delta_in_microseconds = delta_time.seconds * 1000000 + delta_time.microseconds
        return delta_in_microseconds

    def calculate_results(self,
                          average_latency_milliseconds: float,
                          entity_sets_received: int,
                          delta_in_microseconds: int) -> tuple:
        """Calculate iteration results."""
        throughput_entity_sets_per_second = entity_sets_received / \
            (delta_in_microseconds / 1000000)
        throughput_kilobytes_per_seconds = entity_sets_received * get_entity_set_size(self.is_single) / \
            (delta_in_microseconds / 1000)

        delta_in_seconds = delta_in_microseconds / 1000000
        delta_in_milliseconds = delta_in_microseconds / 1000

        return (average_latency_milliseconds,  # 0
                entity_sets_received,  # 1
                delta_in_milliseconds,  # 2
                delta_in_seconds,  # 3
                throughput_entity_sets_per_second,  # 4
                throughput_kilobytes_per_seconds  # 5
                )

    def close_subscriber(self) -> None:
        """Close the subscriber socket and terminate the ZMQ context."""
        self.sub_socket.close()
        self.context.term()

    def record_results(self) -> None:
        """Record test results to chosen output."""
        if PRINT_RESULTS:
            self.print_results()
        else:
            self.results_to_csv_file()

    def print_results(self) -> None:
        """Print all results to stdout."""
        i = 1
        for result in self.results:
            result = tuple(result)
            print(f"-------- Iteration {i} ------------")
            print(f"Average latency: {result[0]} ms\n")
            print(f"Received {result[1]} entity sets in {result[3]} s.\n")
            print(f"Throughput: {result[4]} sets/s\n")
            print(f"Throughput: {result[5]} kB/s\n")
            i += 1

    def results_to_csv_file(self) -> None:
        """Write self.results to given result file."""
        with open(self.sub_file_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.field_names)
            writer.writeheader()
            i = 1
            for result in self.results:
                result = tuple(result)
                writer.writerow({'iteration': i,
                                'avg_latency_ms': result[0],
                                 'entity_sets_receieved': result[1],
                                 'throughput_sets/s': result[4],
                                 'throughput_kB/s': result[5],
                                 'delta_milliseconds': result[2]
                                 })
                i += 1

        with open(self.sub_latency_file_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=self.latency_field_names)
            writer.writeheader()
            i = 1
            for iteration_latencies in self.latencies_milliseconds:
                entity_set = 1
                for entity_set_latency_milliseconds in iteration_latencies:
                    entity_set_latency_milliseconds = float(
                        entity_set_latency_milliseconds)
                    writer.writerow({'iteration': i,
                                     'entity_set_nr': entity_set,
                                     'entity_set_latency_milliseconds': entity_set_latency_milliseconds
                                     })
                    entity_set += 1
                i += 1

    def print_message_sizes(self, message_parts, send_time) -> None:
        """Print all message sizes to stdout."""
        print(f"Topic size: {len(message_parts[0])}")
        print(f"Timestamp: {send_time}")
        print(
            f"Entity size: {len(message_parts[1])}")
        print(
            f"Total message size: {len(message_parts[0] + message_parts[1])}")
