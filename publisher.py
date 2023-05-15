""" The Publisher base class for testing single-topic versus sub-topic publish-subscribe.
Functions are sorted in the order they are first called.
"""
import time
import csv
from datetime import datetime, timedelta
import zmq
from settings import *


def busy_wait(time_in_microseconds):
    """"Busy wait for time_in_microseconds.
    Provides higher precision waiting compared to time.sleep()."""
    target_time = time.perf_counter_ns() + time_in_microseconds * 1000
    while time.perf_counter_ns() < target_time:
        pass


class Publisher():
    """Base class for Publisher."""

    def __init__(self, test_type: str) -> None:
        test_start_time = datetime.now()

        print(f"Starting {test_type.lower()}-topic Publisher")
        self.test_type = test_type
        self.pub_file_path = get_test_filename("PUB_" + self.test_type)
        self.is_single = test_type == "SINGLE"

        self.field_names = [
            # Iteration number (to mark which data applies to which iteration)
            'iteration',
            # (end_time - start_time) / 1000
            'duration_milliseconds',
            # Entity sets per iteration
            'entity_sets_sent',
            # self.current_frequency
            'programmed_frequency',
            # ITERATION_ENTITY_SETS / (end_time - start_time)
            'actual_frequency',
            # (get_entity_set_size() / 1000) * self.current_frequency
            'programmed_throughput_kB/s',
            # (ITERATION_ENTITY_SETS * get_entity_set_size()) / (end_time - start_time)
            'actual_throughput_kB/s',
            # sum((iteration_end - iteration_start).microseconds - get_publish_rate_in_microseconds()) / 1000
            'drift_microseconds'
        ]

        self.current_frequency = ENTITY_SET_FREQUENCY
        self.current_drift = 0

        self.results = []

        # ZMQ context
        self.context = zmq.Context()

        self.open_publisher()
        while self.get_throughput_ratio() > THROUGHPUT_RATIO_CUTOFF:
            self.run_test_iteration()
            self.current_frequency += ENTITY_SET_FREQUENCY_STEP
        self.close_publisher()
        self.record_results()

        test_end_time = datetime.now()
        print(f"Total test time: {test_end_time - test_start_time}")

    def open_publisher(self) -> None:
        """Open the publisher socket."""
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect(PUB_SOCKET_ADDRESS)

    def get_throughput_ratio(self) -> float:
        """Return the ratio in procentages between the actual
        and programmed throughput during last iteration.
        Return 1 if first iteration."""
        if len(self.results) > 0:
            actual_throughput_last_iteration = float(
                tuple(self.results[-1])[1])
            programmed_throughput_last_iteration = (
                get_entity_set_size(self.is_single) / 1000) * self.results[-1][4]
            return actual_throughput_last_iteration / programmed_throughput_last_iteration

        return 1

    def run_test_iteration(self) -> None:
        """Run a test iteration."""
        # Wait a bit before starting
        time.sleep(1)

        # Publish
        start_time = datetime.now()
        iter_delta = timedelta(
            microseconds=self.get_publish_rate_in_microseconds())
        for _ in range(ITERATION_ENTITY_SETS):
            iter_start = datetime.now()

            self.publish_entity_set()

            self.current_drift += iter_delta.microseconds - \
                self.get_publish_rate_in_microseconds()
            wait_time = self.get_publish_rate_in_microseconds() - self.current_drift
            if wait_time > 0:
                busy_wait(wait_time)

            iter_delta = datetime.now() - iter_start

        end_time = datetime.now()
        self.results.append(
            self.calculate_delta_time_throughput_and_drift(start_time, end_time))
        self.reset_for_iteration()

    def get_publish_rate_in_microseconds(self) -> float:
        """Return current publish rate in microseconds."""
        return (1 / self.current_frequency) * 1000000

    def publish_entity_set(self) -> None:
        """Publish one entity set. Should be overridden by inheriting subclasses."""
        raise NotImplementedError("Not implemented in base class.")

    def calculate_delta_time_throughput_and_drift(self,
                                                  start_time: datetime,
                                                  end_time: datetime) -> tuple:
        """Calculate all iteration results."""

        # Delta time calculations
        delta_time = end_time - start_time
        delta_time_in_microseconds = delta_time.seconds * 1000000 + delta_time.microseconds
        delta_time_in_milliseconds = delta_time_in_microseconds / 1000

        # Throughput calculations
        actual_throughput = ((ITERATION_ENTITY_SETS * get_entity_set_size(self.is_single)) /
                             delta_time_in_microseconds) * 1000

        return (delta_time_in_milliseconds,  # 0
                actual_throughput,  # 1
                self.current_drift,  # 2
                self.get_programmed_throughput(),  # 3
                self.current_frequency  # 4
                )

    def get_programmed_throughput(self) -> float:
        """Return the current throughput."""
        return (get_entity_set_size(self.is_single) / 1000) * self.current_frequency

    def reset_for_iteration(self) -> None:
        """Reset all values relevant to an iteration back to default."""
        self.current_drift = 0

    def close_publisher(self) -> None:
        """Close publisher socket."""
        self.pub_socket.close()
        self.context.term()

    def record_results(self) -> None:
        """Record test results to chosen output."""
        if PRINT_RESULTS:
            self.print_results()
        else:
            self.write_results_to_log_file()

    def print_results(self) -> None:
        """Print all results to stdout."""
        i = 1
        for result in self.results:
            print(f"-------- Iteration {i} ------------")
            print(
                f"Published {ITERATION_ENTITY_SETS} entity sets in {result[0]} milliseconds.\n")
            print(
                f"Programmed throughput: {result[3]} kB/s\n")
            print(f"Actual throughput: {result[1]} kB/s\n")
            print(f"Drift after timing compensation: {result[2]} µs\n")
            i += 1

    def write_results_to_log_file(self) -> None:
        """Write all information to given log file."""
        with open(self.pub_file_path, "w", newline="", encoding="utf-8") as csvfile:

            writer = csv.DictWriter(csvfile, fieldnames=self.field_names)
            writer.writeheader()
            i = 1
            for result in self.results:
                result = tuple(result)
                writer.writerow({'iteration': i,
                                'duration_milliseconds': result[0],
                                 'entity_sets_sent': ITERATION_ENTITY_SETS,
                                 'programmed_frequency': result[4],
                                 'actual_frequency': self.calculate_actual_frequency(result[0]),
                                 'programmed_throughput_kB/s': result[3],
                                 'actual_throughput_kB/s': result[1],
                                 'drift_microseconds': result[2]
                                 })
                i += 1

    def calculate_actual_frequency(self, duration: float) -> float:
        """Calculate the actual entity set frequency."""
        return (1000 * ITERATION_ENTITY_SETS) / duration
