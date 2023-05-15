"""Middleware class to deliver data and cache between publisher and subscriber."""
import zmq


FRONTEND_SOCKET_ADDRESS = "tcp://*:5555"
BACKEND_SOCKET_ADDRESS = "tcp://*:5556"


class Middleware():
    """Middleware Class to deliever and cache data between publisher and subscriber."""

    def __init__(self) -> None:
        print("Starting Middleware")

        context = zmq.Context()

        # Cache for storing latest published values (LVC)
        cache = {}

        # Set up frontend (Publisher-facing) socket
        frontend = context.socket(zmq.SUB)
        frontend.bind(FRONTEND_SOCKET_ADDRESS)
        # Subscribe to all topic updates
        frontend.subscribe(b'')

        # Set up backend (Subscriber-facing) socket
        backend = context.socket(zmq.XPUB)
        backend.bind(BACKEND_SOCKET_ADDRESS)

        # Set up proxy polling
        poller = zmq.Poller()
        poller.register(frontend, zmq.POLLIN)
        poller.register(backend, zmq.POLLIN)

        # Main proxy loop
        try:
            while True:
                events = dict(poller.poll(500))

                # Forward publish events
                if frontend in events:
                    message = frontend.recv_multipart()

                    # Store messages in cache
                    topic, value = message
                    cache[topic] = value

                    backend.send_multipart(message)

                # Forward subscribe events
                if backend in events:
                    event = backend.recv()

                    # First byte in event is 1 for subscribe, 0 for unsubscribe
                    if event[0] == 1:
                        # Resend latest cached topic data for all matching topics
                        topic = event[1:]
                        for key in cache:
                            if key.startswith(topic):
                                backend.send_multipart([key, cache[key]])
        except KeyboardInterrupt:
            print("Exiting Middleware")
        finally:
            frontend.close()
            backend.close()
            context.term()


if __name__ == "__main__":
    Middleware()
