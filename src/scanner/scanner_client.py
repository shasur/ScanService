import asyncio
import sys
import socket
from pathlib import Path

# Add the parent directory of 'src' to the Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.scanner.data_parser import DataParser
from src.utils.logger import get_logger

class ScannerClient:
    def __init__(self, config, mqtt_publisher):
        # Initialize scanner configuration
        self.ip = config['ip']
        self.port = config['port']
        self.logger = get_logger(__name__)
        self.reader = None
        self.writer = None
        self.parser = DataParser()
        self.is_connected = False
        self.mqtt_publisher = mqtt_publisher
        self.last_activity_time = 0
        self.ping_interval = 5  # Ping every 5 seconds
        self.last_ping_status = None

    async def connect(self):
        """Establish connection to the scanner."""
        try:
            self.reader, self.writer = await asyncio.open_connection(self.ip, self.port)
            self.is_connected = True
            self.last_activity_time = asyncio.get_event_loop().time()
            self.logger.info(f"Connected to scanner at {self.ip}:{self.port}")
            self.mqtt_publisher.publish_connection(True)
        except Exception as e:
            self.logger.error(f"Failed to connect to scanner: {e}")
            self.is_connected = False
            self.mqtt_publisher.publish_connection(False)

    async def ping(self):
        """Ping the scanner's TCP/IP server."""
        try:
            # Create a socket object
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Set a timeout for the connection attempt
            sock.settimeout(2.0)
            # Attempt to connect to the scanner's IP and port
            result = sock.connect_ex((self.ip, self.port))
            sock.close()

            # If the result is 0, the connection was successful
            is_connected = (result == 0)

            # Only publish if the status has changed
            if is_connected != self.last_ping_status:
                self.mqtt_publisher.publish_ping(is_connected)
                self.last_ping_status = is_connected

            return is_connected
        except Exception as e:
            self.logger.error(f"Error pinging scanner: {e}")
            if self.last_ping_status is not False:
                self.mqtt_publisher.publish_ping(False)
                self.last_ping_status = False
            return False

    async def ping_loop(self):
        """Continuously ping the scanner at set intervals."""
        while self.is_connected:
            if not await self.ping():
                self.logger.warning("Failed to ping scanner, reconnecting...")
                self.is_connected = False
                break
            await asyncio.sleep(self.ping_interval)

    async def read_data(self):
        """Read and parse data from the scanner."""
        buffer = b''
        try:
            while self.is_connected:
                try:
                    # Read data from the scanner
                    chunk = await asyncio.wait_for(self.reader.read(1024), timeout=60)
                    if not chunk:
                        raise asyncio.IncompleteReadError(buffer, None)
                    
                    buffer += chunk
                    self.last_activity_time = asyncio.get_event_loop().time()

                    # Process complete messages in the buffer
                    while b'\x02' in buffer and b'\r\n' in buffer:
                        start = buffer.index(b'\x02')
                        end = buffer.index(b'\r\n', start) + 2
                        message = buffer[start:end].decode('ascii', errors='replace')
                        buffer = buffer[end:]

                        # Parse the message
                        parsed_data = self.parser.parse(message)
                        if parsed_data:
                            self.logger.info(f"Parsed data: {parsed_data}")
                            try:
                                # Publish different types of messages
                                if parsed_data['type'] == 'scan_result':
                                    self.mqtt_publisher.publish_scan_result(parsed_data['data'], parsed_data.get('parsed', []))
                                    self.logger.info(f"Published scan result: {parsed_data['data']}")
                                elif parsed_data['type'] == 'noread':
                                    self.mqtt_publisher.publish_noread()
                                    self.logger.info("Published NoRead")
                                elif parsed_data['type'] == 'heartbeat':
                                    self.mqtt_publisher.publish_keep_alive(parsed_data['data'])
                                    self.logger.info(f"Published KeepAlive: {parsed_data['data']}")
                            except Exception as e:
                                self.logger.error(f"Error publishing data: {e}")
                        else:
                            self.logger.warning(f"Failed to parse data: {message}")

                except asyncio.TimeoutError:
                    self.logger.warning("No data received for 60 seconds.")
                    self.mqtt_publisher.publish_keep_alive()  # Publish timeout on KeepAlive topic
                    self.is_connected = False
                    continue  # Continue the loop to attempt reading again

        except asyncio.IncompleteReadError:
            self.logger.error("Connection closed by the scanner.")
            self.is_connected = False
            self.mqtt_publisher.publish_connection(False)
        except Exception as e:
            self.logger.error(f"Error reading data: {e}")
            self.is_connected = False
            self.mqtt_publisher.publish_connection(False)
        finally:
            self.mqtt_publisher.publish_connection(False)

    async def start(self):
        """Start the scanner client."""
        while True:
            if not self.is_connected:
                await self.connect()
            if self.is_connected:
                ping_task = asyncio.create_task(self.ping_loop())
                read_task = asyncio.create_task(self.read_data())
                done, pending = await asyncio.wait(
                    [ping_task, read_task],
                    return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                if read_task in done:
                    # If read_data completes, it means we lost connection
                    self.is_connected = False
            await asyncio.sleep(1)  # Short delay before attempting to reconnect

    async def stop(self):
        """Stop the scanner client and close the connection."""
        self.is_connected = False
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.logger.info("Disconnected from scanner")
            self.mqtt_publisher.publish_connection(False)

    def disconnect_sync(self):
        """Synchronous method to disconnect from the scanner."""
        self.is_connected = False
        if self.writer:
            self.writer.close()
            self.logger.info("Disconnected from scanner")
            self.mqtt_publisher.publish_connection(False)
