import asyncio
import sys
import socket
import time
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
        self.is_port_open = False
        self.is_heartbeat_ok = False
        self.last_heartbeat_time = 0
        self.heartbeat_timeout = 60  # Consider heartbeat missing after 60 seconds

    async def connect(self):
        """Establish connection to the scanner."""
        try:
            self.reader, self.writer = await asyncio.open_connection(self.ip, self.port)
            self.is_connected = True
            self.is_port_open = True
            self.last_activity_time = asyncio.get_event_loop().time()
            self.logger.info(f"Connected to scanner at {self.ip}:{self.port}")
            await self.update_status()
        except Exception as e:
            self.logger.error(f"Failed to connect to scanner: {e}")
            self.is_connected = False
            self.is_port_open = False
            await self.update_status()

    async def ping(self):
        """Ping the scanner's TCP/IP server."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            result = sock.connect_ex((self.ip, self.port))
            sock.close()

            is_connected = (result == 0)
            if is_connected != self.is_connected:
                self.is_connected = is_connected
                await self.update_status()

            return is_connected
        except Exception as e:
            self.logger.error(f"Error pinging scanner: {e}")
            self.is_connected = False
            await self.update_status()
            return False

    async def update_status(self):
        """Update and publish the current status."""
        current_time = time.time()
        self.is_heartbeat_ok = (current_time - self.last_heartbeat_time) < self.heartbeat_timeout
        self.mqtt_publisher.publish_dstatus(self.is_connected, self.is_port_open, self.is_heartbeat_ok)

    async def ping_loop(self):
        """Continuously ping the scanner at set intervals."""
        while self.is_connected:
            if not await self.ping():
                self.logger.warning("Failed to ping scanner, reconnecting...")
                self.is_connected = False
                self.is_port_open = False
                await self.update_status()
                break
            await asyncio.sleep(self.ping_interval)

    async def read_data(self):
        """Read and parse data from the scanner."""
        buffer = b''
        try:
            while self.is_connected:
                try:
                    chunk = await asyncio.wait_for(self.reader.read(1024), timeout=60)
                    if not chunk:
                        raise asyncio.IncompleteReadError(buffer, None)
                    
                    buffer += chunk
                    self.last_activity_time = asyncio.get_event_loop().time()

                    while b'\x02' in buffer and b'\r\n' in buffer:
                        start = buffer.index(b'\x02')
                        end = buffer.index(b'\r\n', start) + 2
                        message = buffer[start:end].decode('ascii', errors='replace')
                        buffer = buffer[end:]

                        parsed_data = self.parser.parse(message)
                        if parsed_data:
                            self.logger.info(f"Parsed data: {parsed_data}")
                            try:
                                if parsed_data['type'] == 'scan_result':
                                    self.mqtt_publisher.publish_ddata(parsed_data['data'], parsed_data.get('parsed', []))
                                    self.logger.info(f"Published scan result: {parsed_data['data']}")
                                elif parsed_data['type'] == 'noread':
                                    self.mqtt_publisher.publish_ddata(None, is_noread=True)
                                    self.logger.info("Published NoRead")
                                elif parsed_data['type'] == 'heartbeat':
                                    self.last_heartbeat_time = time.time()
                                    self.is_heartbeat_ok = True
                                    await self.update_status()
                                    self.logger.info(f"Received HeartBeat: {parsed_data['data']}")
                            except Exception as e:
                                self.logger.error(f"Error publishing data: {e}")
                        else:
                            self.logger.warning(f"Failed to parse data: {message}")

                except asyncio.TimeoutError:
                    self.logger.warning("No data received for 60 seconds.")
                    self.is_heartbeat_ok = False
                    await self.update_status()
                    continue

        except asyncio.IncompleteReadError:
            self.logger.error("Connection closed by the scanner.")
            self.is_connected = False
            self.is_port_open = False
        except Exception as e:
            self.logger.error(f"Error reading data: {e}")
            self.is_connected = False
            self.is_port_open = False
        finally:
            await self.update_status()

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
                    self.is_connected = False
            await asyncio.sleep(1)  # Short delay before attempting to reconnect

    async def stop(self):
        """Stop the scanner client and close the connection."""
        self.is_connected = False
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.logger.info("Disconnected from scanner")
            self.mqtt_publisher.publish_ndeath("OFFLINE")

    def disconnect_sync(self):
        """Synchronous method to disconnect from the scanner."""
        self.is_connected = False
        if self.writer:
            self.writer.close()
            self.logger.info("Disconnected from scanner")
            self.mqtt_publisher.publish_ndeath("OFFLINE")