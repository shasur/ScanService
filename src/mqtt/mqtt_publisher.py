import json
import time
from datetime import datetime, timezone
import socket
import os
from paho.mqtt import client as mqtt_client
from utils.logger import get_logger

class MqttPublisher:
    def __init__(self, config):
        # Extract MQTT and scanner configurations
        mqtt_config = config['mqtt']
        scanner_config = config['scanner']
        
        # Set up MQTT client parameters
        self.broker = mqtt_config['broker']
        self.port = mqtt_config['port']
        self.base_topic = mqtt_config['topic']
        self.service_name = "ScannerService"
        self.scanner_ip = scanner_config['ip']
        self.scanner_port = scanner_config['port']
        self.client_id = f"ScannerService_{self.service_name}"
        
        # Initialize MQTT client
        self.client = mqtt_client.Client(client_id=self.client_id, protocol=mqtt_client.MQTTv5)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        
        # Set up logger
        self.logger = get_logger(__name__)
        
        # Initialize other attributes
        self.bd_seq = 0
        self.connected = False
        self.service_id = f"ScannerService_{self.service_name}"
        self.scanner_start_code = '\x02'  # STX
        self.scanner_end_code = '\x03'  # ETX
        self.heartbeat_frequency = 60  # Heartbeat frequency in seconds
        self.scanner_heartbeat_format = "HeartBeat"
        self.service_started_correctly = True  # Set this based on your startup logic
        self.last_heartbeat_time = None
        self.last_noread_time = None
        self.last_scan_time = None
        self.topic_prefix = mqtt_config['topic']
        self.sequence_numbers = {}  # Dictionary to store sequence numbers for each topic

        # Remove the Docker client initialization
        # self.docker_client = docker.from_env()

        # Instead, get Docker information from environment variables
        self.image_id = os.environ.get('DOCKER_IMAGE_ID', '')
        self.image_name = os.environ.get('DOCKER_IMAGE_NAME', '')
        self.image_tag = os.environ.get('DOCKER_IMAGE_TAG', '')
        self.container_id = os.environ.get('HOSTNAME', '')
        self.container_name = os.environ.get('DOCKER_CONTAINER_NAME', '')
        self.container_port = os.environ.get('DOCKER_CONTAINER_PORT', '')

    def on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback for when the client receives a CONNACK response from the server."""
        if rc == 0:
            self.logger.info("Connected to MQTT Broker!")
            self.connected = True
        else:
            self.logger.error(f"Failed to connect to MQTT Broker, return code {rc}")

    def on_disconnect(self, client, userdata, rc, properties=None):
        """Callback for when the client disconnects from the server."""
        if rc != 0:
            self.logger.warning(f"Unexpected disconnection from MQTT Broker. RC: {rc}")
        else:
            self.logger.info("Disconnected from MQTT Broker")
        self.connected = False

    def connect(self):
        """Establish connection to the MQTT broker."""
        try:
            self.logger.info(f"Attempting to connect to MQTT broker at {self.broker}:{self.port}")
            self.client.connect(self.broker, self.port)
            self.client.loop_start()
            self.logger.info("MQTT client loop started")
        except Exception as e:
            self.logger.error(f"Error connecting to MQTT broker: {e}")
            self.logger.error(f"Broker: {self.broker}, Port: {self.port}")
            self.logger.error(f"Client ID: {self.client_id}")
            self.logger.exception("Full exception traceback:")
            raise  # Re-raise the exception to be handled by the caller

    def _get_next_sequence_number(self, topic):
        """Get the next sequence number for a given topic."""
        if topic not in self.sequence_numbers:
            self.sequence_numbers[topic] = 0
        else:
            self.sequence_numbers[topic] = (self.sequence_numbers[topic] + 1) % 1001
        return self.sequence_numbers[topic]

    def _publish_message(self, suffix: str, payload: dict):
        """Publish a message to a specific topic with a sequence number."""
        topic = f"{self.base_topic}/{suffix}"
        seq_no = self._get_next_sequence_number(topic)
        payload['seqNo'] = seq_no
        try:
            result = self.client.publish(topic, json.dumps(payload))
            if result[0] == 0:
                self.logger.debug(f"Published {suffix} message to {topic}: {payload}")
            else:
                self.logger.error(f"Failed to publish {suffix} message to {topic}, result code: {result[0]}")
        except Exception as e:
            self.logger.error(f"Error publishing {suffix} message to {topic}: {e}")

    def publish_ndeath(self, status):
        """Publish the NDEATH message."""
        payload = {
            "timestamp": int(time.time() * 1000),
            "status": status
        }
        self._publish_message("NDEATH", payload)

    def publish_dbirth(self):
        """Publish the DBIRTH message."""
        payload = {
            "timestamp": int(time.time() * 1000),
            "ipaddress": socket.gethostbyname(socket.gethostname()),
            "port": self.port
        }
        self._publish_message("DBIRTH", payload)

    def publish_nbirth(self):
        """Publish the NBIRTH message."""
        payload = {
            "Service": [
                {
                    "timestamp": int(time.time() * 1000),
                    "ComputerName": socket.gethostname(),
                    "UserDomainName": os.environ.get('USERDOMAIN', 'N/A'),
                    "ExePath": os.path.dirname(os.path.abspath(__file__)),
                    "ProjectPath": os.getcwd(),
                    "ServiceVersion": "1.0.0",  # Update this with your actual version
                    "ConfigVersion": "1.0.0"  # Update this with your actual config version
                }
            ],
            "Docker": [
                {
                    "Image": [
                        {
                            "ID": self.image_id,
                            "Name": self.image_name,
                            "Tag (version)": self.image_tag
                        }
                    ],
                    "Container": [
                        {
                            "ID": self.container_id,
                            "Name": self.container_name,
                            "Port": self.container_port
                        }
                    ]
                }
            ]
        }
        self._publish_message("NBIRTH", payload)

    def publish_ddata(self, scan_data, parsed_ais=None, is_noread=False):
        """Publish a scan result or noread event."""
        payload = {
            "timestamp": int(time.time() * 1000),
            "dataType": "String",
            "data": scan_data if not is_noread else "NOREAD",
            "parsedAIs": parsed_ais if parsed_ais else "no AI",
            "isNoRead": is_noread
        }
        self._publish_message("DDATA", payload)

    def publish_dstatus(self, is_connected, is_port_open, is_heartbeat_ok):
        """Publish the DSTATUS message."""
        status = "OK" if is_connected and is_port_open and is_heartbeat_ok else "NOK"
        description = self._get_status_description(is_connected, is_port_open, is_heartbeat_ok)
        
        payload = {
            "Status": [
                {
                    "timestamp": int(time.time() * 1000),
                    "Connected": str(is_connected).lower(),
                    "State": status,
                    "Description": description
                }
            ],
            "Connection": [
                {
                    "IP": str(is_connected).lower(),
                    "Port": str(is_port_open).lower(),
                    "HeartBeat": str(is_heartbeat_ok).lower()
                }
            ]
        }
        self._publish_message("DSTATUS", payload)

    def _get_status_description(self, is_connected, is_port_open, is_heartbeat_ok):
        if is_connected and is_port_open and is_heartbeat_ok:
            return "Connected"
        elif is_connected and not is_port_open:
            return "Network reachable, service port inaccessible"
        elif not is_connected:
            return "Port/endpoint unreachable"
        elif not is_heartbeat_ok:
            return "Missing Heartbeat"

    def disconnect(self):
        """Disconnect from the MQTT broker."""
        self.client.loop_stop()
        self.client.disconnect()
        self.logger.info("Disconnected from MQTT Broker")

    def test_dstatus_publishing(self):
        """Test method to verify DSTATUS publishing"""
        test_cases = [
            (True, True, True),   # All OK
            (True, False, True),  # IP reachable, port closed, heartbeat OK
            (True, False, False), # IP reachable, port closed, no heartbeat
            (False, False, False) # Nothing reachable
        ]
        
        for is_connected, is_port_open, is_heartbeat_ok in test_cases:
            self.logger.info(f"Testing DSTATUS with IP: {is_connected}, Port: {is_port_open}, Heartbeat: {is_heartbeat_ok}")
            self.publish_dstatus(is_connected, is_port_open, is_heartbeat_ok)