import json
import time
from datetime import datetime, timezone
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

    def publish_nbirth(self, startup_log="Service started without any error"):
        """Publish the NBIRTH message."""
        if not self.connected:
            self.logger.error("Cannot publish NBIRTH: Not connected to MQTT broker")
            return
        
        nbirth_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dataType": "String",
            "service_id": self.service_id,
            "scanner_config": {
                "ip": self.scanner_ip,
                "port": self.scanner_port
            },
            "metrics": [
                {
                    "name": "Node Control/Rebirth",
                    "value": self.service_started_correctly,
                    "type": "Boolean"
                },
                {
                    "name": "Node Control/SetIPAndPort",
                    "value": f"{{Parameters: {{IP: {self.scanner_ip}, Port: {self.scanner_port}}}, ClientId: {self.client_id}}}",
                    "type": "String"
                },
                {
                    "name": "Node Control/SetServiceSettings",
                    "value": json.dumps({
                        "Parameters": {
                            "StartCode": self.scanner_start_code,
                            "EndCode": self.scanner_end_code,
                            "HeartbeatFrequency": self.heartbeat_frequency,
                            "HeartbeatFormat": self.scanner_heartbeat_format,
                            "Setting1": "<value>",
                            "Setting2": "<value>"
                        },
                        "ClientId": "<requester id>"
                    }),
                    "type": "String"
                },
                {
                    "name": "LogInfo",
                    "value": startup_log,
                    "type": "String"
                }
            ],
            "seq": 0
        }
        
        self._publish_message("NBIRTH", nbirth_payload)

    def publish_ndeath(self):
        """Publish the NDEATH message."""
        if not self.connected:
            self.logger.error("Cannot publish NDEATH: Not connected to MQTT broker")
            return
        ndeath_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dataType": "String",
            "service_id": self.service_id,
            "metrics": [
                {"name": "bdSeq", "value": self.bd_seq, "type": "Int64"}
            ]
        }
        self._publish_message("NDEATH", ndeath_payload)

    def publish_connection(self, status: bool):
        """Publish the connection status."""
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dataType": "Boolean",
            "status": status
        }
        self._publish_message("Connection", payload)

    def publish_ping(self, status: bool):
        """Publish the ping status."""
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dataType": "Boolean",
            "status": status
        }
        self._publish_message("Ping", payload)

    def publish_keep_alive(self, heartbeat_text: str = None):
        """Publish the keep-alive status."""
        current_time = datetime.now(timezone.utc)
        if heartbeat_text:
            self.last_heartbeat_time = current_time
            payload = {
                "timestamp": self.last_heartbeat_time.isoformat(),
                "dataType": "String",
                "status": "active",
                "heartbeatText": heartbeat_text
            }
        else:
            payload = {
                "timestamp": current_time.isoformat(),
                "dataType": "String",
                "status": "timeout",
                "message": "No heartbeat received for 60 seconds"
            }
        self._publish_message("KeepAlive", payload)

    def publish_noread(self):
        """Publish a no-read event."""
        self.last_noread_time = datetime.now(timezone.utc)
        payload = {
            "timestamp": self.last_noread_time.isoformat(),
            "dataType": "String",
            "status": "noread"
        }
        self._publish_message("Noread", payload)

    def publish_scan_result(self, scan_data, parsed_ais):
        """Publish a scan result."""
        self.last_scan_time = datetime.now(timezone.utc)
        payload = {
            "timestamp": self.last_scan_time.isoformat(),
            "dataType": "String",
            "data": scan_data,
            "parsedAIs": parsed_ais if parsed_ais else "no AI"
        }
        self._publish_message("ScanResult", payload)

    def disconnect(self):
        """Disconnect from the MQTT broker."""
        self.client.loop_stop()
        self.client.disconnect()
        self.logger.info("Disconnected from MQTT Broker")
