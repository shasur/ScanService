import sys
import asyncio
import atexit
import signal
from pathlib import Path

# Add the parent directory of 'src' to the Python path
sys.path.append(str(Path(__file__).parent.parent))

# Now import your modules
from src.config.config import load_config
from src.utils.logger import setup_logger
from src.scanner.scanner_client import ScannerClient
from src.mqtt.mqtt_publisher import MqttPublisher

# Global variables
mqtt_publisher = None
scanner_client = None
logger = None

def cleanup():
    """Synchronous cleanup function to be called on exit"""
    global mqtt_publisher, scanner_client, logger
    if logger:
        logger.info("Cleaning up before exit...")
    if scanner_client:
        scanner_client.disconnect_sync()
    if mqtt_publisher:
        mqtt_publisher.publish_ndeath()
        # Add a small delay to allow the NDEATH message to be sent
        asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.5))
        mqtt_publisher.disconnect()
    if logger:
        logger.info("Cleanup completed. Exiting.")

def signal_handler(signum, frame):
    """Signal handler for graceful shutdown"""
    global logger
    if logger:
        logger.info(f"Received signal {signum}. Initiating shutdown...")
    cleanup()
    exit(0)

async def main():
    global mqtt_publisher, scanner_client, logger

    # Load configuration
    config = load_config()

    # Set up logger
    logger = setup_logger()

    logger.info("ScanService starting...")

    # Initialize components
    mqtt_publisher = MqttPublisher(config)
    mqtt_publisher.connect()

    # Wait for MQTT connection to be established
    for _ in range(10):  # Try for 5 seconds
        if mqtt_publisher.connected:
            break
        await asyncio.sleep(0.5)

    if mqtt_publisher.connected:
        # If everything started correctly
        mqtt_publisher.publish_nbirth()
    else:
        logger.error("Failed to connect to MQTT broker. Exiting.")
        return

    scanner_client = ScannerClient(config['scanner'], mqtt_publisher)

    # Start components
    scanner_task = asyncio.create_task(scanner_client.start())

    logger.info("ScanService running. Press Ctrl+C to exit.")
    try:
        # Keep the main coroutine running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt. Initiating shutdown...")
    finally:
        logger.info("ScanService shutting down...")
        await scanner_client.stop()
        scanner_task.cancel()
        try:
            await scanner_task
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    # Register the cleanup function to be called on normal program exit
    atexit.register(cleanup)

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run the main async function
    asyncio.run(main())
