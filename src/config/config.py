import os
import yaml

def load_config():
    with open("config.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)

    # Override with environment variables if they exist
    config['scanner']['ip'] = os.getenv('SCANNER_IP', config['scanner']['ip'])
    config['scanner']['port'] = int(os.getenv('SCANNER_PORT', config['scanner']['port']))
    config['mqtt']['broker'] = os.getenv('MQTT_BROKER', config['mqtt']['broker'])
    config['mqtt']['port'] = int(os.getenv('MQTT_PORT', config['mqtt']['port']))
    config['mqtt']['group_id'] = os.getenv('MQTT_GROUP_ID', config['mqtt'].get('group_id', 'thetoplevel/codeit'))
    config['mqtt']['edge_node_id_device_id'] = os.getenv('MQTT_EDGE_NODE_ID_DEVICE_ID', config['mqtt'].get('edge_node_id_device_id', ''))

    return config
