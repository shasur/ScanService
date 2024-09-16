import sys
from pathlib import Path

# Add the parent directory of 'src' to the Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.logger import get_logger
from src.utils.GS1AIHandler import GS1AIHandler

class DataParser:
    def __init__(self):
        self.logger = get_logger(__name__)
        self.gs1_handler = GS1AIHandler()

    def parse(self, data):
        try:
            # Replace bracketed ASCII representations with actual characters
            data = data.replace('<STX>', '\x02').replace('<GS>', '\x1D')
            data = data.replace('<CR>', '\r').replace('<LF>', '\n')

            if data.startswith('\x02') and data.endswith('\r\n'):
                # Remove STX, CR, and LF
                cleaned_data = data[1:-2]
                
                # Replace Group Separator with '#' for consistency
                cleaned_data = cleaned_data.replace('\x1D', '#')
                
                if cleaned_data == '\x18':
                    return {
                        'type': 'noread',
                        'data': 'NoRead'
                    }
                elif cleaned_data.startswith('HeartBeat'):
                    return {
                        'type': 'heartbeat',
                        'data': cleaned_data
                    }
                else:
                    parsed_ais = self.gs1_handler.parse_gs1_barcode(cleaned_data)
                    return {
                        'type': 'scan_result',
                        'data': cleaned_data,
                        'parsed': parsed_ais
                    }
            else:
                self.logger.warning(f"Received malformed data: {data}")
                return None
        except Exception as e:
            self.logger.error(f"Error parsing data: {e}")
            return None
