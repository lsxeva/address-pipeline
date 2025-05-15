import usaddress
import pandas as pd
import logging
from typing import Dict, Any, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressParser:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Map usaddress fields to our database fields
        self.field_mapping = {
            'AddressNumber': 'house',
            'StreetName': 'street',
            'StreetNamePostType': 'strtype',
            'OccupancyType': 'apttype',
            'OccupancyIdentifier': 'aptnbr',
            'PlaceName': 'city',
            'StateName': 'state',
            'ZipCode': 'zip'
        }

    def parse_address(self, address: str) -> Dict[str, Any]:
        """
        Parse a raw address string into components using usaddress.
        
        Args:
            address (str): Raw address string
            
        Returns:
            Dict[str, Any]: Dictionary containing parsed address components
        """
        try:
            tagged_address, _ = usaddress.tag(address)
            # Map the fields to our database schema
            mapped_address = {}
            for us_field, db_field in self.field_mapping.items():
                if us_field in tagged_address:
                    mapped_address[db_field] = tagged_address[us_field]
            return mapped_address
        except usaddress.RepeatedLabelError:
            self.logger.warning(f"Repeated label in address: {address}")
            return {}
        except Exception as e:
            self.logger.error(f"Error parsing address {address}: {str(e)}")
            return {}

    def normalize_address(self, address: str) -> str:
        """
        Normalize address string by:
        1. Converting to uppercase
        2. Removing extra spaces
        3. Standardizing common abbreviations
        
        Args:
            address (str): Raw address string
            
        Returns:
            str: Normalized address string
        """
        if not address:
            return ""
            
        # Convert to uppercase and remove extra spaces
        address = " ".join(address.upper().split())
        
        # Standardize common abbreviations
        replacements = {
            "STREET": "ST",
            "AVENUE": "AVE",
            "BOULEVARD": "BLVD",
            "ROAD": "RD",
            "DRIVE": "DR",
            "LANE": "LN",
            "PLACE": "PL",
            "COURT": "CT",
            "CIRCLE": "CIR",
            "SUITE": "STE",
            "APARTMENT": "APT",
            "FLOOR": "FL",
            "NORTH": "N",
            "SOUTH": "S",
            "EAST": "E",
            "WEST": "W"
        }
        
        for old, new in replacements.items():
            address = address.replace(old, new)
            
        return address

    def parse_and_normalize(self, address: str) -> Tuple[Dict[str, Any], str]:
        """
        Parse and normalize an address string.
        
        Args:
            address (str): Raw address string
            
        Returns:
            Tuple[Dict[str, Any], str]: Parsed components and normalized address
        """
        parsed = self.parse_address(address)
        normalized = self.normalize_address(address)
        return parsed, normalized
