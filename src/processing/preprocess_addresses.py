import pandas as pd
import usaddress
import logging
from typing import Dict, Any
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressPreprocessor:
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
        Normalize address string, remove 'UNIT' but keep all other unit info.
        """
        if not address:
            return ""
        # Convert to uppercase and remove extra spaces
        address = " ".join(address.upper().split())
        # Remove the word 'UNIT' but keep the number/letter after it
        address = address.replace("UNIT ", "")
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

def main():
    print("\n=== Starting Address Preprocessing ===")
    preprocessor = AddressPreprocessor()
    # Load transactions
    print("\nLoading transactions...")
    transactions = pd.read_csv("data/raw/transactions_2_11211.csv")
    print(f"Loaded {len(transactions)} transactions")
    # Process addresses
    print("\nProcessing addresses...")
    processed_data = []
    for idx, row in tqdm(transactions.iterrows(), total=len(transactions), desc="Processing addresses"):
        # Combine address lines
        address = str(row['address_line_1'])
        if pd.notna(row['address_line_2']):
            address = f"{address} {row['address_line_2']}"
        # Parse and normalize
        parsed = preprocessor.parse_address(address)
        normalized = preprocessor.normalize_address(address)
        # Store results
        processed_data.append({
            'transaction_id': row['id'],
            'original_address': address,
            'normalized_address': normalized,  # Only 'UNIT' removed, others kept
            'house': parsed.get('house', ''),
            'street': parsed.get('street', ''),
            'strtype': parsed.get('strtype', ''),
            'apttype': parsed.get('apttype', ''),
            'aptnbr': parsed.get('aptnbr', ''),
            'city': parsed.get('city', ''),
            'state': parsed.get('state', ''),
            'zip': parsed.get('zip', '')
        })
    # Convert to DataFrame and save
    processed_df = pd.DataFrame(processed_data)
    output_path = "data/processed/processed_transactions.csv"
    processed_df.to_csv(output_path, index=False)
    # Print sample of processed data
    print("\n=== Sample of Processed Data ===")
    print(processed_df.head().to_string())
    print(f"\nProcessed data saved to: {output_path}")

if __name__ == "__main__":
    main()