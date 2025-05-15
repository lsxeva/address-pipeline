# src/processing/preprocess_utils.py

from collections import defaultdict
import metaphone
from typing import Dict, List, Any
import concurrent.futures
from functools import lru_cache

@lru_cache(maxsize=10000)
def clean(value: str) -> str:
    """
    Clean and normalize a string value with caching
    """
    if value is None:
        return ''
    value = str(value).strip().lower()
    # 处理常见的地址缩写
    replacements = {
        'street': 'st',
        'avenue': 'ave',
        'boulevard': 'blvd',
        'road': 'rd',
        'drive': 'dr',
        'lane': 'ln',
        'place': 'pl',
        'court': 'ct',
        'circle': 'cir',
        'terrace': 'ter',
        'parkway': 'pkwy',
        'highway': 'hwy',
        'north': 'n',
        'south': 's',
        'east': 'e',
        'west': 'w',
        'northeast': 'ne',
        'northwest': 'nw',
        'southeast': 'se',
        'southwest': 'sw'
    }
    for full, abbr in replacements.items():
        value = value.replace(full, abbr)
    return value

def make_normalized_key(house, street, strtype, city=None, state=None, apttype=None, aptnbr=None):
    """
    Create a normalized key from address components with improved handling.
    """
    parts = []
    
    # 处理门牌号
    if house:
        house = str(house).strip()
        # 处理范围格式 (例如: "123-125")
        if '-' in house:
            house = house.split('-')[0]  # 使用较小的数字
        parts.append(clean(house))
    
    # 处理街道名
    if street:
        parts.append(clean(street))
    
    # 处理街道类型
    if strtype:
        parts.append(clean(strtype))
    
    # 处理公寓信息
    if apttype and aptnbr:
        parts.append(clean(apttype))
        parts.append(clean(aptnbr))
    
    # 处理城市和州
    if city:
        parts.append(clean(city))
    if state:
        parts.append(clean(state))
    
    return " ".join(parts)

def build_normalized_index_extended(canonical_list: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Build an extended normalized index with parallel processing
    """
    index = {}
    
    def process_record(record):
        try:
            # Pre-calculate cleaned values
            house = clean(record.get("house", ""))
            street = clean(record.get("street", ""))
            strtype = clean(record.get("strtype", ""))
            city = clean(record.get("city", ""))
            state = clean(record.get("state", ""))
            
            # Build keys
            keys = [
                f"{house} {street} {strtype}",
                f"{house} {street} {strtype} {city} {state}"
            ]
            return [(key, record) for key in keys if key.strip()]
        except Exception as e:
            print(f"Warning: Skipping record due to error: {e}")
            return []

    # Use parallel processing for index building
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(process_record, canonical_list))
        
    # Build index from results
    for key_records in results:
        for key, record in key_records:
            index[key] = record
            
    return index

def build_prefix_index(canonical_addresses: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Build an optimized blocking index using street prefix and street type
    Args:
        canonical_addresses: List of canonical address dictionaries
    Returns:
        Dictionary with blocking keys mapping to lists of addresses
    """
    index = defaultdict(list)
    
    for addr in canonical_addresses:
        street = clean(addr.get('street', ''))
        strtype = clean(addr.get('strtype', ''))
        
        if street:
            # 1. Street prefix blocking (first 3 chars)
            prefix = street[:3]
            index[prefix].append(addr)
            
            # 2. Street prefix + type blocking
            if strtype:
                combined_key = f"{prefix}_{strtype}"
                index[combined_key].append(addr)
            
            # 3. Street prefix (first 4 chars) for longer streets
            if len(street) > 3:
                prefix4 = street[:4]
                index[prefix4].append(addr)
    
    return index

def build_metaphone_index(canonical_addresses: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Build an optimized Metaphone index
    """
    index = defaultdict(list)
    
    # Pre-calculate address strings and their Metaphone encodings
    for addr in canonical_addresses:
        address_str = f"{clean(addr.get('house', ''))} {clean(addr.get('street', ''))} {clean(addr.get('strtype', ''))}"
        if address_str.strip():
            metaphone_key = metaphone.dm(address_str)[0]
            if metaphone_key:
                index[metaphone_key].append(addr)
                
    return index