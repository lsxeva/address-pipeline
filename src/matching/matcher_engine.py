# src/matching/matcher_engine.py

from typing import Dict, Any, List, Optional, Tuple
from rapidfuzz import fuzz, process
from src.processing.preprocess_utils import make_normalized_key, clean
from collections import defaultdict

def exact_match_dict(transaction: Dict[str, Any], canonical_addresses: List[Dict[str, Any]], 
                    normalized_index: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Perform exact match lookup using component-wise comparison and index
    """
    # Try index-based lookup first
    normalized_address = clean(transaction.get('normalized_address', ''))
    if normalized_address:
        match = normalized_index.get(normalized_address)
        if match:
            return match

    # Then try component-wise exact match with street prefix filtering
    street_prefix = clean(transaction['street'])[:3]
    candidates = [addr for addr in canonical_addresses 
                 if clean(addr.get('street', ''))[:3] == street_prefix]
    
    for canon_addr in candidates:
        canon = {
            'house': clean(canon_addr.get('house')),
            'street': clean(canon_addr.get('street')),
            'strtype': clean(canon_addr.get('strtype')),
            'apttype': clean(canon_addr.get('apttype')),
            'aptnbr': clean(canon_addr.get('aptnbr')),
            'city': clean(canon_addr.get('city')),
            'state': clean(canon_addr.get('state'))
        }

        if (clean(transaction['house']) == canon['house'] and
            clean(transaction['street']) == canon['street'] and
            clean(transaction['strtype']) == canon['strtype'] and
            clean(transaction['city']) == canon['city'] and
            clean(transaction['state']) == canon['state'] and
            clean(transaction['apttype']) == canon['apttype'] and
            clean(transaction['aptnbr']) == canon['aptnbr']):
            return canon_addr

    return None

def building_match(transaction: Dict[str, Any], canonical_addresses: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Match addresses ignoring apartment information
    """
    # Filter candidates by street prefix for efficiency
    street_prefix = clean(transaction['street'])[:3]
    candidates = [addr for addr in canonical_addresses 
                 if clean(addr.get('street', ''))[:3] == street_prefix]
    
    for addr in candidates:
        if (clean(addr.get('house')) == clean(transaction['house']) and
            clean(addr.get('street')) == clean(transaction['street']) and
            clean(addr.get('strtype')) == clean(transaction['strtype']) and
            clean(addr.get('city')) == clean(transaction['city']) and
            clean(addr.get('state')) == clean(transaction['state'])):
            return addr
    return None

def fuzzy_match_block(normalized_address: str, street: str, prefix_index: Dict[str, List[Dict[str, Any]]], 
                     threshold: float = 0.85) -> Optional[Tuple[Dict[str, Any], float]]:
    """
    Perform fuzzy matching using street prefix and type blocking
    Args:
        normalized_address: Normalized address string to match
        prefix_index: Dictionary containing blocking indexes
        threshold: Minimum similarity score to consider a match (default: 0.85)
    Returns:
        Tuple of (matched address, score) if found, None otherwise
    """
    # Parse address components
        
    
    # Get candidate pools using different blocking strategies
    candidate_pools = []
    
    # 1. Try 3-char street prefix
    prefix3 = clean(street)[:3]
    candidates = prefix_index.get(prefix3, [])
    candidate_pools.extend(candidates)
    
    # 2. Try 4-char street prefix for longer streets
    if len(street) > 3:
        prefix4 = clean(street)[:4]
        candidates = prefix_index.get(prefix4, [])
        candidate_pools.extend(candidates)
    
    
    if not candidate_pools:
        return None
        
    # Use RapidFuzz's process for efficient matching
    candidates = [f"{clean(c.get('house'))} {clean(c.get('street'))} {clean(c.get('strtype'))}" 
                 for c in candidate_pools]
    
    result = process.extractOne(
        normalized_address,
        candidates,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=int(threshold * 100)
    )
    
    if result:
        score = result[1] / 100.0
        match = candidate_pools[candidates.index(result[0])]
        return match, score
        
    return None