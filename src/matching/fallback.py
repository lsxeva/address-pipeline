# src/matching/fallback.py

from rapidfuzz import fuzz
import metaphone
from typing import Dict, Any, List, Optional, Tuple
from src.processing.preprocess_utils import clean, make_normalized_key
import time


def phonetic_fallback(transaction: Dict[str, Any], metaphone_index: Dict[str, List[Dict[str, Any]]], threshold: float = 0.75) -> Optional[Tuple[Dict[str, Any], float]]:
    """
    Try to find a phonetic (Metaphone) match for the transaction's normalized address.
    Returns best matching address and score if any above threshold.
    """
    key = make_normalized_key(transaction['house'], transaction['street'], transaction['strtype'])
    mkey = metaphone.dm(key)[0]

    best_score = 0
    best_match = None

    for candidate in metaphone_index.get(mkey, [])[:20]:
        cand_key = make_normalized_key(candidate.get('house'), candidate.get('street'), candidate.get('strtype'))
        score = fuzz.token_sort_ratio(key, cand_key) / 100.0
        if score > best_score:
            best_score = score
            best_match = candidate

    if best_score >= threshold:
        return best_match, best_score
    return None

def api_fallback(transaction: Dict[str, Any], street_index: Dict[str, List[Dict[str, Any]]], api_validator, threshold: float = 0.8) -> Optional[Tuple[Dict[str, Any], float]]:
    """
    API fallback that validates address and tries to match using validated address
    """
    # 验证地址
    validated_address, api_conf = api_validator.validate_address(transaction)
    if not validated_address or api_conf < threshold:
        return None

    # 使用验证后的地址进行匹配
    street = clean(validated_address['street'])
    if not street or street not in street_index:
        return None

    # 获取相同街道的候选地址
    candidates = street_index[street]
    if not candidates:
        return None

    # 构建验证后的地址键
    validated_key = make_normalized_key(
        validated_address['house'],
        validated_address['street'],
        validated_address['strtype']
    )

    # 在候选地址中查找最佳匹配
    best_score = 0
    best_match = None

    for cand in candidates:
        cand_key = make_normalized_key(
            cand.get('house'),
            cand.get('street'),
            cand.get('strtype')
        )
        score = fuzz.token_sort_ratio(validated_key, cand_key) / 100.0
        if score > best_score:
            best_score = score
            best_match = cand

    # 如果找到足够好的匹配，返回结果
    if best_score >= threshold:
        # 将API置信度与匹配分数结合
        final_score = best_score * api_conf
        return best_match, final_score

    return None
