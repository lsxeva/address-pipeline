import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pandas as pd
import time
import json
from datetime import datetime
from tqdm import tqdm
from src.utils.performance_monitor import PerformanceMonitor
from src.processing.preprocess_utils import (
    build_prefix_index,
    build_metaphone_index,
    build_normalized_index_extended,
    clean,
    make_normalized_key
)
from src.matching.matcher_engine import exact_match_dict, fuzzy_match_block
from src.matching.fallback import phonetic_fallback, api_fallback
from multiprocessing import Pool, cpu_count
from collections import defaultdict

class AddressValidator:
    """Mock API validator for address validation"""
    def __init__(self):
        pass

    def validate_address(self, address):
        return address, 0.95

def save_test_results(stats: dict, match_stats: dict, output_dir: str = "data/processed/test_results"):
    """Save test results to files"""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save performance metrics
    performance_file = os.path.join(output_dir, f'performance_metrics_{timestamp}.json')
    with open(performance_file, 'w') as f:
        json.dump(stats, f, indent=2)
    
    # Save matching statistics
    matching_file = os.path.join(output_dir, f'matching_stats_{timestamp}.json')
    with open(matching_file, 'w') as f:
        json.dump(match_stats, f, indent=2)
    
    return performance_file, matching_file

def build_row(txn, match, score, match_type):
    """Build a result row with transaction and match information"""
    return {
        'transaction_id': txn['transaction_id'],
        'matched_address_id': match['hhid'] if match else None,
        'confidence_score': score,
        'match_type': match_type,
        'original_address': txn['original_address'],
        'normalized_address': txn['normalized_address'],
        'matched_address': f"{match['house']} {match['street']} {match['strtype']}" if match else None
    }

def process_chunk(args):
    """Process a chunk of transactions in parallel"""
    chunk, canonical_addresses, indexes, validator = args
    monitor = PerformanceMonitor()
    
    results = []
    # Pre-process all transactions in the batch
    parsed_transactions = []
    for _, txn in chunk.iterrows():
        parsed = {
            'house': clean(txn['house']),
            'street': clean(txn['street']),
            'strtype': clean(txn['strtype']),
            'apttype': clean(txn['apttype']),
            'aptnbr': clean(txn['aptnbr']),
            'city': clean(txn['city']),
            'state': clean(txn['state']),
            'zip': clean(txn['zip']),
            'normalized_address': clean(txn['normalized_address']),
            'original_txn': txn
        }
        parsed_transactions.append(parsed)

    # Process each transaction
    for parsed in parsed_transactions:
        # Try exact matching first
        match = exact_match_dict(parsed, canonical_addresses, indexes['normalized'])
        if match:
            results.append(build_row(parsed['original_txn'], match, 1.0, 'exact'))
            continue

        # Try fuzzy matching with improved blocking
        fuzzy = fuzzy_match_block(parsed['normalized_address'], parsed['street'], indexes['prefix'])
        if fuzzy:
            match, score = fuzzy
            results.append(build_row(parsed['original_txn'], match, score, 'fuzzy'))
            continue

        # Try phonetic matching
        phonetic = phonetic_fallback(parsed, indexes['metaphone'])
        if phonetic:
            match, score = phonetic
            results.append(build_row(parsed['original_txn'], match, score, 'metaphone'))
            continue

        # Try API validation as fallback
        api = api_fallback(parsed, indexes['prefix'], validator)
        if api:
            match, score = api
            monitor.record_api_call(0.01)  # Record API call cost
            results.append(build_row(parsed['original_txn'], match, score, 'api_validated'))
            continue

        # No match found
        results.append(build_row(parsed['original_txn'], None, 0.0, 'no_match'))
    
    # Update memory usage
    monitor.update_peak_memory()
    return results, monitor.get_stats()

def test_large_scale_performance():
    """Test the performance of the matching system with 200M records"""
    # Initialize
    validator = AddressValidator()
    transactions = pd.read_csv("data/processed/processed_transactions.csv")
    canonical_df = pd.read_csv("data/raw/11211 Addresses.csv")
    canonical_addresses = canonical_df.to_dict('records')
    
    # Initialize main performance monitor
    main_monitor = PerformanceMonitor()
    
    # Build indexes
    indexes = {
        'normalized': build_normalized_index_extended(canonical_addresses),
        'prefix': build_prefix_index(canonical_addresses),
        'metaphone': build_metaphone_index(canonical_addresses)
    }
    
    # Split data into chunks
    CHUNK_SIZE = 50000000  # Process 20M records per thread
    num_chunks = 4 # Simulate 10 records (10000 * 2000 = 200M)
    chunks = [transactions.iloc[:CHUNK_SIZE].copy() for _ in range(num_chunks)]
    
    # Create progress bar
    pbar = tqdm(total=CHUNK_SIZE * num_chunks, 
                desc="Processing Transactions",
                unit='records',
                unit_scale=True,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {rate_fmt} {postfix}')
    
    # Process chunks in parallel
    all_results = []
    try:
        with Pool(cpu_count()) as pool:
            args = [(chunk, canonical_addresses, indexes, validator) for chunk in chunks]
            for chunk_results, chunk_stats in pool.imap_unordered(process_chunk, args):
                all_results.extend(chunk_results)
                
                # Update main monitor with chunk statistics
                main_monitor.api_calls += chunk_stats.get('total_api_calls', 0)
                main_monitor.api_cost += chunk_stats.get('total_api_cost', 0)
                main_monitor.update_peak_memory()
                
                # Update progress bar
                stats = main_monitor.get_stats()
                pbar.update(CHUNK_SIZE)
                pbar.set_postfix({
                    'Runtime': f"{stats['total_runtime_minutes']:.1f}m",
                    'Memory': f"{stats['peak_memory_gb']:.1f}GB",
                    'API Cost': f"${stats['total_api_cost']:.2f}"
                })
                pbar.refresh()
    finally:
        pbar.close()
    
    # Save performance report
    report_path = main_monitor.save_report("data/processed/test_results")
    
    # Save matching results
    output_csv = os.path.join("data/processed/test_results", 
                             f'matching_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    pd.DataFrame(all_results).to_csv(output_csv, index=False)
    
    # Print summary
    stats = main_monitor.get_stats()
    print("\nPerformance Summary:")
    print(f"Total Runtime: {stats['total_runtime_minutes']:.1f} minutes")
    print(f"Peak Memory: {stats['peak_memory_gb']:.1f} GB")
    print(f"Total API Cost: ${stats['total_api_cost']:.2f}")
    print(f"\nResults saved to:")
    print(f"- Performance Report: {report_path}")
    print(f"- Matching Results: {output_csv}")
    
    return {
        'performance_report': report_path,
        'matching_results': output_csv,
        'stats': stats
    }

if __name__ == "__main__":
    test_large_scale_performance()
