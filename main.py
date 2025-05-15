# main.py

import pandas as pd
import logging
from datetime import datetime
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from src.matching.matcher_engine import (
    exact_match_dict,
    fuzzy_match_block
)
from src.matching.fallback import phonetic_fallback, api_fallback
from src.utils.performance_monitor import PerformanceMonitor
from src.processing.preprocess_utils import (
    clean, 
    make_normalized_key, 
    build_prefix_index, 
    build_metaphone_index, 
    build_normalized_index_extended
)
from src.reporting.report_generator import (
    generate_matching_summary,
    generate_confidence_distribution,
    generate_unmatched_analysis,
    generate_performance_metrics,
    save_results
)
import psutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AddressValidator:
    """Mock API validator for address validation"""
    def __init__(self):
        pass

    def validate_address(self, address):
        return address, 0.95

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

def get_optimal_workers():
    """
    Calculate optimal number of workers based on system resources.
    Returns the optimal number of worker processes based on available CPU cores and memory.
    """
    cpu_count = cpu_count()
    memory_gb = psutil.virtual_memory().total / (1024 * 1024 * 1024)
    
    # Calculate workers based on CPU and memory
    cpu_based = cpu_count
    memory_based = int(memory_gb * 2)  # Assuming 0.5GB memory per worker
    
    return min(cpu_based, memory_based, 8)  # Limit maximum workers to 8

def get_optimal_chunk_size(total_records):
    """
    Calculate optimal chunk size based on total records.
    Ensures efficient processing while maintaining memory constraints.
    """
    workers = get_optimal_workers()
    # Ensure minimum chunk size for efficiency
    min_chunk = 1000
    # Set maximum chunk size to 50M records per thread
    max_chunk = 50000000
    
    chunk_size = total_records // (workers * 4)
    return max(min_chunk, min(chunk_size, max_chunk))

def process_chunk(args):
    """
    Process a chunk of transactions in parallel.
    
    Args:
        args: Tuple containing (chunk, canonical_addresses, indexes, validator)
    
    Returns:
        Tuple of (results, stats) where results is a list of matching results
        and stats contains performance metrics
    """
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

        # Try fuzzy matching
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

        # No match found - record the highest score from previous attempts
        highest_score = 0.0
        if fuzzy:
            highest_score = max(highest_score, fuzzy[1])
        if phonetic:
            highest_score = max(highest_score, phonetic[1])
        if api:
            highest_score = max(highest_score, api[1])
            
        monitor.record_unmatched('no_match_found')
        results.append(build_row(parsed['original_txn'], None, highest_score, 'no_match'))
    
    # Record batch statistics
    monitor.record_batch_stats(len(chunk), monitor.get_runtime())
    return results, monitor.get_stats()

def main():
    """
    Main function to run the address matching pipeline.
    Handles data loading, processing, and result saving.
    """
    # Initialize performance monitor
    monitor = PerformanceMonitor()
    validator = AddressValidator()
    
    # Load transactions and canonical addresses
    transactions = pd.read_csv("data/processed/processed_transactions.csv")
    canonical_df = pd.read_csv("data/raw/11211 Addresses.csv")
    
    # Convert canonical addresses to records
    canonical_addresses = canonical_df.to_dict('records')
    
    # Build indexes
    indexes = {
        'normalized': build_normalized_index_extended(canonical_addresses),
        'prefix': build_prefix_index(canonical_addresses),
        'metaphone': build_metaphone_index(canonical_addresses)
    }
    
    # Set fixed chunk size and number of processes
    CHUNK_SIZE = 50000000  # Process 50M records per thread
    num_processes = 8  # Fixed number of processes
    
    # Split data into chunks for parallel processing
    chunks = [transactions.iloc[i:i+CHUNK_SIZE].copy() 
             for i in range(0, len(transactions), CHUNK_SIZE)]
    
    # Create progress bar
    pbar = tqdm(total=len(transactions), 
                desc="Processing Transactions",
                unit='records',
                unit_scale=True,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {rate_fmt} {postfix}')
    
    # Process chunks in parallel
    all_results = []
    try:
        with Pool(num_processes) as pool:
            args = [(chunk, canonical_addresses, indexes, validator) for chunk in chunks]
            for chunk_results, chunk_stats in pool.imap_unordered(process_chunk, args):
                all_results.extend(chunk_results)
                pbar.update(len(chunk_results))
                pbar.set_postfix({
                    'Processed': f"{len(all_results):,}",
                    'Runtime': f"{chunk_stats['total_runtime_seconds']:.1f}s",
                    'Memory': f"{chunk_stats['peak_memory_mb']/1024:.1f}GB"
                })
                pbar.refresh()
    finally:
        pbar.close()
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(all_results)
    
    # Generate timestamp for file names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save results using report generator
    results_path = f"data/processed/matching_results_{timestamp}.csv"
    if save_results(results_df, results_path):
        print(f"\nResults saved to: {results_path}")
    
    # Generate and print reports
    print(generate_matching_summary(results_df))
    print(generate_confidence_distribution(results_df))
    print(generate_unmatched_analysis(monitor, results_df))
    print(generate_performance_metrics(monitor))
    
    # Save report to file
    report_path = f"data/processed/matching_report_{timestamp}.txt"
    with open(report_path, 'w') as f:
        f.write(generate_matching_summary(results_df))
        f.write(generate_confidence_distribution(results_df))
        f.write(generate_unmatched_analysis(monitor, results_df))
        f.write(generate_performance_metrics(monitor))
    
    print(f"\nDetailed report saved to: {report_path}")

if __name__ == "__main__":
    main()
