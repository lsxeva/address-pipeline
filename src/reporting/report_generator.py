import pandas as pd
from typing import List, Dict, Any
from src.utils.performance_monitor import PerformanceMonitor
from datetime import datetime

def generate_matching_summary(df: pd.DataFrame) -> str:
    """Generate matching summary statistics"""
    summary = []
    summary.append("\n=== Matching Summary ===")
    
    match_counts = df['match_type'].value_counts()
    summary.append(match_counts.to_string())
    
    total_matches = len(df[df['matched_address_id'].notna()])
    total_records = len(df)
    match_percentage = (total_matches / total_records * 100)
    summary.append(f"\nTotal matches: {total_matches:,}/{total_records:,} ({match_percentage:.1f}%)")
    
    return "\n".join(summary)

def generate_confidence_distribution(df: pd.DataFrame) -> str:
    """Generate confidence score distribution report"""
    summary = []
    summary.append("\n=== Confidence Score Distribution ===")
    
    for a, b in [(0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.0)]:
        count = len(df[(df['confidence_score'] >= a) & (df['confidence_score'] < b)])
        percentage = count / len(df) * 100
        summary.append(f"Scores {a:.1f}-{b:.1f}: {count:,} ({percentage:.1f}%)")
    
    return "\n".join(summary)

def generate_unmatched_analysis(monitor: PerformanceMonitor, results_df: pd.DataFrame) -> str:
    """
    Generate detailed analysis of unmatched records with English annotations.
    
    Args:
        monitor: PerformanceMonitor instance containing unmatched reasons
        results_df: DataFrame containing all matching results
    
    Returns:
        str: Detailed analysis report of unmatched records
    """
    summary = []
    summary.append("\n=== Unmatched Records Analysis ===")
    
    # Get all unmatched records
    unmatched_df = results_df[results_df['matched_address_id'].isna()].copy()
    total_unmatched = len(unmatched_df)
    
    # Basic statistics
    summary.append(f"Total unmatched records: {total_unmatched:,}")
    
    # Analyze confidence scores
    summary.append("\nConfidence Score Analysis:")
    score_ranges = [(0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.0)]
    for low, high in score_ranges:
        count = len(unmatched_df[(unmatched_df['confidence_score'] >= low) & 
                               (unmatched_df['confidence_score'] < high)])
        if count > 0:
            summary.append(f"Scores {low:.1f}-{high:.1f}: {count:,} records")
    
    # Analyze unmatch reasons
    summary.append("\nDetailed Unmatch Reasons:")
    for reason, count in monitor.unmatched_reasons.items():
        percentage = count / total_unmatched * 100
        summary.append(f"{reason}: {count:,} ({percentage:.1f}%)")
    
    # Analyze address format issues
    summary.append("\nAddress Format Analysis:")
    # Check for special characters
    special_chars = unmatched_df['original_address'].str.contains(r'[^a-zA-Z0-9\s]').sum()
    summary.append(f"Addresses with special characters: {special_chars:,}")
    
    # Check address length
    short_addresses = unmatched_df['original_address'].str.len() < 10
    summary.append(f"Very short addresses (<10 chars): {short_addresses.sum():,}")
    
    # Check for numbers
    no_numbers = unmatched_df['original_address'].str.contains(r'\d').sum() == 0
    summary.append(f"Addresses without numbers: {no_numbers.sum():,}")
    
    # Generate sample unmatched records
    summary.append("\nSample Unmatched Records:")
    sample_size = min(5, total_unmatched)
    if sample_size > 0:
        sample_df = unmatched_df.sample(sample_size)
        for _, row in sample_df.iterrows():
            summary.append(f"\nTransaction ID: {row['transaction_id']}")
            summary.append(f"Original Address: {row['original_address']}")
            summary.append(f"Normalized Address: {row['normalized_address']}")
            summary.append(f"Confidence Score: {row['confidence_score']:.2f}")
            summary.append(f"Match Type: {row['match_type']}")
    
    # Save unmatched records to separate file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unmatched_path = f"data/processed/unmatched_records_{timestamp}.csv"
    unmatched_df.to_csv(unmatched_path, index=False)
    summary.append(f"\nDetailed unmatched records saved to: {unmatched_path}")
    
    return "\n".join(summary)

def generate_performance_metrics(monitor: PerformanceMonitor) -> str:
    """Generate performance metrics report"""
    summary = []
    summary.append("\n=== Performance Metrics ===")
    
    stats = monitor.get_stats()
    summary.append(f"Total runtime: {stats['total_runtime_seconds']:.2f} seconds")
    summary.append(f"Total runtime: {stats['total_runtime_minutes']:.2f} minutes")
    summary.append(f"Peak memory usage: {stats['peak_memory_mb']/1024:.2f} GB")
    summary.append(f"Total API calls: {stats['total_api_calls']:,}")
    summary.append(f"Total API cost: ${stats['total_api_cost']:.2f}")
    
    
    return "\n".join(summary)

def save_results(df: pd.DataFrame, output_path: str) -> bool:
    """Save results to CSV file"""
    try:
        df.to_csv(output_path, index=False)
        return True
    except PermissionError:
        print(f"Error: Cannot save results file to {output_path}. Please ensure the file is not open in another program.")
        return False
