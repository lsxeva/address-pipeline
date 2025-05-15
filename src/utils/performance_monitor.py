import time
import psutil
import os
from datetime import datetime
import json
from typing import Dict, Any, List
from collections import defaultdict

class PerformanceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.peak_memory = 0
        self.api_calls = 0
        self.api_cost = 0.0
        self.unmatched_reasons = defaultdict(int)
        self.batch_stats = []
        self.match_type_stats = defaultdict(int)
        self.confidence_score_distribution = defaultdict(int)
        self.processing_speeds = []
        self.memory_usage_history = []
        self.last_update_time = self.start_time
        self.last_processed_count = 0
        
    def update_peak_memory(self):
        """Update peak memory usage and record memory history"""
        process = psutil.Process(os.getpid())
        current_memory = process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = max(self.peak_memory, current_memory)
        self.memory_usage_history.append({
            'timestamp': time.time(),
            'memory_mb': current_memory
        })
        
    def record_api_call(self, cost: float = 0.0):
        """Record API call and its cost"""
        self.api_calls += 1
        self.api_cost += cost
        
    def record_unmatched(self, reason: str):
        """Record reason for unmatched address"""
        self.unmatched_reasons[reason] += 1
        
    def record_match(self, match_type: str, confidence_score: float):
        """Record match type and confidence score"""
        self.match_type_stats[match_type] += 1
        # Record confidence score in appropriate range
        score_range = f"{int(confidence_score * 5) * 20}-{(int(confidence_score * 5) + 1) * 20}"
        self.confidence_score_distribution[score_range] += 1
        
    def record_batch_stats(self, batch_size: int, batch_time: float):
        """Record batch processing statistics"""
        current_time = time.time()
        time_diff = current_time - self.last_update_time
        processed_diff = batch_size - self.last_processed_count
        
        if time_diff > 0:
            speed = processed_diff / time_diff
            self.processing_speeds.append({
                'timestamp': current_time,
                'speed': speed,
                'batch_size': batch_size,
                'processing_time': batch_time
            })
        
        self.batch_stats.append({
            'batch_size': batch_size,
            'processing_time': batch_time,
            'speed': processed_diff / time_diff if time_diff > 0 else 0
        })
        
        self.last_update_time = current_time
        self.last_processed_count = batch_size
        
    def get_runtime(self) -> float:
        """Get total runtime in seconds"""
        return time.time() - self.start_time
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics"""
        total_runtime = self.get_runtime()
        total_records = sum(stat['batch_size'] for stat in self.batch_stats)
        
        return {
            # Basic metrics
            'total_runtime_seconds': total_runtime,
            'total_runtime_minutes': total_runtime / 60,
            'peak_memory_mb': self.peak_memory,
            'peak_memory_gb': self.peak_memory / 1024,
            
            # API metrics
            'total_api_calls': self.api_calls,
            'total_api_cost': self.api_cost,
            'api_calls_per_second': self.api_calls / total_runtime if total_runtime > 0 else 0,
            
            # Processing metrics
            'total_records_processed': total_records,
            'records_per_second': total_records / total_runtime if total_runtime > 0 else 0,
            'average_batch_size': sum(stat['batch_size'] for stat in self.batch_stats) / len(self.batch_stats) if self.batch_stats else 0,
            'average_batch_time': sum(stat['processing_time'] for stat in self.batch_stats) / len(self.batch_stats) if self.batch_stats else 0,
            
            # Matching statistics
            'match_type_distribution': dict(self.match_type_stats),
            'confidence_score_distribution': dict(self.confidence_score_distribution),
            'unmatched_reasons': dict(self.unmatched_reasons),
            'match_rate': sum(self.match_type_stats.values()) / total_records if total_records > 0 else 0,
            
            # Detailed statistics
            'batch_stats': self.batch_stats,
            'processing_speeds': self.processing_speeds,
            'memory_usage_history': self.memory_usage_history
        }
    
    def save_report(self, output_dir: str) -> str:
        """Save detailed performance report to JSON file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = os.path.join(output_dir, f'performance_report_{timestamp}.json')
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save detailed report
        with open(report_path, 'w') as f:
            json.dump(self.get_stats(), f, indent=2)
            
        # Save summary report
        summary_path = os.path.join(output_dir, f'performance_summary_{timestamp}.json')
        summary = {
            'timestamp': timestamp,
            'total_runtime_minutes': self.get_runtime() / 60,
            'peak_memory_gb': self.peak_memory / 1024,
            'total_records_processed': sum(stat['batch_size'] for stat in self.batch_stats),
            'records_per_second': sum(stat['batch_size'] for stat in self.batch_stats) / self.get_runtime() if self.get_runtime() > 0 else 0,
            'total_api_calls': self.api_calls,
            'total_api_cost': self.api_cost,
            'match_rate': sum(self.match_type_stats.values()) / sum(stat['batch_size'] for stat in self.batch_stats) if self.batch_stats else 0
        }
        
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
            
        return report_path, summary_path
