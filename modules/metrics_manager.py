from collections import defaultdict
from modules.logger import logger
import json
from datetime import datetime
class MetricsTracker:
    def __init__(self):
        self.metrics = {
            'posts_seen': 0,
            'posts_attempted': 0,
            'posts_extracted': 0,
            'posts_skipped': 0,
            'posts_failed': 0,
            'skipped_reasons': defaultdict(int),
            'failed_reasons': defaultdict(int),
            'retries_by_step': defaultdict(int),
            'start_time': None,
            'end_time': None
        }

    def start_session(self):
        self.metrics['start_time'] = datetime.now()

    def end_session(self):
        self.metrics['end_time'] = datetime.now()

    def increment(self, metric):
        if metric in self.metrics:
            self.metrics[metric] += 1

    def track_skip(self, reason):
        self.metrics['posts_skipped'] += 1
        self.metrics['skipped_reasons'][reason] += 1

    def track_failure(self, reason):
        self.metrics['posts_failed'] += 1
        self.metrics['failed_reasons'][reason] += 1

    def track_retry(self, step_name):
        self.metrics['retries_by_step'][step_name] += 1

    def print_summary(self):
        duration = "N/A"
        if self.metrics['start_time'] and self.metrics['end_time']:
            duration = str(self.metrics['end_time'] - self.metrics['start_time'])

        summary = [
            "\n" + "="*50,
            "           EXECUTION SUMMARY REPORT           ",
            "="*50,
            f"Duration:        {duration}",
            f"Total Seen:      {self.metrics['posts_seen']}",
            f"Total Attempted: {self.metrics['posts_attempted']}",
            f"Successfully Extracted: {self.metrics['posts_extracted']}",
            f"Skipped:         {self.metrics['posts_skipped']}",
            f"Failed:          {self.metrics['posts_failed']}",
            "-"*50,
            "SKIPPED BREAKDOWN:"
        ]
        
        for reason, count in self.metrics['skipped_reasons'].items():
            summary.append(f"  - {reason}: {count}")
            
        summary.append("-" * 50)
        summary.append("FAILURE BREAKDOWN:")
        for reason, count in self.metrics['failed_reasons'].items():
            summary.append(f"  - {reason}: {count}")

        summary.append("-" * 50)
        summary.append("RETRY COUNTS BY STEP:")
        for step, count in self.metrics['retries_by_step'].items():
            summary.append(f"  - {step}: {count}")
            
        summary.append("="*50 + "\n")
        
        report = "\n".join(summary)
        print(report)
        logger.info("Session Summary:\n" + report, extra={"step_name": "Summary"})
