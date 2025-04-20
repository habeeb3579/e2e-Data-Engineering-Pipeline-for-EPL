# src/scraping/utils/rate_limiter.py

import time
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import List


class RateLimiter:
    """
    Rate limiter to prevent sending too many requests in a short period
    """
    
    def __init__(self, requests_per_minute: int = 10):
        """
        Initialize rate limiter
        
        Args:
            requests_per_minute: Maximum number of requests allowed per minute
        """
        self.requests_per_minute = requests_per_minute
        self.time_window = 60  # seconds
        self.request_timestamps = deque()
        self.lock = threading.Lock()
    
    def wait(self):
        """
        Wait until a request can be made according to the rate limit
        """
        with self.lock:
            current_time = datetime.now()
            
            # Remove timestamps older than the time window
            while (
                self.request_timestamps and 
                (current_time - self.request_timestamps[0]).total_seconds() > self.time_window
            ):
                self.request_timestamps.popleft()
            
            # If we've reached the limit, wait until we can make another request
            if len(self.request_timestamps) >= self.requests_per_minute:
                oldest_timestamp = self.request_timestamps[0]
                time_to_wait = self.time_window - (current_time - oldest_timestamp).total_seconds()
                
                if time_to_wait > 0:
                    time.sleep(time_to_wait)
            
            # Add current timestamp to the queue
            self.request_timestamps.append(current_time)