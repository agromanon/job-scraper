#!/usr/bin/env python3
"""
Job Status Checker
Periodically verifies if jobs in database are still active
"""

import os
import sys
import logging
import requests
import psycopg2
from datetime import datetime, timedelta
from typing import List, Dict
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = os.getenv('POSTGRES_URL', 'postgresql://postgres:password@localhost:5432/job_scraping')

class JobStatusChecker:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Configure proxies if available
        self._setup_proxies()
    
    def _setup_proxies(self):
        """Configure HTTP proxies from environment variables"""
        # Check for Webshare proxy configuration first
        webshare_host = os.getenv('WEBSHARE_PROXY_HOST')
        webshare_port = os.getenv('WEBSHARE_PROXY_PORT')
        webshare_username = os.getenv('WEBSHARE_USERNAME')
        webshare_password = os.getenv('WEBSHARE_PASSWORD')
        
        if webshare_host and webshare_port:
            # Webshare rotating proxy
            proxy_url = f"http://{webshare_host}:{webshare_port}"
            if webshare_username and webshare_password:
                proxy_url = f"http://{webshare_username}:{webshare_password}@{webshare_host}:{webshare_port}"
            
            proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            self.session.proxies.update(proxies)
            logger.info(f"Configured Webshare proxy: {proxy_url}")
            return
        
        # Check for standard HTTP proxy environment variables
        http_proxy = os.getenv('HTTP_PROXY') or os.getenv('http_proxy')
        https_proxy = os.getenv('HTTPS_PROXY') or os.getenv('https_proxy')
        
        if http_proxy or https_proxy:
            proxies = {}
            if http_proxy:
                proxies['http'] = http_proxy
            if https_proxy:
                proxies['https'] = https_proxy
            
            self.session.proxies.update(proxies)
            logger.info(f"Configured HTTP proxies: {proxies}")
            return
        
        # Check for individual proxy configuration from PROXIES environment variable
        proxies_env = os.getenv('PROXIES')
        if proxies_env:
            try:
                proxy_list = [p.strip() for p in proxies_env.split(',') if p.strip()]
                if proxy_list:
                    # Use the first proxy from the list
                    proxy_url = proxy_list[0]
                    proxies = {
                        'http': proxy_url,
                        'https': proxy_url
                    }
                    self.session.proxies.update(proxies)
                    logger.info(f"Configured proxy from PROXIES env: {proxy_url}")
            except Exception as e:
                logger.warning(f"Failed to configure proxy from PROXIES env: {e}")
    
    def get_stale_jobs(self, days_old: int = 7, limit: int = 100) -> List[Dict]:
        """Get jobs that haven't been checked recently"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            # Get jobs from all job tables that haven't been checked in X days
            # or have never been checked
            query = """
                SELECT id, job_url, site, scraped_at
                FROM (
                    SELECT id, job_url, 'linkedin' as site, scraped_at FROM linkedin_br
                    UNION ALL
                    SELECT id, job_url, 'indeed' as site, scraped_at FROM indeed_br
                    UNION ALL
                    SELECT id, job_url, 'glassdoor' as site, scraped_at FROM glassdoor_br
                ) all_jobs
                WHERE scraped_at < %s
                ORDER BY scraped_at ASC
                LIMIT %s
            """
            
            cursor.execute(query, (
                datetime.utcnow() - timedelta(days=days_old),
                limit
            ))
            
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to get stale jobs: {e}")
            return []
    
    def check_job_status(self, job_url: str, site: str) -> bool:
        """Check if a job is still active by making a HEAD request"""
        try:
            # Add delays to avoid rate limiting
            time.sleep(1)
            
            response = self.session.head(job_url, timeout=10, allow_redirects=True)
            
            # Job is active if we get a 200 or 301/302 (redirect)
            if response.status_code in [200, 301, 302]:
                return True
            
            # Job might be inactive if we get 404, 410, etc.
            if response.status_code in [404, 410]:
                return False
                
            # For other status codes, try a GET request
            response = self.session.get(job_url, timeout=10)
            return response.status_code == 200
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to check job status for {job_url}: {e}")
            return False  # Assume inactive if we can't check
        except Exception as e:
            logger.error(f"Unexpected error checking job {job_url}: {e}")
            return False
    
    def remove_inactive_jobs(self, inactive_jobs: List[Dict]):
        """Remove inactive jobs from database"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            
            removed_count = 0
            
            for job in inactive_jobs:
                table_name = f"{job['site']}_br"
                try:
                    cursor.execute(
                        f"DELETE FROM {table_name} WHERE id = %s",
                        (job['id'],)
                    )
                    removed_count += cursor.rowcount
                except Exception as e:
                    logger.error(f"Failed to remove job {job['id']} from {table_name}: {e}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Removed {removed_count} inactive jobs from database")
            
        except Exception as e:
            logger.error(f"Failed to remove inactive jobs: {e}")
    
    def run_check_cycle(self, days_old: int = 7, batch_size: int = 50):
        """Run one cycle of job status checking"""
        logger.info("Starting job status check cycle")
        
        # Get stale jobs to check
        stale_jobs = self.get_stale_jobs(days_old, batch_size)
        logger.info(f"Found {len(stale_jobs)} stale jobs to check")
        
        if not stale_jobs:
            logger.info("No stale jobs to check")
            return
        
        # Check each job
        inactive_jobs = []
        active_jobs = []
        
        for i, job in enumerate(stale_jobs):
            logger.info(f"Checking job {i+1}/{len(stale_jobs)}: {job['job_url']}")
            
            is_active = self.check_job_status(job['job_url'], job['site'])
            
            if is_active:
                active_jobs.append(job)
                logger.debug(f"Job is active: {job['job_url']}")
            else:
                inactive_jobs.append(job)
                logger.debug(f"Job is inactive: {job['job_url']}")
        
        # Remove inactive jobs
        if inactive_jobs:
            logger.info(f"Found {len(inactive_jobs)} inactive jobs to remove")
            self.remove_inactive_jobs(inactive_jobs)
        else:
            logger.info("No inactive jobs found")
        
        logger.info(f"Check cycle completed. Active: {len(active_jobs)}, Inactive: {len(inactive_jobs)}")

def main():
    """Main function to run the job status checker"""
    checker = JobStatusChecker(DATABASE_URL)
    
    # Run continuously or once based on environment
    run_once = os.getenv('RUN_ONCE', 'false').lower() == 'true'
    
    if run_once:
        # Run once and exit
        checker.run_check_cycle()
    else:
        # Run continuously
        check_interval = int(os.getenv('CHECK_INTERVAL_HOURS', '24')) * 3600
        
        while True:
            try:
                checker.run_check_cycle()
                logger.info(f"Sleeping for {check_interval} seconds")
                time.sleep(check_interval)
            except KeyboardInterrupt:
                logger.info("Received interrupt, shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(300)  # Wait 5 minutes before retrying

if __name__ == "__main__":
    main()