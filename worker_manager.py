#!/usr/bin/env python3
"""
Modular Worker Manager for Job Scraping System
 orchestrates specialized scraping workers with individual scheduling, configuration, and database routing
"""

import json
import time
import threading
import logging
import signal
import sys
import os
import uuid
import traceback
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import JobSpy and related modules
try:
    from jobspy import scrape_jobs
    from jobspy.model import JobType, DescriptionFormat, Site
    logger = logging.getLogger(__name__)
except ImportError as e:
    print(f"Failed to import JobSpy: {e}")
    print("Make sure JobSpy is installed: pip install python-jobspy")
    sys.exit(1)

# Import our modules
# Import proxy manager if available
try:
    from proxy_manager import proxy_manager, initialize_webshare_proxy
    _proxy_manager_available = True
except ImportError:
    _proxy_manager_available = False
    proxy_manager = None
    initialize_webshare_proxy = None
import psycopg2
from psycopg2 import sql, extras
from psycopg2.pool import ThreadedConnectionPool
import schedule

from jobspy import scrape_jobs
from jobspy.model import Site, DescriptionFormat, JobType

logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    """Configuration for a scraping worker"""
    id: int
    name: str
    site: str
    search_term: str
    location: str
    country: str
    distance: int
    job_type: List[str]
    is_remote: bool
    easy_apply: bool
    linkedin_company_ids: List[int]
    hours_old: int
    results_per_run: int
    schedule_hours: int
    schedule_minute_offset: int
    timezone: str
    proxy_rotation_policy: str
    proxies: List[str]
    max_retries: int
    timeout: int
    rate_limit_requests: int
    rate_limit_seconds: int
    description_format: str
    linkedin_fetch_description: bool
    database_id: int
    table_name: str
    status: str
    memory_limit_mb: int
    cpu_limit_cores: float
    max_runtime_minutes: int
    tags: List[str]
    # Pagination offset for rotating through search results
    current_offset: int = 0


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    id: int
    name: str
    host: str
    port: int
    database_name: str
    username: str
    password: str
    ssl_mode: str
    connection_pool_size: int
    max_connections: int
    connection_timeout_seconds: int
    target_table_prefix: str
    batch_size: int
    deduplication_method: str
    deduplication_fields: List[str]


class WorkerDatabaseManager:
    """Manages database connections and operations for workers"""
    
    def __init__(self, db_config: Dict):
        self.pools: Dict[int, ThreadedConnectionPool] = {}
        self.configs: Dict[int, DatabaseConfig] = {}
        self.lock = threading.Lock()
    
    def register_database(self, db_id: int, config: DatabaseConfig):
        """Register a database configuration"""
        with self.lock:
            if db_id not in self.pools:
                try:
                    pool = ThreadedConnectionPool(
                        1, config.connection_pool_size,
                        host=config.host,
                        port=config.port,
                        database=config.database_name,
                        user=config.username,
                        password=config.password,
                        sslmode=config.ssl_mode
                    )
                    self.pools[db_id] = pool
                    self.configs[db_id] = config
                    logger.info(f"Registered database: {config.name} (ID: {db_id})")
                except Exception as e:
                    logger.error(f"Failed to register database {db_id}: {e}")
                    raise
    
    def get_connection(self, db_id: int):
        """Get a database connection from the pool"""
        with self.lock:
            if db_id in self.pools:
                return self.pools[db_id].getconn()
            raise ValueError(f"Database {db_id} not registered")
    
    def release_connection(self, db_id: int, conn):
        """Release a database connection back to the pool"""
        with self.lock:
            if db_id in self.pools:
                self.pools[db_id].putconn(conn)


class ScrapingWorker:
    """Individual scraping worker instance"""
    
    def __init__(self, config: WorkerConfig, db_manager: WorkerDatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.execution_id = str(uuid.uuid4())
        self.start_time = datetime.utcnow()
        self.metrics = {
            'jobs_found': 0,
            'jobs_inserted': 0,
            'jobs_updated': 0,
            'jobs_skipped': 0,
            'duplicates_found': 0,
            'duration_seconds': 0
        }
        
        # Initialize Webshare proxy if credentials are provided in environment
        if _proxy_manager_available:
            webshare_api_key = os.getenv('WEBSHARE_API_KEY')
            webshare_username = os.getenv('WEBSHARE_USERNAME')
            webshare_password = os.getenv('WEBSHARE_PASSWORD')
            
            logger.info(f"Webshare credentials from env: api_key={'*' * len(webshare_api_key) if webshare_api_key else None}, username={webshare_username}, password={'*' * len(webshare_password) if webshare_password else None}")
            
            if webshare_api_key:
                try:
                    initialize_webshare_proxy(api_key=webshare_api_key)
                    logger.info("Initialized Webshare.io proxy service with API key")
                except Exception as e:
                    logger.warning(f"Failed to initialize Webshare.io proxy with API key: {e}")
            elif webshare_username and webshare_password:
                try:
                    initialize_webshare_proxy(webshare_username, webshare_password)
                    logger.info("Initialized Webshare.io proxy service with username/password")
                except Exception as e:
                    logger.warning(f"Failed to initialize Webshare.io proxy with username/password: {e}")
    
    def execute(self) -> Dict[str, Any]:
        """Execute the scraping job"""
        self.start_time = datetime.utcnow()
        self.execution_id = str(uuid.uuid4())
        
        try:
            logger.info(f"Starting worker: {self.config.name} (ID: {self.config.id})")
            
            # Get database connection
            db_conn = self.db_manager.get_connection(self.config.database_id)
            db_config = self.db_manager.configs[self.config.database_id]
            
            try:
                # Execute scraping
                results = self._scrape_jobs()
                
                # Process and store results
                processed_results = self._process_results(results)
                
                # Store in database
                storage_results = self._store_results(db_conn, db_config, processed_results)
                
                # Update metrics
                self.metrics.update(storage_results)
                self.metrics['duration_seconds'] = int((datetime.utcnow() - self.start_time).total_seconds())
                
                return {
                    'status': 'success',
                    'execution_id': self.execution_id,
                    'worker_id': self.config.id,
                    'database_id': self.config.database_id,
                    'start_time': self.start_time,
                    'end_time': datetime.utcnow(),
                    **self.metrics
                }
                
            finally:
                self.db_manager.release_connection(self.config.database_id, db_conn)
                
        except Exception as e:
            logger.error(f"Worker {self.config.name} failed: {e}")
            return {
                'status': 'failed',
                'execution_id': self.execution_id,
                'worker_id': self.config.id,
                'database_id': self.config.database_id,
                'start_time': self.start_time,
                'end_time': datetime.utcnow(),
                'error_message': str(e),
                'error_stacktrace': traceback.format_exc(),
                'duration_seconds': int((datetime.utcnow() - self.start_time).total_seconds()),
                **self.metrics
            }
    
    def _scrape_jobs(self) -> List[Dict]:
        """Execute job scraping using JobSpy"""
        try:
            # Map site name to JobSpy Site enum
            site_enum = Site[self.config.site.upper()]
            
            # Map job types
            job_types = []
            for jt in self.config.job_type:
                if jt in ['FULL_TIME', 'PART_TIME', 'CONTRACT', 'INTERNSHIP']:
                    job_types.append(JobType[jt])
            
            # Map description format
            desc_format = DescriptionFormat[self.config.description_format.upper()]
            
            # Convert LinkedIn company IDs to proper format
            linkedin_company_ids = self.config.linkedin_company_ids if self.config.linkedin_company_ids else None
            
            # Execute scraping
            logger.info(f"Scraping {site_enum.name} with offset={self.config.current_offset}, results_wanted={self.config.results_per_run}")
            
            # For Glassdoor, use enhanced strategies to get diverse results
            search_term = self.config.search_term or None
            google_search_term = None
            hours_old = self.config.hours_old
            
            if site_enum == Site.GLASSDOOR:
                # Apply date filtering if not set
                if not hours_old:
                    hours_old = 24  # Focus on recent jobs to get new listings
                
                # Use offset-based search variation to get different results
                # This helps bypass Glassdoor's tendency to return same jobs regardless of offset
                offset_variation = self.config.current_offset // 10  # Group offsets (0,10,20 -> 0; 100,110,120 -> 10)
                
                if search_term:
                    search_term = f"{search_term} {offset_variation}"
                else:
                    search_term = f"job OR vaga OR position OR role OR career OR trabalho OR employment {offset_variation}"
            
            elif site_enum == Site.GOOGLE:
                # For Google jobs, construct the search term properly
                # Google jobs require specific format: "job_title jobs near location"
                location = self.config.location or ""
                if search_term and location:
                    google_search_term = f"{search_term} jobs near {location}"
                elif search_term:
                    google_search_term = f"{search_term} jobs"
                else:
                    google_search_term = "jobs"
            
            df = scrape_jobs(
                site_name=[site_enum],
                search_term=search_term,
                google_search_term=google_search_term,
                location=self.config.location or None,
                country_indeed=self.config.country.lower() if self.config.country and site_enum in [Site.INDEED, Site.GLASSDOOR] else 'usa',
                distance=self.config.distance,
                job_type=job_types if job_types else None,
                is_remote=self.config.is_remote,
                easy_apply=self.config.easy_apply,
                linkedin_fetch_description=self.config.linkedin_fetch_description,
                linkedin_company_ids=linkedin_company_ids,
                hours_old=hours_old,
                results_wanted=self.config.results_per_run,
                offset=self.config.current_offset,
                description_format=desc_format,
                timeout=self.config.timeout,
                proxies=self._get_proxies()
            )
            
            self.metrics['jobs_found'] = len(df)
            # Debug logging
            if len(df) > 0:
                logger.debug(f"JobSpy DataFrame columns: {list(df.columns)}")
                logger.debug(f"First job data: {df.iloc[0].to_dict()}")
                # Log a few job URLs to see if they're different
                job_urls = df['job_url'].head(5).tolist() if 'job_url' in df.columns else []
                logger.debug(f"First 5 job URLs: {job_urls}")
                logger.info(f"Scraped {len(df)} jobs from {site_enum.name}")
            else:
                logger.info(f"No jobs found from {site_enum.name}")
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Scraping failed for worker {self.config.name}: {e}")
            raise
    
    def _get_proxies(self):
        """Get proxies for scraping - prioritize Webshare.io, fallback to configured proxies"""
        # First try Webshare.io rotating proxy
        if _proxy_manager_available and proxy_manager:
            try:
                webshare_proxies = proxy_manager.get_proxy_dict()
                if webshare_proxies:
                    logger.info("Using Webshare.io rotating proxy")
                    return webshare_proxies
            except Exception as e:
                logger.warning(f"Failed to get Webshare.io proxies: {e}")
        
        # Fallback to configured proxies
        if self.config.proxies:
            # Convert list of proxy strings to format expected by JobSpy
            if isinstance(self.config.proxies, list) and len(self.config.proxies) > 0:
                logger.info(f"Using configured proxies: {len(self.config.proxies)} proxies available")
                return self.config.proxies
            elif isinstance(self.config.proxies, str):
                logger.info("Using single configured proxy")
                return [self.config.proxies]
        
        # No proxies configured
        logger.info("No proxies configured, running without proxies")
        return None
    
    def _process_results(self, results: List[Dict]) -> List[Dict]:
        """Process and normalize job results"""
        processed = []
        
        for result in results:
            # Add worker metadata
            result['worker_id'] = self.config.id
            result['worker_name'] = self.config.name
            result['scraped_at'] = datetime.utcnow()
            result['source_site'] = self.config.site
            
            # Handle field name mapping
            # If jobspy returns 'site', map it to 'source_site' to avoid conflict
            if 'site' in result and 'source_site' not in result:
                result['source_site'] = result.pop('site')
            
            # Handle company information
            if 'company_name' not in result or result['company_name'] is None:
                if 'company' in result and result['company'] is not None and not (isinstance(result['company'], float) and math.isnan(result['company'])):
                    # Handle different company data formats from JobSpy
                    if isinstance(result['company'], dict) and 'name' in result['company']:
                        result['company_name'] = result['company']['name']
                    elif hasattr(result['company'], 'name'):
                        result['company_name'] = result['company'].name
                    elif isinstance(result['company'], str):
                        # Company is provided as a direct string
                        result['company_name'] = result['company']
                # Fallback: Try to extract company name from description
                elif 'description' in result and result['description']:
                    import re
                    # Look for company name patterns in description
                    company_patterns = [
                        r'\*\*([^\*]{3,150})\*\*',  # Bold company name at start like "**Company Name**"
                        r'Empresa:\s*([^\n\r.]{3,100})',
                        r'Organização:\s*([^\n\r.]{3,100})',
                        r'Companhia:\s*([^\n\r.]{3,100})',
                        r'Contratante:\s*([^\n\r.]{3,100})',
                        r'(?:Empresa|Organização|Companhia):\s*([A-Z][^\n\r.]{3,100})'  # More general pattern
                    ]
                    for pattern in company_patterns:
                        company_match = re.search(pattern, result['description'], re.IGNORECASE | re.MULTILINE)
                        if company_match:
                            company_name = company_match.group(1).strip()
                            # Filter out common job board names and generic terms
                            excluded_terms = ['divulga vagas', 'indeed', 'linkedin', 'glassdoor', 'cargo', 'função', 'vaga', 'oportunidade', 'descrição']
                            if not any(excluded_term in company_name.lower() for excluded_term in excluded_terms):
                                # Clean up the company name
                                company_name = re.sub(r'[\*\n\r]+', '', company_name).strip()
                                if len(company_name) > 2:  # Make sure it's a reasonable length
                                    result['company_name'] = company_name
                                    break
                    
                    # If still no company name, try to extract from email domain as last resort
                    if ('company_name' not in result or result['company_name'] is None) and 'emails' in result and result['emails']:
                        if isinstance(result['emails'], list) and len(result['emails']) > 0:
                            email = result['emails'][0]
                            if '@' in email:
                                domain = email.split('@')[1]
                                # Remove common email domain parts
                                company_name = domain.split('.')[0]
                                if not any(excluded in company_name.lower() for excluded in ['gmail', 'hotmail', 'yahoo', 'outlook']):
                                    result['company_name'] = company_name.title()
            
            if 'company_url' not in result or result['company_url'] is None:
                if 'company' in result and result['company'] is not None and not (isinstance(result['company'], float) and math.isnan(result['company'])):
                    if isinstance(result['company'], dict) and 'url' in result['company']:
                        result['company_url'] = result['company']['url']
                    elif hasattr(result['company'], 'url'):
                        result['company_url'] = result['company'].url
            
            # Handle location information
            if ('location_city' not in result or result['location_city'] is None or
                'location_state' not in result or result['location_state'] is None):
                if 'location' in result and result['location'] is not None and not (isinstance(result['location'], float) and math.isnan(result['location'])):
                    location_data = result['location']
                    # Handle location as string from JobSpy (e.g., "Cuiabá, MT, BR" or "São Paulo, São Paulo")
                    if isinstance(location_data, str):
                        # Parse location string
                        parts = [part.strip() for part in location_data.split(',')]
                        if len(parts) >= 1:
                            result['location_city'] = parts[0]
                        if len(parts) >= 2:
                            result['location_state'] = parts[1]
                        # If we have a Brazilian worker and only city/state, assume Brazil
                        if (len(parts) == 2 and 
                            self.config.country and 
                            'brazil' in self.config.country.lower()):
                            result['location_country'] = 'Brazil'
                        elif len(parts) >= 3:
                            result['location_country'] = parts[2]
                    # Handle Location object from JobSpy
                    elif hasattr(location_data, 'city'):
                        result['location_city'] = location_data.city
                    elif isinstance(location_data, dict) and 'city' in location_data:
                        result['location_city'] = location_data['city']
                    
                    if hasattr(location_data, 'state'):
                        result['location_state'] = location_data.state
                    elif isinstance(location_data, dict) and 'state' in location_data:
                        result['location_state'] = location_data['state']
                    
                    if hasattr(location_data, 'country'):
                        result['location_country'] = location_data.country
                    elif isinstance(location_data, dict) and 'country' in location_data:
                        result['location_country'] = location_data['country']
                # Fallback: Try to extract location from description
                elif 'description' in result and result['description']:
                    import re
                    # Look for location patterns in description
                    location_match = re.search(r'(?:Local|Localiza..o|Cidade)[:\s]+([^\n\r.]{2,100})', result['description'], re.IGNORECASE)
                    if location_match:
                        location_text = location_match.group(1).strip()
                        # Try to parse city and state from location text
                        if ',' in location_text:
                            parts = location_text.split(',')
                            result['location_city'] = parts[0].strip()
                            if len(parts) > 1:
                                result['location_state'] = parts[1].strip()
                        else:
                            result['location_city'] = location_text
            
            # Handle compensation
            if 'compensation' in result and result['compensation']:
                comp = result['compensation']
                result['salary_min'] = comp.get('min_amount')
                result['salary_max'] = comp.get('max_amount')
                result['salary_currency'] = comp.get('currency', 'USD')
                result['salary_interval'] = comp.get('interval')
            # Fallback: Try to extract salary from description
            elif 'description' in result and result['description'] and not result.get('salary_min'):
                import re
                # Look for salary patterns in description
                salary_patterns = [
                    r'R\$\s*([\d\.,]+)\s*(?:\-|a|até)\s*R\$\s*([\d\.,]+)',  # Range format like "R$ 3.000 - R$ 7.000"
                    r'R\$\s*([\d\.,]+)',  # Simple R$ format like "R$ 3.676,35"
                    r'[R$]?\s*([\d\.,]+)\s*(?:a\s+)?(?:por\s+)?(?:mês|mes)',
                    r'(?:salário|salario|bolsa)\s*(?:fixo)?\s*[R$]?\s*([\d\.,]+)',
                    r'(?:remuneração|remuneracao)\s*[R$]?\s*([\d\.,]+)',
                    r'R\$\s*([\d\.,]+)\s*/mês',  # Monthly salary format
                    r'salário.*?mínimo.*?R\$\s*([\d\.,]+).*?máximo.*?R\$\s*([\d\.,]+)',  # Min/max format
                ]
                for pattern in salary_patterns:
                    salary_match = re.search(pattern, result['description'], re.IGNORECASE)
                    if salary_match:
                        if len(salary_match.groups()) >= 2:
                            # Handle salary range
                            min_salary = salary_match.group(1).strip()
                            max_salary = salary_match.group(2).strip()
                            try:
                                # Handle Brazilian number format (period = thousands separator, comma = decimal)
                                min_val = float(min_salary.replace('.', '').replace(',', '.'))
                                max_val = float(max_salary.replace('.', '').replace(',', '.'))
                                result['salary_min'] = min_val
                                result['salary_max'] = max_val
                                result['salary_currency'] = 'BRL'
                                result['salary_interval'] = 'monthly'
                                break
                            except ValueError:
                                continue
                        else:
                            # Handle single salary
                            salary_text = salary_match.group(1).strip()
                            try:
                                # Handle Brazilian number format (period = thousands separator, comma = decimal)
                                salary_value = float(salary_text.replace('.', '').replace(',', '.'))
                                result['salary_min'] = salary_value
                                result['salary_currency'] = 'BRL'
                                result['salary_interval'] = 'monthly'
                                break
                            except ValueError:
                                pass
            
            # Flatten job type
            if 'job_type' in result and result['job_type']:
                if isinstance(result['job_type'], list):
                    result['job_type'] = [jt.value if hasattr(jt, 'value') else str(jt) for jt in result['job_type']]
                else:
                    result['job_type'] = [result['job_type']]
            
            processed.append(result)
        
        return processed
    
    def _store_results(self, db_conn, db_config: DatabaseConfig, results: List[Dict]) -> Dict[str, int]:
        """Store results in database with deduplication"""
        if not results:
            return {'jobs_inserted': 0, 'jobs_updated': 0, 'jobs_skipped': 0, 'duplicates_found': 0}
        
        cursor = db_conn.cursor()
        
        # Create table if it doesn't exist
        self._ensure_table_exists(cursor, db_config, self.config.table_name)
        
        # Filter out duplicates before insertion
        duplicates_found = 0
        jobs_to_insert = []
        
        for result in results:
            # Check if job already exists
            cursor.execute(f"""
                SELECT 1 FROM {self.config.table_name} 
                WHERE job_url = %s
            """, (result.get('job_url'),))
            
            if cursor.fetchone():
                duplicates_found += 1
                # Skip this job (don't insert)
            else:
                jobs_to_insert.append(result)
        
        logger.info(f"Filtered results: {len(results)} total, {len(jobs_to_insert)} new, {duplicates_found} duplicates")
        
        if not jobs_to_insert:
            cursor.close()
            return {
                'jobs_inserted': 0, 
                'jobs_updated': 0, 
                'jobs_skipped': len(results), 
                'duplicates_found': duplicates_found
            }
        
        # Insert only new jobs
        try:
            inserted_count = self._batch_insert(cursor, db_config, self.config.table_name, jobs_to_insert)
            db_conn.commit()
            
            cursor.close()
            return {
                'jobs_inserted': inserted_count,
                'jobs_updated': 0,
                'jobs_skipped': len(results) - inserted_count,
                'duplicates_found': duplicates_found
            }
            
        except Exception as e:
            db_conn.rollback()
            cursor.close()
            logger.error(f"Failed to insert jobs: {e}")
            raise
    
    def _ensure_table_exists(self, cursor, db_config: DatabaseConfig, table_name: str):
        """Ensure target table exists with proper schema"""
        # Properly quote the table name to handle special characters
        quoted_table_name = f'"{table_name}"'
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {quoted_table_name} (
                id SERIAL PRIMARY KEY,
                worker_id INTEGER,
                worker_name VARCHAR(100),
                source_site VARCHAR(50),
                scraped_at TIMESTAMP,
                job_id VARCHAR(100) UNIQUE,
                job_url VARCHAR(500) UNIQUE,
                title VARCHAR(255),
                company_name VARCHAR(255),
                company_url VARCHAR(500),
                description TEXT,
                location_city VARCHAR(100),
                location_state VARCHAR(100),
                location_country VARCHAR(100),
                is_remote BOOLEAN,
                job_type JSONB,
                salary_min DECIMAL(10,2),
                salary_max DECIMAL(10,2),
                salary_currency VARCHAR(10),
                salary_interval VARCHAR(20),
                date_posted DATE,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def _is_duplicate(self, cursor, db_config: DatabaseConfig, table_name: str, result: Dict) -> bool:
        """Check if job already exists"""
        # Properly quote the table name
        quoted_table_name = f'"{table_name}"'
        if db_config.deduplication_method == 'unique_id':
            cursor.execute(f"SELECT 1 FROM {quoted_table_name} WHERE job_id = %s", (result.get('id'),))
        elif db_config.deduplication_method == 'composite_key':
            conditions = []
            params = []
            for field in db_config.deduplication_fields:
                if result.get(field):
                    conditions.append(f"{field} = %s")
                    params.append(result[field])
            if conditions:
                cursor.execute(f"SELECT 1 FROM {quoted_table_name} WHERE {' AND '.join(conditions)}", params)
            else:
                return False
        else:
            return False
        
        return cursor.fetchone() is not None
    
    def _should_update_duplicate(self, cursor, db_config: DatabaseConfig, table_name: str, result: Dict) -> bool:
        """Determine if duplicate should be updated"""
        # Properly quote the table name
        quoted_table_name = f'"{table_name}"'
        # Simple logic: update if original is older than 7 days
        cursor.execute(f"SELECT created_at FROM {quoted_table_name} WHERE job_url = %s", (result.get('job_url'),))
        row = cursor.fetchone()
        if row and row[0]:
            created_at = row[0]
            return (datetime.utcnow() - created_at.replace(tzinfo=None)).days > 7
        return False
    
    def _batch_insert(self, cursor, db_config: DatabaseConfig, table_name: str, jobs: List[Dict]) -> int:
        """Batch insert jobs into database"""
        if not jobs:
            return 0
            
        inserted_count = 0
        # Define the expected database columns
        expected_columns = {
            'worker_id', 'worker_name', 'source_site', 'scraped_at', 'job_id', 'job_url', 
            'title', 'company_name', 'company_url', 'description', 'location_city', 
            'location_state', 'location_country', 'is_remote', 'job_type', 'salary_min', 
            'salary_max', 'salary_currency', 'salary_interval', 'date_posted', 'is_active'
        }
        
        # Split into batches
        for i in range(0, len(jobs), db_config.batch_size):
            batch = jobs[i:i + db_config.batch_size]
            
            for job in batch:
                # Filter job data to only include expected columns
                filtered_job = {k: v for k, v in job.items() if k in expected_columns}
                
                if not filtered_job:
                    continue
                    
                # Process special data types and handle NaN values
                processed_job = {}
                for key, value in filtered_job.items():
                    if key == 'job_type' and isinstance(value, list):
                        # Convert array to JSON for JSONB column, handling NaN values
                        try:
                            # Filter out NaN values from the array
                            filtered_array = [v for v in value if v is not None and (not isinstance(v, float) or not math.isnan(v))]
                            processed_job[key] = json.dumps(filtered_array)
                        except (TypeError, ValueError):
                            # If we can't serialize, store as empty array
                            processed_job[key] = json.dumps([])
                    elif isinstance(value, float) and math.isnan(value):
                        # Skip NaN values entirely
                        continue
                    elif isinstance(value, float) and math.isinf(value):
                        # Skip infinity values entirely
                        continue
                    else:
                        processed_job[key] = value
                
                # Build the column names and placeholders
                columns = list(processed_job.keys())
                col_names = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
                
                # Properly quote the table name
                quoted_table_name = f'"{table_name}"'
                query = f"INSERT INTO {quoted_table_name} ({col_names}) VALUES ({placeholders})"
                
                # Extract values in the same order as columns
                values = [processed_job.get(col) for col in columns]
                
                cursor.execute(query, values)
                inserted_count += 1
        
        return inserted_count
    
    def _batch_update(self, cursor, db_config: DatabaseConfig, table_name: str, jobs: List[Dict]) -> int:
        """Batch update existing jobs"""
        # Properly quote the table name
        quoted_table_name = f'"{table_name}"'
        updated_count = 0
        for job in jobs:
            set_clauses = []
            params = []
            
            for key, value in job.items():
                if key not in ['id', 'job_id', 'job_url']:  # Don't update unique keys
                    set_clauses.append(f"{key} = %s")
                    params.append(value)
            
            if set_clauses:
                query = f"UPDATE {quoted_table_name} SET {', '.join(set_clauses)}, updated_at = CURRENT_TIMESTAMP WHERE job_url = %s"
                params.append(job.get('job_url'))
                
                cursor.execute(query, params)
                updated_count += cursor.rowcount
        
        return updated_count


class WorkerManager:
    """Main worker manager that orchestrates all scraping workers"""
    
    def __init__(self, postgres_url: str):
        self.postgres_url = postgres_url
        self.db_manager = WorkerDatabaseManager({})
        self.running_workers: Dict[int, ScrapingWorker] = {}
        self.executor = ThreadPoolExecutor(max_workers=20)
        self.shutdown_flag = threading.Event()
        
        # Set up logging
        logging.basicConfig(level=logging.INFO if not os.getenv('DEBUG', False) else logging.DEBUG,
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Register signal handlers only if we're in the main thread
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("Worker Manager initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown_flag.set()
        sys.exit(0)
    
    def register_databases(self):
        """Register all active databases from the database"""
        try:
            conn = psycopg2.connect(self.postgres_url)
            cursor = conn.cursor(cursor_factory=extras.DictCursor)
            
            cursor.execute("""
                SELECT id, name, host, port, database_name, username, password, 
                       ssl_mode, connection_pool_size, max_connections, 
                       connection_timeout_seconds, target_table_prefix, batch_size,
                       deduplication_method, deduplication_fields
                FROM scraping_databases 
                WHERE is_active = true
            """)
            
            for row in cursor.fetchall():
                db_config = DatabaseConfig(**dict(row))
                self.db_manager.register_database(row['id'], db_config)
            
            cursor.close()
            conn.close()
            
            logger.info(f"Registered {len(self.db_manager.pools)} databases")
            
        except Exception as e:
            logger.error(f"Failed to register databases: {e}")
            raise
    
    def load_active_workers(self) -> List[WorkerConfig]:
        """Load active workers from database"""
        try:
            conn = psycopg2.connect(self.postgres_url)
            cursor = conn.cursor(cursor_factory=extras.DictCursor)
            
            cursor.execute("""
                SELECT * FROM scraping_workers 
                WHERE status = 'active'
                ORDER BY next_run
            """)
            
            workers = []
            for row in cursor.fetchall():
                worker_config = WorkerConfig(**dict(row))
                workers.append(worker_config)
            
            cursor.close()
            conn.close()
            
            logger.info(f"Loaded {len(workers)} active workers")
            return workers
            
        except Exception as e:
            logger.error(f"Failed to load workers: {e}")
            raise
    
    def run_worker(self, worker_config: WorkerConfig):
        """Run a single worker"""
        if self.shutdown_flag.is_set():
            return
        
        try:
            worker = ScrapingWorker(worker_config, self.db_manager)
            result = worker.execute()
            
            # Log execution result
            self._log_execution_result(result)
            
            # Update worker status
            self._update_worker_status(result)
            
            # Schedule next run
            self._schedule_next_run(worker_config.id, result['status'] == 'success')
            
        except Exception as e:
            logger.error(f"Failed to run worker {worker_config.name}: {e}")
            
            # Log error
            self._log_execution_result({
                'status': 'failed',
                'execution_id': str(uuid.uuid4()),
                'worker_id': worker_config.id,
                'database_id': worker_config.database_id,
                'start_time': datetime.utcnow(),
                'end_time': datetime.utcnow(),
                'error_message': str(e),
                'duration_seconds': 0,
                'jobs_found': 0,
                'jobs_inserted': 0,
                'jobs_updated': 0,
                'jobs_skipped': 0,
                'duplicates_found': 0,
                'network_requests': 0,
                'network_errors': 0,
                'proxy_errors': 0
            })
            
            # Update worker status (increment error count)
            self._update_worker_error_status(worker_config.id)
            self._schedule_next_run(worker_config.id, False)
    
    def _log_execution_result(self, result: Dict):
        """Log execution result to database"""
        try:
            conn = psycopg2.connect(self.postgres_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO worker_execution_history (
                    worker_id, database_id, execution_start, execution_end, status,
                    jobs_found, jobs_inserted, jobs_updated, jobs_skipped, duplicates_found,
                    duration_seconds, error_message, error_stacktrace,
                    execution_id, hostname
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                result['worker_id'],
                result['database_id'],
                result['start_time'],
                result['end_time'],
                result['status'],
                result.get('jobs_found', 0),
                result.get('jobs_inserted', 0),
                result.get('jobs_updated', 0),
                result.get('jobs_skipped', 0),
                result.get('duplicates_found', 0),
                result.get('duration_seconds', 0),
                result.get('error_message'),
                result.get('error_stacktrace'),
                result['execution_id'],
                result.get('hostname')
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to log execution result: {e}")
    
    def _update_worker_status(self, result: Dict):
        """Update worker status after execution"""
        try:
            conn = psycopg2.connect(self.postgres_url)
            cursor = conn.cursor()
            
            if result['status'] == 'success':
                # Update last success and rotate offset for next run
                cursor.execute("""
                    UPDATE scraping_workers 
                    SET last_run = %s,
                        last_success = %s,
                        consecutive_errors = 0,
                        current_offset = CASE 
                            WHEN current_offset + results_per_run >= 1000 THEN 0  -- Reset if too high
                            ELSE current_offset + results_per_run
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (
                    result['end_time'],
                    result['end_time'],
                    result['worker_id']
                ))
            else:
                # Update error count
                cursor.execute("""
                    UPDATE scraping_workers 
                    SET last_run = %s,
                        last_error = %s,
                        consecutive_errors = consecutive_errors + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (
                    result['end_time'],
                    result['end_time'],
                    result['worker_id']
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to update worker status: {e}")
    
    def _update_worker_error_status(self, worker_id: int):
        """Update worker status after error"""
        try:
            conn = psycopg2.connect(self.postgres_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE scraping_workers 
                SET 
                    consecutive_errors = consecutive_errors + 1,
                    status = CASE 
                        WHEN consecutive_errors + 1 >= max_consecutive_errors AND auto_pause_on_errors 
                        THEN 'paused' 
                        ELSE 'active' 
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (worker_id,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to update worker error status: {e}")
    
    def _schedule_next_run(self, worker_id: int, success: bool):
        """Schedule next run for worker"""
        try:
            conn = psycopg2.connect(self.postgres_url)
            cursor = conn.cursor()
            
            if success:
                cursor.execute("""
                    UPDATE scraping_workers 
                    SET next_run = calculate_next_run_time(last_run, schedule_hours, schedule_minute_offset, timezone),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (worker_id,))
            else:
                # On failure, schedule retry after 1 hour
                cursor.execute("""
                    UPDATE scraping_workers 
                    SET next_run = CURRENT_TIMESTAMP + INTERVAL '1 hour',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (worker_id,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to schedule next run: {e}")
    
    def check_and_run_workers(self):
        """Check for workers that need to run and execute them"""
        if self.shutdown_flag.is_set():
            return
        
        try:
            workers = self.load_active_workers()
            now = datetime.utcnow()
            
            for worker in workers:
                if worker.next_run and worker.next_run <= now:
                    # Submit worker for execution
                    future = self.executor.submit(self.run_worker, worker)
                    self.running_workers[worker.id] = future
            
            # Clean up completed futures
            completed_workers = []
            for worker_id, future in self.running_workers.items():
                if future.done():
                    completed_workers.append(worker_id)
            
            for worker_id in completed_workers:
                future = self.running_workers.pop(worker_id)
                try:
                    future.result()  # Check for exceptions
                except Exception as e:
                    logger.error(f"Worker {worker_id} execution failed: {e}")
            
            if workers:
                logger.info(f"Checked {len(workers)} workers, {len(self.running_workers)} currently running")
            
        except Exception as e:
            logger.error(f"Failed to check and run workers: {e}")
    
    def run_scheduler(self):
        """Main scheduler loop"""
        logger.info("Starting worker scheduler...")
        
        # Register databases
        self.register_databases()
        
        # Schedule worker checks every minute
        while not self.shutdown_flag.is_set():
            try:
                self.check_and_run_workers()
                time.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                time.sleep(60)
        
        logger.info("Scheduler stopped")
    

if __name__ == "__main__":
    # Example usage
    import os
    import uuid

    postgres_url = os.getenv('POSTGRES_URL', 'postgresql://postgres:password@localhost:5432/job_scraping')

    manager = WorkerManager(postgres_url)

    try:
        manager.run_scheduler()
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down...")
        manager.shutdown()
    except Exception as e:
        logger.error(f"Scheduler error: {e}")
        manager.shutdown()
