# Hybrid VPS + Supabase Architecture Guide
# Cost-effective data processing pipeline

## Overview
This guide implements a smart data pipeline that uses your VPS for heavy processing and only sends clean, deduplicated data to expensive Supabase cloud storage.

## Architecture Flow

```
Raw Job Scraping 
       ↓
[VPS PostgreSQL]  ← Heavy processing, raw storage, advanced dedup
       ↓
Data Cleaning & Enhancement (VPS)
       ↓
Supabase Cloud  ← Clean data only, minimal storage cost
```

## Step 1: Environment Variables Setup

Add these to EasyPanel frontend service environment:

```bash
# VPS PostgreSQL (Raw Data)
VPS_POSTGRES_URL=postgresql://aromanon:password@localhost:5432/job-data

# Supabase Cloud (Clean Data)  
SUPABASE_URL=your-supabase-project-url
SUPABASE_KEY=your-supabase-service-role-key
SUPABASE_DATABASE_URL=postgresql://postgres:password@db.project-ref.supabase.co:5432/postgres

# Data Processing Settings
CLEAN_DATA_SCHEDULE_HOURS=24  # How often to sync clean data to Supabase
CLEAN_DATA_BATCH_SIZE=1000    # Batch size for syncing to Supabase
DATA_RETENTION_DAYS_VPS=30    # Keep raw data for 30 days on VPS
```

## Step 2: Database Configuration in Admin Dashboard

### Database 1: VPS Processing (Raw Data)
```
Name: VPS Processing Database
Description: Raw scraped job data with full deduplication
Host: localhost  
Port: 5432
Database Name: job-data
Username: aromanon
Password: your_vps_password
SSL Mode: prefer
Target Table Prefix: raw_job_listings_
Batch Size: 500
Deduplication Method: unique_id
Deduplication Fields: job_url
```

### Database 2: Supabase Cloud (Clean Data)
```
Name: Supabase Production Cloud
Description: Clean, enhanced job data for production use
Host: db.your-project-ref.supabase.co
Port: 5432
Database Name: postgres
Username: postgres
Password: your_supabase_password
SSL Mode: require
Target Table Prefix: clean_job_listings_
Batch Size: 1000
Deduplication Method: composite_key
Deduplication Fields: 
title
company_name
location_city
```

## Step 3: Worker Strategy

### Type A: Raw Scraping Workers (VPS-Focused)
Create workers for each job site targeting the **VPS Processing Database**:

**LinkedIn Raw Worker:**
```
Name: LinkedIn Raw Scraper
Database: VPS Processing Database
Table Name: raw_job_listings_linkedin
Search Term: developer OR engineer OR software
Location: Brazil
Results Per Run: 200
Schedule Hours: 2
Job Type: FULL_TIME,CONTRACT
```

**Indeed Raw Worker:**
```
Name: Indeed Raw Scraper  
Database: VPS Processing Database
Table Name: raw_job_listings_indeed
Search Term: developer OR engineer
Location: Remote
Results Per Run: 150
Schedule Hours: 3
```

### Type B: Data Sync Workers (Supabase-Focused)
Create workers that process and sync clean data to **Supabase**:

**LinkedIn Clean Sync Worker:**
```
Name: LinkedIn to Supabase Sync
Database: Supabase Production Cloud
Table Name: clean_job_listings_linkedin
Schedule Hours: 24  # Run daily to save Supabase resources
Results Per Run: 1000
Advanced Mode: SQL Query (see below)
```

## Step 4: Data Processing SQL Queries

### Create these SQL queries for your sync workers:

#### Query 1: Clean and Enhance LinkedIn Data
```sql
-- Run this on VPS, then sync results to Supabase
SELECT 
    ROW_NUMBER() OVER (ORDER BY scraped_at DESC) as id,
    'linkedin' as source_site,
    title,
    company_name,
    COALESCE(location_city, 'Remote') as location_city,
    COALESCE(location_country, 'BR') as location_country,
    is_remote,
    job_type,
    description,
    job_url,
    DATE_TRUNC('day', scraped_at) as processing_date,
    CASE 
        WHEN title ILIKE '%senior%' OR title ILIKE '%lead%' OR title ILIKE '%principal%' THEN 'Senior'
        WHEN title ILIKE '%junior%' OR title ILIKE '%trainee%' OR title ILIKE '%intern%' THEN 'Junior'  
        ELSE 'Mid-level'
    END as experience_level,
    CASE 
        WHEN title ILIKE '%python%' OR title ILIKE '%java%' OR title ILIKE '%javascript%' THEN 'tech'
        WHEN title ILIKE '%data%' OR title ILIKE '%analyst%' THEN 'data' 
        WHEN title ILIKE '%devops%' OR title ILIKE '%cloud%' THEN 'devops'
        ELSE 'general'
    END as category,
    scraped_at as created_at
FROM raw_job_listings_linkedin
WHERE scraped_at >= CURRENT_DATE - INTERVAL '7 days'
  AND (description IS NOT NULL AND LENGTH(description) > 100)
  AND (company_name NOT ILIKE '%test%' AND company_name NOT ILIKE '%demo%')
LIMIT 1000
```

#### Query 2: Remove Old Raw Data (VPS Cleanup)
```sql
-- Keep only recent raw data on VPS to save storage
DELETE FROM raw_job_listings_linkedin 
WHERE scraped_at < CURRENT_DATE - INTERVAL '30 days';

DELETE FROM raw_job_listings_indeed 
WHERE scraped_at < CURRENT_DATE - INTERVAL '30 days';
```

## Step 5: Enhanced Worker Manager

Add this data processing capability to your `worker_manager.py`:

```python
class DataProcessingWorker:
    def __init__(self, vps_db_config, supabase_db_config):
        self.vps_db = vps_db_config
        self.supabase_db = supabase_db_config
    
    def process_and_sync_clean_data(self, source_table: str, target_table: str):
        """Process VPS raw data and sync clean data to Supabase"""
        
        # 1. Extract clean data from VPS
        clean_data = self._extract_clean_data(source_table)
        
        # 2. Transform and enhance data
        enhanced_data = self._enhance_job_data(clean_data)
        
        # 3. Load to Supabase
        sync_result = self._sync_to_supabase(enhanced_data, target_table)
        
        # 4. Clean up old VPS data
        self._cleanup_old_data(source_table)
        
        return sync_result
    
    def _extract_clean_data(self, source_table: str):
        """Extract high-quality job listings from raw data"""
        query = f"""
        SELECT DISTINCT ON (job_url) 
            *,
            CASE 
                WHEN LENGTH(description) > 200 AND 
                     company_name NOT ILIKE '%test%' AND
                     scraped_at > CURRENT_DATE - INTERVAL '7 days' 
                THEN true ELSE false 
            END as is_quality_data
        FROM {source_table} 
        WHERE scraped_at > CURRENT_DATE - INTERVAL '30 days'
        ORDER BY job_url, scraped_at DESC
        """
        return self._execute_query(query, self.vps_db)
    
    def _enhance_job_data(self, raw_data):
        """Add computed fields for better filtering"""
        enhanced = []
        for job in raw_data:
            if job.get('is_quality_data'):
                # Add experience level
                job['experience_level'] = self._calculate_experience_level(job)
                # Add job category
                job['category'] = self._calculate_category(job)
                # Add quality score
                job['quality_score'] = self._calculate_quality_score(job)
                enhanced.append(job)
        return enhanced
    
    def _sync_to_supabase(self, clean_data, target_table):
        """Sync enhanced data to Supabase with proper deduplication"""
        conn = self._get_connection(self.supabase_db)
        cursor = conn.cursor()
        
        for batch in self._chunk_data(clean_data, 100):
            # Use UPSERT to avoid duplicates in Supabase
            upsert_query = f"""
            INSERT INTO {target_table} (source_site, title, company_name, location_city, 
                                        experience_level, category, quality_score, job_url, 
                                        description, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (title, company_name, location_city) 
            DO UPDATE SET 
                description = EXCLUDED.description,
                quality_score = EXCLUDED.quality_score,
                created_at = EXCLUDED.created_at
            """
            cursor.executemany(upsert_query, batch)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {'synced_records': len(clean_data)}
```

## Step 6: Cost Optimization Settings

### VPS Database Settings
- **Retention**: 30 days for raw data
- **Table Compression**: Enable
- **Index Strategy**: Optimize for insert performance
- **Maintenance**: Weekly VACUUM and ANALYZE

### Supabase Database Settings  
- **Retention**: Permanent for clean data
- **Table Compression**: Enable
- **Index Strategy**: Optimize for read performance
- **Row Level Security**: Enable for production safety

## Step 7: Monitoring and Analytics

Add these views to track cost savings:

```sql
-- Create in both databases
CREATE VIEW cost_savings_view AS
SELECT 
    'vps' as storage_location,
    COUNT(*) as total_records,
    COUNT(*) * 0.001 as estimated_supabase_cost,
    0 as actual_cost
FROM raw_job_listings_linkedin
WHERE scraped_at > CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT 
    'supabase' as storage_location,
    COUNT(*) as total_records,
    0 as estimated_supabase_cost,
    COUNT(*) * 0.006 as actual_cost
FROM clean_job_listings_linkedin;
```

## Expected Cost Savings

### Current Approach (All in Supabase):
- 10,000 raw jobs/day × $0.006/row = **$60/day**
- **Monthly cost**: ~$1,800

### Hybrid Approach (VPS + Supabase):
- 10,000 raw jobs/day on VPS = **$0** (included in VPS cost)
- 1,000 clean jobs/day × $0.006/row = **$6/day**
- **Monthly cost**: ~$180

### **Savings: 90% reduction in Supabase costs!**

## Implementation Checklist

- [ ] Add environment variables to EasyPanel
- [ ] Create VPS Processing Database connection  
- [ ] Create Supabase Production Cloud connection
- [ ] Set up Raw Scraping Workers (VPS-focused)
- [ ] Set up Data Sync Workers (Supabase-focused)
- [ ] Implement data processing queries
- [ ] Add cost monitoring views
- [ ] Test data pipeline flow
- [ ] Monitor Supabase usage and cost

This hybrid approach gives you the best of both worlds: heavy processing on your affordable VPS and clean, efficient storage on scalable Supabase cloud!