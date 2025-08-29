# Supabase Database Configuration Guide

## Overview
This guide shows how to configure your Job Scraper application to send deduplicated job data to a Supabase cloud database.

## Where to Configure Supabase Connection

### 1. EasyPanel Environment Variables
 login to your EasyPanel dashboard and add these environment variables to your frontend service:

**Required Environment Variables:**
```
SUPABASE_URL=your-supabase-project-url
SUPABASE_KEY=your-supabase-service-role-key
SUPABASE_DATABASE_URL=postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres
```

**How to get these values:**
1. Go to your [Supabase Dashboard](https://app.supabase.com)
2. Select your project
3. Go to Settings → API
   - **SUPABASE_URL**: Project URL (e.g., https://your-project-id.supabase.co)
   - **SUPABASE_KEY**: service_role key (under "Project API keys")
4. Go to Settings → Database
   - **SUPABASE_DATABASE_URL**: Connection string with your database password

### 2. Database Configuration in Admin Interface
After setting up the environment variables, you'll need to add the Supabase database through the web interface:

1. **Login to your Job Scraper dashboard** at https://my-job-scraper.neuroworks.com.br/
   - Default password: `admin123`
   
2. **Add Supabase Database Connection:**
   - Click "Databases" in the navigation
   - Click "Add Database"
   - Fill in the connection details:
     - **Name**: `Supabase Production`
     - **Description**: `Supabase cloud database for job listings`
     - **Host**: Your Project Reference (e.g., `db.your-project-id.supabase.co`)
     - **Port**: `5432`
     - **Database Name**: `postgres`
     - **Username**: `postgres`
     - **Password**: Your Supabase database password
     - **SSL Mode**: `require`

### 3. Configure Workers to Use Supabase
When creating or editing workers:

1. **Go to Workers → Create Worker**
2. **Configure Worker Settings:**
   - **Name**: Give your worker a descriptive name
   - **Site**: Choose job site (LinkedIn, Indeed, etc.)
   - **Database**: Select your "Supabase Production" database
   - **Table Name**: `job_listings_[your_site_name]` (e.g., `job_listings_linkedin`)
3. **Set up deduplication:**
   - **Deduplication Method**: `unique_id`
   - **Deduplication Fields**: `job_url`

## Application Code Updates

### Worker Manager Updates
The `worker_manager.py` and worker configuration system already supports multiple databases, including Supabase. No additional code changes are needed - just add the Supabase connection through the admin interface.

### Supabase Schema
The application will automatically create the necessary tables in your Supabase database when you run workers. The schema includes:

```sql
-- These tables will be auto-created by worker_manager.py
CREATE TABLE job_listings_* (
    id SERIAL PRIMARY KEY,
    title TEXT,
    company TEXT,
    location TEXT,
    job_url TEXT UNIQUE,
    description TEXT,
    salary_min NUMERIC,
    salary_max NUMERIC,
    currency VARCHAR(10),
    job_type VARCHAR(50),
    experience_level VARCHAR(50),
    posted_at TIMESTAMP,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_site VARCHAR(50),
    source_country VARCHAR(10),
    is_remote BOOLEAN DEFAULT false,
    metadata JSONB,
    search_term TEXT,
    search_location TEXT
);

-- Indexes for performance (auto-created)
CREATE INDEX idx_job_listings_url ON job_listings_* (job_url);
CREATE INDEX idx_job_listings_company ON job_listings_* (company);
CREATE INDEX idx_job_listings_posted_at ON job_listings_* (posted_at);
```

## Environment Variables in EasyPanel

To add environment variables in EasyPanel:

1. **Go to your EasyPanel dashboard**
2. **Select your frontend service** (my-job-scraper-fe)
3. **Click on "Environment" tab**
4. **Add the following variables:**
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_KEY`: Your Supabase service role key
   - `SUPABASE_DATABASE_URL`: Your PostgreSQL connection string

## Testing the Connection

1. **After adding the database in the admin interface:**
   - Go to Databases
   - Click the "Test" button next to your Supabase database
   - You should see a "Database connection successful" message

2. **Test data flow:**
   - Create a test worker targeting your Supabase database
   - Run the worker manually
   - Check your Supabase dashboard for inserted data

## Security Best Practices

1. **Password Security:** Store your Supabase password only in environment variables, never in code
2. **Connection Security:** Use SSL mode `require` for encrypted connections
3. **Access Control:** Use the service role key only for backend operations
4. **Data Privacy:** Ensure sensitive job seeker data is handled according to privacy laws

## Monitoring and Troubleshooting

### Check connection status:
- In Admin Dashboard: Databases → Your Supabase DB → Look for connection status
- In Supabase Dashboard: Monitor active connections and query performance

### Common issues:
1. **Connection timeouts:** Check firewall settings and SSL mode
2. **Authentication errors:** Verify password and SSL configuration
3. **Permission errors:** Ensure the postgres user has table creation permissions

## Next Steps

1. **Deploy these changes:** Commit and push the updated Dockerfile
2. **Add Supabase credentials** in EasyPanel environment variables
3. **Test the connection** through the admin interface
4. **Create a test worker** to verify data flow to Supabase