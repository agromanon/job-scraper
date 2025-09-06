#!/usr/bin/env python3
"""
Migration script to add use_webshare_proxies column to scraping_workers table
This script adds the use_webshare_proxies column if it doesn't exist yet.
"""

import os
import sys

# Handle missing dependencies gracefully
try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    psycopg2 = None
    PSYCOPG2_AVAILABLE = False
    print("Warning: psycopg2 not available, using fallback method")

try:
    import logging
    LOGGING_AVAILABLE = True
    logger = logging.getLogger(__name__)
except ImportError:
    LOGGING_AVAILABLE = False
    logger = None

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def get_db_connection():
    """Get database connection using environment variables"""
    postgres_url = os.getenv('POSTGRES_URL')
    if not postgres_url:
        raise Exception("POSTGRES_URL environment variable not set")
    
    if not PSYCOPG2_AVAILABLE:
        raise Exception("psycopg2 not available")
    
    return psycopg2.connect(postgres_url)

def migrate_database():
    """Add use_webshare_proxies column to scraping_workers table"""
    if not PSYCOPG2_AVAILABLE:
        print("Skipping database migration - psycopg2 not available")
        return
        
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if column already exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'scraping_workers' 
            AND column_name = 'use_webshare_proxies'
        """)
        
        if cursor.fetchone():
            print("Column 'use_webshare_proxies' already exists. Migration not needed.")
        else:
            # Add the column
            cursor.execute("""
                ALTER TABLE scraping_workers 
                ADD COLUMN use_webshare_proxies BOOLEAN DEFAULT true
            """)
            print("Successfully added 'use_webshare_proxies' column to scraping_workers table.")
        
        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Database migration completed successfully.")
        
    except Exception as e:
        print(f"Error during migration: {e}")
        if logger:
            logger.error(f"Migration error: {e}")
        sys.exit(1)

def show_help():
    """Show help message"""
    print("""
Webshare Proxy Setting Migration Script
=======================================

This script adds the 'use_webshare_proxies' column to the scraping_workers table.

Usage:
    python3 migrate_webshare_setting.py [OPTIONS]

Options:
    --help, -h    Show this help message
    --dry-run     Check if migration is needed without making changes

Environment Variables:
    POSTGRES_URL  Database connection string (required)

If you're using EasyPanel, you can run this migration by:
1. Uploading this script to your VPS
2. Setting the POSTGRES_URL environment variable
3. Running: python3 migrate_webshare_setting.py

Example:
    export POSTGRES_URL="postgresql://user:pass@host:port/dbname"
    python3 migrate_webshare_setting.py
    """)

if __name__ == "__main__":
    # Handle command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] in ['--help', '-h']:
            show_help()
            sys.exit(0)
        elif sys.argv[1] == '--dry-run':
            print("Dry run mode - checking if migration is needed...")
            # Just check if column exists
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'scraping_workers' 
                    AND column_name = 'use_webshare_proxies'
                """)
                
                if cursor.fetchone():
                    print("Column 'use_webshare_proxies' already exists. No migration needed.")
                else:
                    print("Column 'use_webshare_proxies' is missing. Migration is needed.")
                
                cursor.close()
                conn.close()
            except Exception as e:
                print(f"Error during dry run: {e}")
            sys.exit(0)
    
    migrate_database()