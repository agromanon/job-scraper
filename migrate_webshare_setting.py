#!/usr/bin/env python3
"""
Migration script to add use_webshare_proxies column to scraping_workers table
This script adds the use_webshare_proxies column if it doesn't exist yet.
"""

import os
import sys
import psycopg2
from psycopg2 import sql

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def get_db_connection():
    """Get database connection using environment variables"""
    postgres_url = os.getenv('POSTGRES_URL')
    if not postgres_url:
        raise Exception("POSTGRES_URL environment variable not set")
    
    return psycopg2.connect(postgres_url)

def migrate_database():
    """Add use_webshare_proxies column to scraping_workers table"""
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
        sys.exit(1)

if __name__ == "__main__":
    migrate_database()