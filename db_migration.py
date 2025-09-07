#!/usr/bin/env python3
"""
Database Migration Script for JobSpy
Adds the use_webshare_proxies column to the scraping_workers table if it doesn't exist
"""

import os
import sys
import psycopg2
from psycopg2 import sql

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
            cursor.close()
            conn.close()
            return True
        
        # Add the column if it doesn't exist
        print("Adding 'use_webshare_proxies' column to scraping_workers table...")
        cursor.execute("""
            ALTER TABLE scraping_workers 
            ADD COLUMN use_webshare_proxies BOOLEAN DEFAULT true
        """)
        
        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Successfully added 'use_webshare_proxies' column to scraping_workers table.")
        return True
        
    except Exception as e:
        print(f"Error during migration: {e}")
        return False

def rollback_migration():
    """Remove use_webshare_proxies column from scraping_workers table (rollback)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if column exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'scraping_workers' 
            AND column_name = 'use_webshare_proxies'
        """)
        
        if not cursor.fetchone():
            print("Column 'use_webshare_proxies' does not exist. Rollback not needed.")
            cursor.close()
            conn.close()
            return True
        
        # Remove the column if it exists
        print("Removing 'use_webshare_proxies' column from scraping_workers table...")
        cursor.execute("""
            ALTER TABLE scraping_workers 
            DROP COLUMN use_webshare_proxies
        """)
        
        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Successfully removed 'use_webshare_proxies' column from scraping_workers table.")
        return True
        
    except Exception as e:
        print(f"Error during rollback: {e}")
        return False

def show_help():
    """Show help message"""
    print("""
JobSpy Database Migration Script
===============================

This script adds the 'use_webshare_proxies' column to the scraping_workers table.

Usage:
    python3 db_migration.py [OPTIONS]

Options:
    --help, -h    Show this help message
    --rollback    Remove the column (rollback migration)
    --dry-run     Check if migration is needed without making changes

Environment Variables:
    POSTGRES_URL  Database connection string (required)

If you're using EasyPanel, you can run this migration by:
1. Uploading this script to your VPS
2. Setting the POSTGRES_URL environment variable
3. Running: python3 db_migration.py

Example:
    export POSTGRES_URL="postgresql://user:pass@host:port/database"
    python3 db_migration.py
    """)

if __name__ == "__main__":
    # Handle command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] in ['--help', '-h']:
            show_help()
            sys.exit(0)
        elif sys.argv[1] == '--rollback':
            print("Rolling back database migration...")
            success = rollback_migration()
            sys.exit(0 if success else 1)
        elif sys.argv[1] == '--dry-run':
            print("Dry run mode - checking if migration is needed...")
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
    
    # Run the migration
    print("Running database migration...")
    success = migrate_database()
    sys.exit(0 if success else 1)