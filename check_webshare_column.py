#!/usr/bin/env python3
"""
Diagnostic script to check if use_webshare_proxies column exists in scraping_workers table
"""

import os
import sys

# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def check_webshare_column():
    """Check if use_webshare_proxies column exists in scraping_workers table"""
    try:
        # Import psycopg2
        import psycopg2
        print("✓ psycopg2 imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import psycopg2: {e}")
        print("Please install psycopg2: pip install psycopg2-binary")
        return False
    
    # Get database connection
    postgres_url = os.getenv('POSTGRES_URL')
    if not postgres_url:
        print("✗ POSTGRES_URL environment variable not set")
        print("Please set the POSTGRES_URL environment variable with your database connection string")
        return False
    
    print(f"✓ POSTGRES_URL environment variable found")
    
    try:
        # Connect to database
        conn = psycopg2.connect(postgres_url)
        cursor = conn.cursor()
        print("✓ Connected to database successfully")
        
        # Check if scraping_workers table exists
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name = 'scraping_workers'
        """)
        
        if not cursor.fetchone():
            print("✗ scraping_workers table does not exist")
            cursor.close()
            conn.close()
            return False
        
        print("✓ scraping_workers table exists")
        
        # Check if use_webshare_proxies column exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'scraping_workers' 
            AND column_name = 'use_webshare_proxies'
        """)
        
        result = cursor.fetchone()
        if result:
            print(f"✓ Column 'use_webshare_proxies' exists: {result[0]}")
            
            # Get column details
            cursor.execute("""
                SELECT 
                    column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = 'scraping_workers' 
                AND column_name = 'use_webshare_proxies'
            """)
            
            details = cursor.fetchone()
            if details:
                print(f"  Column details:")
                print(f"    Name: {details[0]}")
                print(f"    Type: {details[1]}")
                print(f"    Nullable: {details[2]}")
                print(f"    Default: {details[3]}")
        else:
            print("✗ Column 'use_webshare_proxies' does not exist")
            
            # List all columns in the table to see what's actually there
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = 'scraping_workers' 
                ORDER BY ordinal_position
            """)
            
            columns = cursor.fetchall()
            print("\nAll columns in scraping_workers table:")
            for column in columns:
                print(f"  - {column[0]} ({column[1]})")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Database connection error: {e}")
        return False

if __name__ == "__main__":
    print("Webshare Column Diagnostic Script")
    print("=" * 40)
    
    success = check_webshare_column()
    
    if success:
        print("\nDiagnostic completed successfully")
    else:
        print("\nDiagnostic failed - please check the error messages above")
        sys.exit(1)