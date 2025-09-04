#!/usr/bin/env python3
"""
Direct database fix script to add current_offset column
"""

import os
import psycopg2

# Database connection
DATABASE_URL = os.getenv('POSTGRES_URL', 'postgresql://postgres:password@localhost:5432/job_scraping')

def fix_database():
    """Add current_offset column directly"""
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        print("Checking if current_offset column exists...")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'scraping_workers' 
            AND column_name = 'current_offset'
        """)
        
        result = cursor.fetchone()
        if result:
            print("Column current_offset already exists!")
        else:
            print("Adding current_offset column...")
            cursor.execute("""
                ALTER TABLE scraping_workers 
                ADD COLUMN current_offset INTEGER DEFAULT 0
            """)
            conn.commit()
            print("Column added successfully!")
        
        # Verify the column exists now
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'scraping_workers' 
            AND column_name = 'current_offset'
        """)
        
        result = cursor.fetchone()
        if result:
            print("Verification: Column current_offset exists!")
        else:
            print("ERROR: Column still doesn't exist!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    fix_database()