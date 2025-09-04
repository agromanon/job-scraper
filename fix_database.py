#!/usr/bin/env python3
"""
Direct database fix script to add missing columns
"""

import os
import psycopg2

# Database connection
DATABASE_URL = os.getenv('POSTGRES_URL', 'postgresql://postgres:password@localhost:5432/job_scraping')

def add_column_if_not_exists(cursor, table_name, column_name, column_definition):
    """Add column if it doesn't exist"""
    try:
        cursor.execute(f"""
            ALTER TABLE {table_name} 
            ADD COLUMN IF NOT EXISTS {column_name} {column_definition}
        """)
        print(f"Column {column_name} added/verified successfully!")
        return True
    except Exception as e:
        print(f"Error adding column {column_name}: {e}")
        return False

def fix_database():
    """Add missing columns directly"""
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        print("Adding missing columns to scraping_workers table...")
        
        # List of columns that might be missing with their definitions
        missing_columns = [
            ('current_offset', 'INTEGER DEFAULT 0'),
            ('max_consecutive_errors', 'INTEGER DEFAULT 5'),
            ('auto_pause_on_errors', 'BOOLEAN DEFAULT true'),
            ('status', "VARCHAR(20) DEFAULT 'active'"),
            ('last_run', 'TIMESTAMP'),
            ('next_run', 'TIMESTAMP'),
            ('last_success', 'TIMESTAMP'),
            ('last_error', 'TEXT'),
            ('consecutive_errors', 'INTEGER DEFAULT 0'),
            ('memory_limit_mb', 'INTEGER DEFAULT 512'),
            ('cpu_limit_cores', 'DECIMAL(3,1) DEFAULT 0.5'),
            ('max_runtime_minutes', 'INTEGER DEFAULT 60'),
            ('tags', 'TEXT[]'),
            ('created_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'),
            ('updated_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        ]
        
        # Add each missing column
        for column_name, column_definition in missing_columns:
            add_column_if_not_exists(cursor, 'scraping_workers', column_name, column_definition)
        
        conn.commit()
        print("All missing columns processed!")
        
        # Verify some key columns exist
        key_columns = ['current_offset', 'max_consecutive_errors', 'auto_pause_on_errors']
        for column in key_columns:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'scraping_workers' 
                AND column_name = %s
            """, (column,))
            
            result = cursor.fetchone()
            if result:
                print(f"Verification: Column {column} exists!")
            else:
                print(f"WARNING: Column {column} still doesn't exist!")
        
        cursor.close()
        conn.close()
        
        print("Database fix completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    fix_database()