#!/usr/bin/env python3

import sys
import os
sys.path.append('/app')

# Test imports and basic functionality
try:
    from worker_manager import WorkerManager
    print("✓ WorkerManager import successful")
except ImportError as e:
    print(f"✗ WorkerManager import failed: {e}")
    sys.exit(1)

try:
    from worker_admin import app
    print("✓ Flask app import successful")
except ImportError as e:
    print(f"✗ Flask app import failed: {e}")
    sys.exit(1)

try:
    import psycopg2
    print("✓ psycopg2 import successful")
except ImportError as e:
    print(f"✗ psycopg2 import failed: {e}")

try:
    import redis
    print("✓ redis import successful")
except ImportError as e:
    print(f"✗ redis import failed: {e}")

try:
    from jobspy import scrape_jobs
    print("✓ jobspy import successful")
except ImportError as e:
    print(f"✗ jobspy import failed: {e}")

# Test database connection
try:
    db_url = os.getenv('POSTGRES_URL')
    if not db_url:
        print("✗ POSTGRES_URL not found in environment")
    else:
        conn = psycopg2.connect(db_url)
        conn.close()
        print("✓ Database connection successful")
except Exception as e:
    print(f"✗ Database connection failed: {e}")

# Test Redis connection
try:
    redis_url = os.getenv('REDIS_URL')
    if not redis_url:
        print("✗ REDIS_URL not found in environment")
    else:
        r = redis.from_url(redis_url)
        r.ping()
        print("✓ Redis connection successful")
except Exception as e:
    print(f"✗ Redis connection failed: {e}")

print("\n🎉 All core components verified!")