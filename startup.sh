#!/bin/bash
set -e

# Run database fix script
echo "Running database fix script..."
cd /app && python3 fix_database.py

# Start the main application
echo "Starting job scraper application..."
exec python worker_admin.py