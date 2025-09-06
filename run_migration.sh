#!/bin/bash

# Webshare Proxy Setting Migration Script
# This script adds the use_webshare_proxies column to the scraping_workers table

echo "Webshare Proxy Setting Migration Script"
echo "======================================="

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo "Error: psql is not installed or not in PATH"
    exit 1
fi

# Check if POSTGRES_URL is set
if [ -z "$POSTGRES_URL" ]; then
    echo "Error: POSTGRES_URL environment variable is not set"
    echo ""
    echo "Please set the POSTGRES_URL environment variable:"
    echo "  export POSTGRES_URL='postgresql://user:password@host:port/database'"
    echo ""
    echo "Or run with:"
    echo "  POSTGRES_URL='postgresql://user:password@host:port/database' ./run_migration.sh"
    exit 1
fi

echo "Adding use_webshare_proxies column to scraping_workers table..."

# Extract database connection details from POSTGRES_URL
# This is a simple approach - for production use, you might want to use a proper URL parser
DB_URL=$POSTGRES_URL

# Run the migration SQL
psql "$DB_URL" -c "
-- Add the column with default value true
ALTER TABLE scraping_workers 
ADD COLUMN IF NOT EXISTS use_webshare_proxies BOOLEAN DEFAULT true;

-- Add a comment to describe the column
COMMENT ON COLUMN scraping_workers.use_webshare_proxies IS 'Enable/disable Webshare.io proxy integration for this worker';

-- Verify the column was added
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns 
WHERE table_name = 'scraping_workers' 
AND column_name = 'use_webshare_proxies';
"

if [ $? -eq 0 ]; then
    echo ""
    echo "Migration completed successfully!"
    echo "The 'use_webshare_proxies' column has been added to the scraping_workers table."
else
    echo ""
    echo "Error: Migration failed. Please check the error messages above."
    exit 1
fi