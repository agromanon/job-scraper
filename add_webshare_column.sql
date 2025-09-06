-- SQL script to add use_webshare_proxies column to scraping_workers table
-- This script adds the column if it doesn't exist yet

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