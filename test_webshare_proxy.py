#!/usr/bin/env python3
"""
Test script to verify Webshare proxy integration
"""

import os
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_webshare_proxy():
    """Test Webshare proxy integration"""
    logger.info("=== Webshare Proxy Integration Test ===")
    
    # Check if running in the correct directory
    if not os.path.exists('proxy_manager.py'):
        logger.error("proxy_manager.py not found in current directory")
        logger.info("Please run this script from the project root directory")
        return False
    
    # Test importing proxy manager
    logger.info("1. Testing proxy manager import...")
    try:
        from proxy_manager import proxy_manager, initialize_webshare_proxy, REQUESTS_AVAILABLE
        logger.info(f"   Proxy manager imported successfully. Requests available: {REQUESTS_AVAILABLE}")
        logger.info(f"   Proxy manager instance: {proxy_manager}")
    except ImportError as e:
        logger.error(f"   Failed to import proxy manager: {e}")
        return False
    
    # Check environment variables
    logger.info("2. Checking Webshare credentials in environment...")
    webshare_api_key = os.getenv('WEBSHARE_API_KEY')
    webshare_username = os.getenv('WEBSHARE_USERNAME')
    webshare_password = os.getenv('WEBSHARE_PASSWORD')
    
    logger.info(f"   WEBSHARE_API_KEY: {'*' * len(webshare_api_key) if webshare_api_key else None}")
    logger.info(f"   WEBSHARE_USERNAME: {webshare_username}")
    logger.info(f"   WEBSHARE_PASSWORD: {'*' * len(webshare_password) if webshare_password else None}")
    
    if not webshare_api_key and not (webshare_username and webshare_password):
        logger.error("   No Webshare credentials found in environment")
        logger.info("   Please set WEBSHARE_API_KEY or both WEBSHARE_USERNAME and WEBSHARE_PASSWORD")
        return False
    
    # Initialize Webshare proxy
    logger.info("3. Initializing Webshare proxy...")
    try:
        if webshare_api_key:
            initialize_webshare_proxy(api_key=webshare_api_key)
            logger.info("   Initialized Webshare proxy with API key")
        elif webshare_username and webshare_password:
            initialize_webshare_proxy(webshare_username, webshare_password)
            logger.info("   Initialized Webshare proxy with username/password")
    except Exception as e:
        logger.error(f"   Failed to initialize Webshare proxy: {e}")
        import traceback
        logger.error(f"   Traceback: {traceback.format_exc()}")
        return False
    
    # Test getting proxies
    logger.info("4. Testing proxy retrieval...")
    try:
        if proxy_manager:
            # Refresh proxy pool
            logger.info("   Refreshing proxy pool...")
            proxy_manager.refresh_proxy_pool()
            
            # Get rotating proxy
            logger.info("   Getting rotating proxy...")
            rotating_proxy = proxy_manager.get_proxy_dict()
            logger.info(f"   Rotating proxy: {rotating_proxy}")
            
            # Get individual proxy
            logger.info("   Getting individual proxy...")
            individual_proxy = proxy_manager.get_next_proxy()
            logger.info(f"   Individual proxy: {individual_proxy}")
            
            if rotating_proxy or individual_proxy:
                logger.info("   Successfully retrieved proxies from Webshare")
            else:
                logger.warning("   No proxies retrieved from Webshare")
        else:
            logger.error("   Proxy manager is None")
            return False
    except Exception as e:
        logger.error(f"   Failed to get proxies: {e}")
        import traceback
        logger.error(f"   Traceback: {traceback.format_exc()}")
        return False
    
    logger.info("=== Webshare Proxy Integration Test Complete ===")
    return True

if __name__ == "__main__":
    success = test_webshare_proxy()
    if success:
        logger.info("Webshare proxy integration test PASSED")
        sys.exit(0)
    else:
        logger.error("Webshare proxy integration test FAILED")
        sys.exit(1)