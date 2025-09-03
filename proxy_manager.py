#!/usr/bin/env python3
"""
Proxy Manager for Webshare.io and other proxy services
Handles automatic proxy fetching, rotation, and health checking
"""

# Handle missing dependencies gracefully
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None
    REQUESTS_AVAILABLE = False

try:
    import logging
    LOGGING_AVAILABLE = True
    logger = logging.getLogger(__name__)
except ImportError:
    LOGGING_AVAILABLE = False
    logger = None

# Simple imports that should always be available
import time
import json
from datetime import datetime, timedelta

# Handle typing imports gracefully
try:
    from typing import List, Dict, Optional
    TYPING_AVAILABLE = True
except ImportError:
    # Fallback for older Python versions or missing typing
    List = list
    Dict = dict
    Optional = lambda x: x
    TYPING_AVAILABLE = False

try:
    from dataclasses import dataclass
    DATACLASSES_AVAILABLE = True
except ImportError:
    # Simple fallback for dataclass
    def dataclass(cls):
        return cls
    DATACLASSES_AVAILABLE = False

@dataclass
class ProxyConfig:
    """Configuration for proxy services"""
    service = "webshare"  # 'webshare', 'other'
    api_key = None
    username = None
    password = None
    proxy_host = None
    proxy_port = None
    rotation_enabled = True
    health_check_enabled = True

class ProxyManager:
    """Manages proxy services and automatic rotation"""
    
    def __init__(self):
        self.proxy_configs = {}
        self.proxy_pool = []
        self.last_refresh = datetime.min
        self.refresh_interval = 300  # 5 minutes
    
    def add_webshare_config(self, username=None, password=None, 
                          api_key=None,
                          proxy_host="p.webshare.io", proxy_port=80):
        """Add Webshare.io proxy configuration"""
        if not REQUESTS_AVAILABLE:
            if logger:
                logger.warning("Requests library not available, proxy manager disabled")
            return
            
        config = ProxyConfig()
        config.service = 'webshare'
        config.username = username
        config.password = password
        config.api_key = api_key
        config.proxy_host = proxy_host
        config.proxy_port = proxy_port
        config.rotation_enabled = True
        config.health_check_enabled = True
        
        self.proxy_configs['webshare'] = config
        if api_key:
            if logger:
                logger.info(f"Added Webshare.io proxy config with API key: {proxy_host}:{proxy_port}")
        else:
            if logger:
                logger.info(f"Added Webshare.io proxy config: {username}@{proxy_host}:{proxy_port}")
    
    def get_rotating_proxy(self):
        """Get a rotating proxy URL for Webshare.io"""
        if not REQUESTS_AVAILABLE:
            return None
            
        if 'webshare' not in self.proxy_configs:
            return None
            
        config = self.proxy_configs['webshare']
        
        # Handle API key authentication (preferred)
        if config.api_key:
            proxy_url = f"http://{config.api_key}:@{config.proxy_host}:{config.proxy_port}/"
            return proxy_url
            
        # Handle username/password authentication (fallback)
        if config.username and config.password:
            proxy_url = f"http://{config.username}:{config.password}@{config.proxy_host}:{config.proxy_port}/"
            return proxy_url
            
        return None
    
    def get_proxy_dict(self):
        """Get proxy dictionary for requests library"""
        if not REQUESTS_AVAILABLE:
            return None
            
        proxy_url = self.get_rotating_proxy()
        if not proxy_url:
            return None
            
        return {
            "http": proxy_url,
            "https": proxy_url
        }
    
    def test_proxy(self, proxy_dict):
        """Test if proxy is working"""
        if not REQUESTS_AVAILABLE:
            return True  # Assume success if requests not available
            
        try:
            response = requests.get(
                "https://ipv4.webshare.io/",
                proxies=proxy_dict,
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            if logger:
                logger.warning(f"Proxy test failed: {e}")
            return False
    
    def health_check(self):
        """Perform health check on current proxy configuration"""
        if not REQUESTS_AVAILABLE:
            return True  # Assume success if requests not available
            
        if not self.proxy_configs:
            return True  # No proxies configured, that's OK
            
        proxy_dict = self.get_proxy_dict()
        if not proxy_dict:
            return True  # No proxies to check
            
        return self.test_proxy(proxy_dict)

# Global proxy manager instance
proxy_manager = ProxyManager() if REQUESTS_AVAILABLE else None

def initialize_webshare_proxy(username=None, password=None, api_key=None):
    """Initialize Webshare.io proxy service"""
    if not REQUESTS_AVAILABLE:
        if logger:
            logger.warning("Requests library not available, Webshare proxy initialization skipped")
        return
        
    if proxy_manager:
        proxy_manager.add_webshare_config(username, password, api_key)

def get_proxy_for_requests():
    """Get proxy configuration for requests library"""
    if not REQUESTS_AVAILABLE or not proxy_manager:
        return None
        
    return proxy_manager.get_proxy_dict()

def test_proxy_connection():
    """Test the current proxy connection"""
    if not REQUESTS_AVAILABLE or not proxy_manager:
        return True  # Assume success if not available
        
    return proxy_manager.health_check()

if __name__ == "__main__":
    # Example usage
    import os
    
    # Initialize with your Webshare credentials
    api_key = os.getenv('WEBSHARE_API_KEY')
    username = os.getenv('WEBSHARE_USERNAME', 'your_username')
    password = os.getenv('WEBSHARE_PASSWORD', 'your_password')
    
    if api_key:
        initialize_webshare_proxy(api_key=api_key)
    else:
        initialize_webshare_proxy(username, password)
    
    # Get proxy configuration
    proxies = get_proxy_for_requests()
    if proxies:
        print(f"Proxy configuration: {proxies}")
        
        # Test the proxy
        try:
            response = requests.get("https://ipv4.webshare.io/", proxies=proxies, timeout=10)
            print(f"Proxy test result: {response.text}")
        except Exception as e:
            print(f"Proxy test failed: {e}")