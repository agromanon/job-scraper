#!/usr/bin/env python3
"""
Proxy Manager for Webshare.io and other proxy services
Handles automatic proxy fetching, rotation, and health checking
"""

import requests
import logging
import time
import json
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

@dataclass
class ProxyConfig:
    """Configuration for proxy services"""
    service: str  # 'webshare', 'other'
    api_key: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    proxy_host: Optional[str] = None
    proxy_port: Optional[int] = None
    rotation_enabled: bool = True
    health_check_enabled: bool = True

class ProxyManager:
    """Manages proxy services and automatic rotation"""
    
    def __init__(self):
        self.proxy_configs: Dict[str, ProxyConfig] = {}
        self.proxy_pool: List[str] = []
        self.last_refresh: datetime = datetime.min
        self.refresh_interval: int = 300  # 5 minutes
    
    def add_webshare_config(self, username: str, password: str, 
                          proxy_host: str = "p.webshare.io", proxy_port: int = 80):
        """Add Webshare.io proxy configuration"""
        config = ProxyConfig(
            service='webshare',
            username=username,
            password=password,
            proxy_host=proxy_host,
            proxy_port=proxy_port,
            rotation_enabled=True,
            health_check_enabled=True
        )
        self.proxy_configs['webshare'] = config
        logger.info(f"Added Webshare.io proxy config: {username}@{proxy_host}:{proxy_port}")
    
    def get_rotating_proxy(self) -> Optional[str]:
        """Get a rotating proxy URL for Webshare.io"""
        if 'webshare' not in self.proxy_configs:
            return None
            
        config = self.proxy_configs['webshare']
        if not config.username or not config.password:
            return None
            
        # Webshare rotating proxy format
        proxy_url = f"http://{config.username}:{config.password}@{config.proxy_host}:{config.proxy_port}/"
        return proxy_url
    
    def get_proxy_dict(self) -> Optional[Dict[str, str]]:
        """Get proxy dictionary for requests library"""
        proxy_url = self.get_rotating_proxy()
        if not proxy_url:
            return None
            
        return {
            "http": proxy_url,
            "https": proxy_url
        }
    
    def test_proxy(self, proxy_dict: Dict[str, str]) -> bool:
        """Test if proxy is working"""
        try:
            response = requests.get(
                "https://ipv4.webshare.io/",
                proxies=proxy_dict,
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Proxy test failed: {e}")
            return False
    
    def health_check(self) -> bool:
        """Perform health check on current proxy configuration"""
        if not self.proxy_configs:
            return True  # No proxies configured, that's OK
            
        proxy_dict = self.get_proxy_dict()
        if not proxy_dict:
            return True  # No proxies to check
            
        return self.test_proxy(proxy_dict)

# Global proxy manager instance
proxy_manager = ProxyManager()

def initialize_webshare_proxy(username: str, password: str):
    """Initialize Webshare.io proxy service"""
    proxy_manager.add_webshare_config(username, password)

def get_proxy_for_requests() -> Optional[Dict[str, str]]:
    """Get proxy configuration for requests library"""
    return proxy_manager.get_proxy_dict()

def test_proxy_connection() -> bool:
    """Test the current proxy connection"""
    return proxy_manager.health_check()

if __name__ == "__main__":
    # Example usage
    import os
    
    # Initialize with your Webshare credentials
    username = os.getenv('WEBSHARE_USERNAME', 'your_username')
    password = os.getenv('WEBSHARE_PASSWORD', 'your_password')
    
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