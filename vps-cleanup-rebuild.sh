#!/bin/bash

# VPS Deployment Cleanup Script
# Use this script to force a complete rebuild on your VPS
# This removes all Docker images, containers, and rebuilds everything from scratch

set -e  # Exit on any error

echo "=== VPS DEPLOYMENT CLEANUP SCRIPT ==="
echo "This will completely rebuild your application with latest changes"
echo

# Backup current state
echo "1. Creating backup of current deployment..."
docker-compose down > /dev/null 2>&1 || true
docker exec my-job-scraper-fe pg_dump -U aromanon -d job-data > backup_$(date +%Y%m%d_%H%M%S).sql 2>/dev/null || echo "Backup skipped (container not running)"

# Stop all containers
echo "2. Stopping all containers..."
docker-compose down

# Remove all Docker containers
echo "3. Removing all containers..."
docker rm -f $(docker ps -aq) 2>/dev/null || echo "No containers to remove"

# Remove all Docker images related to this project
echo "4. Removing project Docker images..."
docker rmi -f $(docker images | grep jobspy | awk '{print $1":"$2}') 2>/dev/null || echo "No jobspy images to remove"
docker rmi -f $(docker images | grep job-scraper | awk '{print $1":"$2}') 2>/dev/null || echo "No job-scraper images to remove"

# Remove all Docker images with "frontend" in the name
echo "5. Removing frontend images..."
docker rmi -f $(docker images | grep frontend | awk '{print $1":"$2}') 2>/dev/null || echo "No frontend images to remove"

# Remove all unused images
echo "6. Cleaning up unused Docker images..."
docker image prune -f

# Clean Docker build cache
echo "7. Cleaning Docker build cache..."
docker builder prune -f

# Remove any old volumes that might contain cached data
echo "8. Cleaning Docker volumes..."
docker volume prune -f

# Pull latest from git
echo "9. Pulling latest changes from git..."
git pull origin main || echo "Git pull failed (not in git repo or other issues)"

# Clean git repository
echo "10. Cleaning git repository..."
git clean -fd || echo "Git clean failed"
git reset --hard || echo "Git reset failed"

# Rebuild with latest Dockerfile
echo "11. Rebuilding with latest Dockerfile (this will take time)..."
docker-compose build --no-cache frontend

# Start services
echo "12. Starting services..."
docker-compose up -d

# Show status
echo "13. Checking deployment status..."
docker-compose ps
docker logs my-job-scraper-fe --tail 20

echo
echo "=== DEPLOYMENT COMPLETED ==="
echo "Your application should now be running with all latest changes"
echo "Access it at: http://your-vps-ip:5000"
echo
echo "To check logs: docker logs my-job-scraper-fe"
echo "To monitor status: docker-compose ps"