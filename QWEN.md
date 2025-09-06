# JobSpy Project Overview

This directory contains the "JobSpy" project, a job scraping system built around the `python-jobspy` library. The project provides a web-based admin interface for configuring and managing job scraping workers that can scrape job listings from various job boards (LinkedIn, Indeed, Glassdoor, etc.) and store the results in PostgreSQL databases.

## Development Environment
This project is developed using a VPS environment with EasyPanel for deployment management. Changes are committed to GitHub, and EasyPanel automatically picks up new commits for deployment. The application does not run in a traditional localhost development environment.

For more information about EasyPanel, see: https://easypanel.io/docs

## Project Type
This is a Python-based web application project that combines a Flask admin interface with background job scraping workers.

## Key Technologies
- Python 3.11
- Flask (Web Framework)
- PostgreSQL (Database)
- Docker & Docker Compose (Containerization)
- Nginx (Reverse Proxy)
- Prometheus & Grafana (Monitoring)
- WTForms (Form Handling)
- psycopg2 (PostgreSQL adapter)
- python-jobspy (Core scraping library)

## Project Structure
The project has a modular architecture:
- `jobspy/`: Core job scraping library code
- `worker_admin.py`: Flask web application for admin interface
- `worker_manager.py`: Background worker execution engine
- `modular_schema.sql`: PostgreSQL database schema
- `Dockerfile.force_rebuild_ultimate`: Docker build configuration
- `docker-compose.yml`: Multi-container application orchestration
- Various shell scripts for deployment and maintenance

## Building and Running

### Prerequisites
- Docker and Docker Compose
- Python 3.11 (for local development)
- PostgreSQL database

### Development Setup
1. Create a virtual environment: `python -m venv venv`
2. Activate it: `source venv/bin/activate` (Linux/Mac) or `venv\Scripts\activate` (Windows)
3. Install dependencies: `pip install -r requirements.txt`
4. Set environment variables:
   - `POSTGRES_URL`: PostgreSQL connection URL
   - `SECRET_KEY`: Flask secret key
   - `ADMIN_PASSWORD`: Admin password for the web interface
5. Run the admin interface: `python worker_admin.py`

### Production Deployment
1. Configure environment variables in a `.env` file
2. Commit changes to GitHub
3. EasyPanel will automatically deploy the new version
4. Access services through your VPS domain/IP

## UI/UX Improvements Implemented

We have successfully implemented a comprehensive UI/UX improvement plan for the admin dashboard. The following enhancements have been completed:

1. Visual Design Improvements:
   - Implemented a cohesive color palette with primary, secondary, success, warning, and danger colors
   - Established clear typography hierarchy with consistent font weights and sizes
   - Added consistent spacing system based on an 4px grid
   - Created modern UI elements with subtle shadows, rounded corners, and smooth transitions

2. Dashboard Improvements:
   - Redesigned metric cards with unified design and consistent styling
   - Enhanced visual hierarchy with clear information grouping
   - Improved dashboard layout with better spacing and organization
   - Added system information panel for quick overview

3. Form Improvements:
   - Organized complex forms with progressive disclosure using accordion components
   - Enhanced user guidance with improved labels, placeholders, and help text
   - Added better validation feedback with clear error messages
   - Improved form layout with consistent spacing and grouping

4. Data Management Improvements:
   - Enhanced tables with filtering capabilities for workers and databases
   - Improved data presentation with better visual hierarchy
   - Added search functionality for finding specific workers/databases
   - Implemented responsive tables that work well on all screen sizes

5. Navigation and Information Architecture:
   - Redesigned site navigation with persistent sidebar
   - Improved branding with consistent header and logo placement
   - Added user profile section in the sidebar
   - Created clear visual hierarchy in navigation elements

6. Accessibility Improvements:
   - Enhanced color contrast for better readability
   - Improved focus states for keyboard navigation
   - Added proper ARIA attributes for interactive elements
   - Implemented responsive design that works on various screen sizes

7. Performance and User Experience:
   - Improved loading states with better button feedback
   - Enhanced user feedback with notifications and alerts
   - Added smooth transitions for interactive elements
   - Optimized forms for better usability and reduced cognitive load

8. Template Structure:
   - Created new base template with sidebar navigation
   - Redesigned dashboard with improved metrics visualization
   - Updated worker form with progressive disclosure
   - Enhanced database form with better organization
   - Improved workers list with filtering capabilities
   - Redesigned databases list with enhanced visualization
   - Updated login page with better feedback
   - Created new CSS file with updated design system

9. Dark Mode Support:
   - Implemented dark/light theme switcher with moon/sun icons
   - Added comprehensive dark theme color palette
   - Included system preference detection
   - Added local storage for theme persistence
   - Ensured accessibility compliance in both themes

10. Proxy Management Improvements:
    - Added option to enable/disable Webshare.io proxies per worker
    - Enhanced proxy configuration with better UI controls
    - Improved proxy rotation policy selection
    - Added comprehensive proxy management in worker forms

## Development Conventions
- Python code follows PEP 8 style guidelines
- Database schema is defined in `modular_schema.sql`
- Docker images are built using `Dockerfile.force_rebuild_ultimate`
- Configuration is managed through environment variables
- Monitoring is implemented with Prometheus and Grafana
- Error handling and logging are implemented throughout the application
- Development occurs on a VPS with EasyPanel deployment management
- Changes are committed to GitHub and automatically deployed by EasyPanel
- UI/UX improvements are implemented following the documented improvement plan

## Key Files
- `worker_admin.py`: Main Flask application for the admin interface
- `worker_manager.py`: Background job execution engine
- `modular_schema.sql`: Database schema definition
- `Dockerfile.force_rebuild_ultimate`: Docker build configuration
- `docker-compose.yml`: Application orchestration
- `requirements.txt`: Python dependencies
- `jobspy/`: Core job scraping library code