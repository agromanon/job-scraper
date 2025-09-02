#!/usr/bin/env python3
"""
Flask Admin Interface for Worker Configuration and Management
Provides web interface for configuring, monitoring, and controlling scraping workers
"""

import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from functools import wraps
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, session, make_response, g
from flask_wtf import FlaskForm
from wtforms import (
    StringField, TextAreaField, IntegerField, BooleanField, 
    SelectField, SelectMultipleField,
    FloatField, SubmitField, FieldList, FormField
)
from wtforms.validators import DataRequired, Length, NumberRange, Optional
import os
import psycopg2
from psycopg2 import sql, extras
from psycopg2.pool import ThreadedConnectionPool
import secrets
import bcrypt

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', secrets.token_hex(32))

# Database connection - parse individual parameters for better reliability
POSTGRES_URL = os.getenv('POSTGRES_URL', "postgresql://aromanon:Afmg2486!@my-job-scraper_my-job-scraper:5432/job-data")
print(f"POSTGRES_URL: {POSTGRES_URL}")  # Debug logging

# Parse the POSTGRES_URL to extract individual components
import urllib.parse as urlparse
from urllib.parse import parse_qs

try:
    parsed = urlparse.urlparse(POSTGRES_URL)
    DB_HOST = parsed.hostname
    DB_PORT = parsed.port or 5432
    DB_NAME = parsed.path[1:]  # Remove leading slash
    DB_USER = parsed.username
    DB_PASSWORD = parsed.password
    
    print(f"Parsed - Host: {DB_HOST}, Port: {DB_PORT}, Database: {DB_NAME}, User: {DB_USER}")
    
    # Build connection string from individual parameters
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    db_pool = ThreadedConnectionPool(1, 10, DATABASE_URL)
    
except Exception as e:
    print(f"Error parsing POSTGRES_URL: {e}")
    raise


# Forms
class WorkerForm(FlaskForm):
    """Form for creating/updating workers"""
    name = StringField('Name', validators=[DataRequired(), Length(max=100)])
    description = TextAreaField('Description', validators=[Optional(), Length(max=500)])
    site = SelectField('Job Site', choices=[
        ('linkedin', 'LinkedIn'),
        ('indeed', 'Indeed'),
        ('glassdoor', 'Glassdoor'),
        ('google', 'Google Jobs'),
        ('zip_recruiter', 'Zip Recruiter'),
        ('bayt', 'Bayt'),
        ('naukri', 'Naukri'),
        ('bdjobs', 'BDJobs')
    ], validators=[DataRequired()])
    
    search_term = StringField('Search Term', validators=[Optional(), Length(max=255)])
    location = StringField('Location', validators=[Optional(), Length(max=255)])
    country = StringField('Country', default='BRAZIL', validators=[Optional(), Length(max=50)])
    distance = IntegerField('Search Radius (miles)', default=25, validators=[NumberRange(min=1, max=500)])
    
    job_type = SelectMultipleField('Job Types', choices=[
        ('FULL_TIME', 'Full Time'),
        ('PART_TIME', 'Part Time'),
        ('CONTRACT', 'Contract'),
        ('INTERNSHIP', 'Internship'),
        ('TEMPORARY', 'Temporary'),
        ('PER_DIEM', 'Per Diem'),
        ('NIGHTS', 'Night Shift'),
        ('OTHER', 'Other'),
        ('SUMMER', 'Summer'),
        ('VOLUNTEER', 'Volunteer')
    ], validators=[Optional()])
    
    is_remote = BooleanField('Remote Jobs Only', default=False)
    easy_apply = BooleanField('Easy Apply Only', default=False)
    linkedin_company_ids = StringField('LinkedIn Company IDs (comma-separated)', validators=[Optional()])
    
    hours_old = IntegerField('Jobs Posted Within Last (hours)', validators=[Optional(), NumberRange(min=1)])
    results_per_run = IntegerField('Results Per Run', default=50, validators=[NumberRange(min=1, max=1000)])
    
    schedule_hours = IntegerField('Run Every (hours)', default=24, validators=[NumberRange(min=1, max=168)])
    schedule_minute_offset = IntegerField('Minute Offset', default=0, validators=[NumberRange(min=0, max=59)])
    timezone = SelectField('Timezone', choices=[
        ('America/Sao_Paulo', 'São Paulo (Brazil)'),
        ('America/New_York', 'New York (US)'),
        ('America/Los_Angeles', 'Los Angeles (US)'),
        ('Europe/London', 'London (UK)'),
        ('Europe/Paris', 'Paris (France)'),
        ('Asia/Tokyo', 'Tokyo (Japan)'),
        ('Asia/Dubai', 'Dubai (UAE)')
    ], default='America/Sao_Paulo', validators=[DataRequired()])
    
    proxy_rotation_policy = SelectField('Proxy Rotation', choices=[
        ('rotating', 'Rotating'),
        ('sticky', 'Sticky'),
        ('none', 'None')
    ], default='rotating', validators=[DataRequired()])
    
    proxies = TextAreaField('Proxy URLs (one per line)', validators=[Optional()])
    max_retries = IntegerField('Max Retries', default=3, validators=[NumberRange(min=0, max=10)])
    timeout = IntegerField('Timeout (seconds)', default=30, validators=[NumberRange(min=10, max=300)])
    rate_limit_requests = IntegerField('Rate Limit (requests)', default=10, validators=[NumberRange(min=1, max=100)])
    rate_limit_seconds = IntegerField('Rate Limit Interval (seconds)', default=60, validators=[NumberRange(min=1, max=3600)])
    
    description_format = SelectField('Description Format', choices=[
        ('markdown', 'Markdown'),
        ('html', 'HTML'),
        ('plain', 'Plain Text')
    ], default='markdown', validators=[DataRequired()])
    
    linkedin_fetch_description = BooleanField('Fetch LinkedIn Description', default=False)
    
    # Database configuration
    database_id = SelectField('Database', coerce=int, validators=[DataRequired()])
    table_name = StringField('Table Name', validators=[DataRequired(), Length(max=100)])
    
    # Resource limits
    memory_limit_mb = IntegerField('Memory Limit (MB)', default=512, validators=[NumberRange(min=64, max=4096)])
    cpu_limit_cores = FloatField('CPU Limit (cores)', default=0.5, validators=[NumberRange(min=0.1, max=4.0)])
    max_runtime_minutes = IntegerField('Max Runtime (minutes)', default=60, validators=[NumberRange(min=5, max=360)])
    
    # Error handling
    max_consecutive_errors = IntegerField('Max Consecutive Errors', default=5, validators=[NumberRange(min=1, max=20)])
    auto_pause_on_errors = BooleanField('Auto-Pause on Errors', default=True)
    
    # Status and monitoring
    status = SelectField('Status', choices=[
        ('active', 'Active'),
        ('paused', 'Paused'),
        ('stopped', 'Stopped'),
        ('error', 'Error')
    ], default='active')
    
    tags = StringField('Tags (comma-separated)', validators=[Optional()])
    
    submit = SubmitField('Save Worker')


class DatabaseForm(FlaskForm):
    """Form for creating/updating databases"""
    name = StringField('Name', validators=[DataRequired(), Length(max=100)])
    description = TextAreaField('Description', validators=[Optional(), Length(max=500)])
    host = StringField('Host', validators=[DataRequired(), Length(max=255)])
    port = IntegerField('Port', default=5432, validators=[NumberRange(min=1, max=65535)])
    database_name = StringField('Database Name', validators=[DataRequired(), Length(max=100)])
    username = StringField('Username', validators=[DataRequired(), Length(max=100)])
    password = StringField('Password', validators=[DataRequired(), Length(max=255)])
    ssl_mode = SelectField('SSL Mode', choices=[
        ('require', 'Require'),
        ('prefer', 'Prefer'),
        ('allow', 'Allow'),
        ('disable', 'Disable'),
        ('verify-full', 'Verify Full')
    ], default='require', validators=[DataRequired()])
    
    connection_pool_size = IntegerField('Connection Pool Size', default=5, validators=[NumberRange(min=1, max=50)])
    max_connections = IntegerField('Max Connections', default=20, validators=[NumberRange(min=1, max=100)])
    connection_timeout_seconds = IntegerField('Connection Timeout (seconds)', default=30, validators=[NumberRange(min=5, max=300)])
    
    target_table_prefix = StringField('Table Prefix', default='job_listings_', validators=[Optional(), Length(max=50)])
    create_schema_if_not_exists = BooleanField('Create Schema if Not Exists', default=True)
    
    batch_size = IntegerField('Batch Size', default=100, validators=[NumberRange(min=10, max=10000)])
    deduplication_method = SelectField('Deduplication Method', choices=[
        ('unique_id', 'Unique ID'),
        ('composite_key', 'Composite Key'),
        ('none', 'None')
    ], default='unique_id', validators=[DataRequired()])
    
    deduplication_fields = TextAreaField('Deduplication Fields (one per line)', validators=[Optional()])
    
    tags = StringField('Tags (comma-separated)', validators=[Optional()])
    
    is_active = BooleanField('Active', default=True)
    
    submit = SubmitField('Save Database')


class ProxyForm(FlaskForm):
    """Form for creating/updating proxies"""
    url = StringField('Proxy URL', validators=[DataRequired(), Length(max=255)])
    description = TextAreaField('Description', validators=[Optional(), Length(max=500)])
    country = StringField('Country', validators=[Optional(), Length(max=50)])
    city = StringField('City', validators=[Optional(), Length(max=50)])
    provider = StringField('Provider', validators=[Optional(), Length(max=100)])
    protocol = SelectField('Protocol', choices=[
        ('http', 'HTTP'),
        ('https', 'HTTPS'),
        ('socks5', 'SOCKS5')
    ], default='http', validators=[DataRequired()])
    
    username = StringField('Username', validators=[Optional(), Length(max=100)])
    password = StringField('Password', validators=[Optional(), Length(max=255)])
    port = IntegerField('Port', validators=[Optional(), NumberRange(min=1, max=65535)])
    
    max_requests_per_hour = IntegerField('Max Requests/Hour', default=1000, validators=[NumberRange(min=1, max=10000)])
    cost_per_request = FloatField('Cost per Request', validators=[Optional(), NumberRange(min=0)])
    monthly_cost_limit = FloatField('Monthly Cost Limit', validators=[Optional(), NumberRange(min=0)])
    is_premium = BooleanField('Premium Proxy', default=False)
    
    tags = StringField('Tags (comma-separated)', validators=[Optional()])
    
    submit = SubmitField('Save Proxy')


class LoginForm(FlaskForm):
    """Simple login form"""
    password = StringField('Password', validators=[DataRequired()])
    submit = SubmitField('Login')


# Database helper functions
def get_db_connection():
    """Get database connection from pool"""
    return db_pool.getconn()


def release_db_connection(conn):
    """Release database connection back to pool"""
    db_pool.putconn(conn)


def execute_query(query: str, params: tuple = (), fetch: bool = True, commit: bool = False):
    """Execute database query with enhanced error handling"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=extras.DictCursor)
        
        # Debug logging for important queries
        if "INSERT" in query or "UPDATE" in query or "DELETE" in query:
            app.logger.debug(f"Executing query: {query[:100]}... with params: {params}")
        
        cursor.execute(query, params)
        
        if fetch:
            result = cursor.fetchall()
            app.logger.debug(f"Query returned {len(result)} rows")
        else:
            result = cursor.rowcount
            app.logger.debug(f"Query affected {result} rows")
        
        if commit:
            conn.commit()
            app.logger.debug("Transaction committed successfully")
        
        cursor.close()
        return result
    
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        app.logger.error(f"PostgreSQL error: {e}")
        app.logger.error(f"Query: {query}")
        app.logger.error(f"Params: {params}")
        raise Exception(f"Database error: {str(e)}")
    except Exception as e:
        if conn:
            conn.rollback()
        app.logger.error(f"Unexpected database error: {e}")
        app.logger.error(f"Query: {query}")
        app.logger.error(f"Params: {params}")
        raise Exception(f"Database error: {str(e)}")
    finally:
        if conn:
            release_db_connection(conn)


# Authentication middleware
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            flash('Please log in to access the dashboard', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


# Authentication routes
@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login route for admin dashboard"""
    form = LoginForm()
    
    if form.validate_on_submit():
        # Simple password authentication (in production, use proper hashing)
        admin_password = os.getenv('ADMIN_PASSWORD', 'admin123')
        
        if form.password.data == admin_password:
            session['logged_in'] = True
            session['username'] = 'admin'
            flash('Login successful!', 'success')
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid password', 'error')
    
    return render_template('login.html', form=form)


@app.route('/logout')
def logout():
    """Logout route"""
    session.clear()
    flash('You have been logged out', 'info')
    return redirect(url_for('login'))


# Routes
@app.route('/')
@login_required
def dashboard():
    """Main dashboard with worker overview"""
    try:
        # Get worker statistics
        stats = execute_query("""
            SELECT 
                COUNT(*) as total_workers,
                COUNT(CASE WHEN status = 'active' THEN 1 END) as active_workers,
                COUNT(CASE WHEN status = 'paused' THEN 1 END) as paused_workers,
                COUNT(CASE WHEN status = 'error' THEN 1 END) as error_workers,
                COUNT(CASE WHEN last_run >= CURRENT_DATE THEN 1 END) as workers_today,
                COUNT(CASE WHEN next_run <= CURRENT_TIMESTAMP + INTERVAL '1 hour' THEN 1 END) as scheduled_soon
            FROM scraping_workers
        """, fetch=True)[0] or {}
        
        # Get recent executions
        recent_executions = execute_query("""
            SELECT 
                eh.*,
                w.name as worker_name,
                d.name as database_name
            FROM worker_execution_history eh
            LEFT JOIN scraping_workers w ON eh.worker_id = w.id
            LEFT JOIN scraping_databases d ON eh.database_id = d.id
            ORDER BY eh.execution_start DESC
            LIMIT 10
        """, fetch=True)
        
        # Get next scheduled runs
        scheduled_runs = execute_query("""
            SELECT 
                w.id, w.name, w.site, w.next_run, w.database_id,
                d.name as database_name
            FROM scraping_workers w
            LEFT JOIN scraping_databases d ON w.database_id = d.id
            WHERE w.status = 'active' AND w.next_run IS NOT NULL
            ORDER BY w.next_run
            LIMIT 5
        """, fetch=True)
        
        return render_template('dashboard.html', 
                             stats=stats,
                             recent_executions=recent_executions,
                             scheduled_runs=scheduled_runs)
    
    except Exception as e:
        app.logger.error(f"Dashboard error: {e}")
        return render_template('error.html', error=str(e))


@app.route('/workers')
@login_required
def list_workers():
    """List all workers"""
    try:
        workers = execute_query("""
            SELECT 
                w.*,
                d.name as database_name
            FROM scraping_workers w
            LEFT JOIN scraping_databases d ON w.database_id = d.id
            ORDER BY w.name
        """, fetch=True)
        
        return render_template('workers.html', workers=workers)
    
    except Exception as e:
        app.logger.error(f"Workers list error: {e}")
        return render_template('error.html', error=str(e))


@app.route('/workers/new', methods=['GET', 'POST'])
@login_required
def new_worker():
    """Create new worker"""
    form = WorkerForm()
    
    # Populate database choices
    databases = execute_query("SELECT id, name FROM scraping_databases WHERE is_active = true ORDER BY name")
    form.database_id.choices = [(db['id'], db['name']) for db in databases]
    
    if form.validate_on_submit():
        try:
            # Parse linkedin_company_ids
            linkedin_company_ids = []
            if form.linkedin_company_ids.data:
                try:
                    linkedin_company_ids = [int(x.strip()) for x in form.linkedin_company_ids.data.split(',')]
                except ValueError:
                    flash('Invalid LinkedIn Company IDs format', 'error')
                    return render_template('worker_form.html', form=form, action='new')
            
            # Parse proxies
            proxies = []
            if form.proxies.data:
                proxies = [line.strip() for line in form.proxies.data.split('\n') if line.strip()]
            
            # Parse tags
            tags = []
            if form.tags.data:
                tags = [tag.strip() for tag in form.tags.data.split(',')]
            
            # Calculate initial next_run time
            schedule_minute_offset = form.schedule_minute_offset.data or 0
            timezone = form.timezone.data
            
            # Insert worker
            execute_query("""
                INSERT INTO scraping_workers (
                    name, description, site, search_term, location, country, distance, job_type,
                    is_remote, easy_apply, linkedin_company_ids, hours_old, results_per_run,
                    schedule_hours, schedule_minute_offset, timezone, proxy_rotation_policy, proxies,
                    max_retries, timeout, rate_limit_requests, rate_limit_seconds, description_format,
                    linkedin_fetch_description, database_id, table_name, memory_limit_mb,
                    cpu_limit_cores, max_runtime_minutes, max_consecutive_errors, status, auto_pause_on_errors, tags,
                    next_run, last_run, last_success, last_error, consecutive_errors, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT)
            """, (
                form.name.data,
                form.description.data,
                form.site.data,
                form.search_term.data,
                form.location.data,
                form.country.data,
                form.distance.data,
                json.dumps(form.job_type.data) if form.job_type.data else None,
                form.is_remote.data,
                form.easy_apply.data,
                linkedin_company_ids,
                form.hours_old.data,
                form.results_per_run.data,
                form.schedule_hours.data,
                schedule_minute_offset,
                timezone,
                form.proxy_rotation_policy.data,
                proxies,
                form.max_retries.data,
                form.timeout.data,
                form.rate_limit_requests.data,
                form.rate_limit_seconds.data,
                form.description_format.data,
                form.linkedin_fetch_description.data,
                form.database_id.data,
                form.table_name.data,
                form.memory_limit_mb.data,
                form.cpu_limit_cores.data,
                form.max_runtime_minutes.data,
                form.max_consecutive_errors.data,
                form.status.data,
                form.auto_pause_on_errors.data,
                tags,
                datetime.utcnow() + timedelta(minutes=schedule_minute_offset)  # Initial next_run
            ), fetch=False, commit=True)
            
            flash('Worker created successfully!', 'success')
            return redirect(url_for('list_workers'))
            
        except psycopg2.IntegrityError as e:
            if "scraping_workers_name_key" in str(e):
                flash('A worker with this name already exists. Please choose a different name.', 'error')
            else:
                flash(f'Error creating worker: {str(e)}', 'error')
            # Rollback the transaction
            conn = get_db_connection()
            conn.rollback()
            release_db_connection(conn)
            return render_template('worker_form.html', form=form, action='new')
        except Exception as e:
            app.logger.error(f"Worker creation error: {e}")
            flash(f'Error creating worker: {str(e)}', 'error')
            return render_template('worker_form.html', form=form, action='new')
    
    return render_template('worker_form.html', form=form, action='new')


@app.route('/workers/<int:worker_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_worker(worker_id):
    """Edit existing worker"""
    try:
        # Get worker data
        worker_data = execute_query("SELECT * FROM scraping_workers WHERE id = %s", (worker_id,), fetch=True)
        if not worker_data:
            flash('Worker not found', 'error')
            return redirect(url_for('list_workers'))
        
        worker = worker_data[0]
        form = WorkerForm()
        
        # Populate database choices
        databases = execute_query("SELECT id, name FROM scraping_databases WHERE is_active = true ORDER BY name")
        form.database_id.choices = [(db['id'], db['name']) for db in databases]
        
        if request.method == 'GET':
            # Populate form with worker data for GET request
            form.name.data = worker['name']
            form.description.data = worker['description']
            form.site.data = worker['site']
            form.search_term.data = worker['search_term']
            form.location.data = worker['location']
            form.country.data = worker['country']
            form.distance.data = worker['distance']
            
            # Handle job_type array
            if worker['job_type']:
                form.job_type.data = worker['job_type']
            
            form.is_remote.data = worker['is_remote']
            form.easy_apply.data = worker['easy_apply']
            
            # Handle linkedin_company_ids array
            if worker['linkedin_company_ids']:
                form.linkedin_company_ids.data = ','.join(map(str, worker['linkedin_company_ids']))
            else:
                form.linkedin_company_ids.data = ''
            
            form.hours_old.data = worker['hours_old']
            form.results_per_run.data = worker['results_per_run']
            form.schedule_hours.data = worker['schedule_hours']
            form.schedule_minute_offset.data = worker['schedule_minute_offset']
            form.timezone.data = worker['timezone']
            form.proxy_rotation_policy.data = worker['proxy_rotation_policy']
            
            # Handle proxies array
            if worker['proxies']:
                form.proxies.data = '\n'.join(worker['proxies'])
            else:
                form.proxies.data = ''
            
            form.max_retries.data = worker['max_retries']
            form.timeout.data = worker['timeout']
            form.rate_limit_requests.data = worker['rate_limit_requests']
            form.rate_limit_seconds.data = worker['rate_limit_seconds']
            form.description_format.data = worker['description_format']
            form.linkedin_fetch_description.data = worker['linkedin_fetch_description']
            form.database_id.data = worker['database_id']
            form.table_name.data = worker['table_name']
            form.memory_limit_mb.data = worker['memory_limit_mb']
            form.cpu_limit_cores.data = float(worker['cpu_limit_cores']) if worker['cpu_limit_cores'] else 0.5
            form.max_runtime_minutes.data = worker['max_runtime_minutes']
            form.max_consecutive_errors.data = worker['max_consecutive_errors']
            form.status.data = worker['status']
            form.auto_pause_on_errors.data = worker['auto_pause_on_errors']
            
            # Handle tags array
            if worker['tags']:
                form.tags.data = ','.join(worker['tags'])
            else:
                form.tags.data = ''
        
        if form.validate_on_submit():
            try:
                # Parse linkedin_company_ids
                linkedin_company_ids = []
                if form.linkedin_company_ids.data:
                    try:
                        linkedin_company_ids = [int(x.strip()) for x in form.linkedin_company_ids.data.split(',')]
                    except ValueError:
                        flash('Invalid LinkedIn Company IDs format', 'error')
                        return render_template('worker_form.html', form=form, action='edit', worker_id=worker_id)
                
                # Parse proxies
                proxies = []
                if form.proxies.data:
                    proxies = [line.strip() for line in form.proxies.data.split('\n') if line.strip()]
                
                # Parse tags
                tags = []
                if form.tags.data:
                    tags = [tag.strip() for tag in form.tags.data.split(',')]
                
                # Update worker
                execute_query("""
                    UPDATE scraping_workers SET
                        name = %s, description = %s, site = %s, search_term = %s, location = %s,
                        country = %s, distance = %s, job_type = %s, is_remote = %s, easy_apply = %s,
                        linkedin_company_ids = %s, hours_old = %s, results_per_run = %s, schedule_hours = %s,
                        schedule_minute_offset = %s, timezone = %s, proxy_rotation_policy = %s, proxies = %s,
                        max_retries = %s, timeout = %s, rate_limit_requests = %s, rate_limit_seconds = %s,
                        description_format = %s, linkedin_fetch_description = %s, database_id = %s,
                        table_name = %s, memory_limit_mb = %s, cpu_limit_cores = %s, max_runtime_minutes = %s,
                        max_consecutive_errors = %s, status = %s, auto_pause_on_errors = %s, tags = %s
                    WHERE id = %s
                """, (
                    form.name.data,
                    form.description.data,
                    form.site.data,
                    form.search_term.data,
                    form.location.data,
                    form.country.data,
                    form.distance.data,
                    json.dumps(form.job_type.data) if form.job_type.data else None,
                    form.is_remote.data,
                    form.easy_apply.data,
                    linkedin_company_ids,
                    form.hours_old.data,
                    form.results_per_run.data,
                    form.schedule_hours.data,
                    form.schedule_minute_offset.data,
                    form.timezone.data,
                    form.proxy_rotation_policy.data,
                    proxies,
                    form.max_retries.data,
                    form.timeout.data,
                    form.rate_limit_requests.data,
                    form.rate_limit_seconds.data,
                    form.description_format.data,
                    form.linkedin_fetch_description.data,
                    form.database_id.data,
                    form.table_name.data,
                    form.memory_limit_mb.data,
                    form.cpu_limit_cores.data,
                    form.max_runtime_minutes.data,
                    form.max_consecutive_errors.data,
                    form.status.data,
                    form.auto_pause_on_errors.data,
                    tags,
                    worker_id
                ), fetch=False, commit=True)
                
                flash('Worker updated successfully!', 'success')
                return redirect(url_for('list_workers'))
                
            except psycopg2.IntegrityError as e:
                if "scraping_workers_name_key" in str(e):
                    flash('A worker with this name already exists. Please choose a different name.', 'error')
                else:
                    flash(f'Error updating worker: {str(e)}', 'error')
                # Rollback the transaction
                conn = get_db_connection()
                conn.rollback()
                release_db_connection(conn)
                return render_template('worker_form.html', form=form, action='edit', worker_id=worker_id)
            except Exception as e:
                app.logger.error(f"Worker update error: {e}")
                flash(f'Error updating worker: {str(e)}', 'error')
                return render_template('worker_form.html', form=form, action='edit', worker_id=worker_id)
        
        return render_template('worker_form.html', form=form, action='edit', worker_id=worker_id)
    
    except Exception as e:
        app.logger.error(f"Worker edit error: {e}")
        return render_template('error.html', error=str(e))


@app.route('/workers/<int:worker_id>/toggle')
@login_required
def toggle_worker(worker_id):
    """Toggle worker active/paused status"""
    try:
        worker_data = execute_query("SELECT status FROM scraping_workers WHERE id = %s", (worker_id,), fetch=True)
        if not worker_data:
            flash('Worker not found', 'error')
            return redirect(url_for('list_workers'))
        
        current_status = worker_data[0]['status']
        new_status = 'paused' if current_status == 'active' else 'active'
        
        execute_query("UPDATE scraping_workers SET status = %s WHERE id = %s", (new_status, worker_id), fetch=False, commit=True)
        
        flash(f'Worker {new_status} successfully!', 'success')
        return redirect(url_for('list_workers'))
    
    except Exception as e:
        app.logger.error(f"Worker toggle error: {e}")
        flash(f'Error toggling worker: {str(e)}', 'error')
        return redirect(url_for('list_workers'))


@app.route('/workers/<int:worker_id>/delete')
@login_required
def delete_worker(worker_id):
    """Delete worker"""
    try:
        execute_query("DELETE FROM scraping_workers WHERE id = %s", (worker_id,), fetch=False, commit=True)
        flash('Worker deleted successfully!', 'success')
        return redirect(url_for('list_workers'))
    
    except Exception as e:
        app.logger.error(f"Worker deletion error: {e}")
        flash(f'Error deleting worker: {str(e)}', 'error')
        return redirect(url_for('list_workers'))


@app.route('/workers/<int:worker_id>/execute')
@login_required
def execute_worker(worker_id):
    """Manually execute worker"""
    try:
        # Import here to avoid circular imports and handle import errors
        try:
            from worker_manager import WorkerManager, WorkerConfig
        except ImportError as e:
            app.logger.error(f"Failed to import WorkerManager: {e}")
            flash(f'Error importing worker components: {str(e)}', 'error')
            return redirect(url_for('list_workers'))
        
        # Get worker configuration
        worker_data = execute_query("SELECT * FROM scraping_workers WHERE id = %s", (worker_id,), fetch=True)
        if not worker_data:
            flash('Worker not found', 'error')
            return redirect(url_for('list_workers'))
        
        worker_record = worker_data[0]
        
        # Create WorkerConfig object
        config = WorkerConfig(
            id=worker_record['id'],
            name=worker_record['name'],
            site=worker_record['site'],
            search_term=worker_record['search_term'],
            location=worker_record['location'],
            country=worker_record['country'],
            distance=worker_record['distance'],
            job_type=worker_record['job_type'] or [],
            is_remote=worker_record['is_remote'],
            easy_apply=worker_record['easy_apply'],
            linkedin_company_ids=worker_record['linkedin_company_ids'] or [],
            hours_old=worker_record['hours_old'],
            results_per_run=worker_record['results_per_run'],
            schedule_hours=worker_record['schedule_hours'],
            schedule_minute_offset=worker_record['schedule_minute_offset'],
            timezone=worker_record['timezone'],
            proxy_rotation_policy=worker_record['proxy_rotation_policy'],
            proxies=worker_record['proxies'] or [],
            max_retries=worker_record['max_retries'],
            timeout=worker_record['timeout'],
            rate_limit_requests=worker_record['rate_limit_requests'],
            rate_limit_seconds=worker_record['rate_limit_seconds'],
            description_format=worker_record['description_format'],
            linkedin_fetch_description=worker_record['linkedin_fetch_description'],
            database_id=worker_record['database_id'],
            table_name=worker_record['table_name'],
            status=worker_record['status'],
            memory_limit_mb=worker_record['memory_limit_mb'],
            cpu_limit_cores=worker_record['cpu_limit_cores'],
            max_runtime_minutes=worker_record['max_runtime_minutes'],
            tags=worker_record['tags'] or []
        )
        
        # Execute worker in background thread
        import threading
        def run_worker():
            try:
                manager = WorkerManager(DATABASE_URL)
                manager.register_databases()
                manager.run_worker(config)
            except Exception as e:
                app.logger.error(f"Worker execution error: {e}")
                # Log the full traceback for debugging
                import traceback
                app.logger.error(f"Worker execution error traceback: {traceback.format_exc()}")
        
        thread = threading.Thread(target=run_worker)
        thread.daemon = True
        thread.start()
        
        flash('Worker execution started!', 'success')
        return redirect(url_for('list_workers'))
        
    except Exception as e:
        app.logger.error(f"Worker execution error: {e}")
        # Log the full traceback for debugging
        import traceback
        app.logger.error(f"Worker execution error traceback: {traceback.format_exc()}")
        flash(f'Error executing worker: {str(e)}', 'error')
        return redirect(url_for('list_workers'))


# Database management routes
@app.route('/databases')
@login_required
def list_databases():
    """List all databases"""
    try:
        databases = execute_query("""
            SELECT 
                d.*,
                COUNT(DISTINCT w.id) as worker_count
            FROM scraping_databases d
            LEFT JOIN scraping_workers w ON d.id = w.database_id AND w.status = 'active'
            GROUP BY d.id
            ORDER BY d.name
        """, fetch=True)
        
        return render_template('databases.html', databases=databases)
    
    except Exception as e:
        app.logger.error(f"Databases list error: {e}")
        return render_template('error.html', error=str(e))


@app.route('/databases/new', methods=['GET', 'POST'])
@login_required
def new_database():
    """Create new database"""
    form = DatabaseForm()
    
    if form.validate_on_submit():
        try:
            # Validate host and database name
            host = form.host.data.strip()
            database_name = form.database_name.data.strip()
            
            if not host:
                flash('Host is required', 'error')
                return render_template('database_form.html', form=form, action='new')
            
            if not database_name:
                flash('Database name is required', 'error')
                return render_template('database_form.html', form=form, action='new')
            
            # Parse deduplication fields
            deduplication_fields = []
            if form.deduplication_fields.data:
                deduplication_fields = [line.strip() for line in form.deduplication_fields.data.split('\n') if line.strip()]
            
            # Parse tags
            tags = []
            if form.tags.data:
                tags = [tag.strip() for tag in form.tags.data.split(',') if tag.strip()]
            
            # Validate deduplication method
            if form.deduplication_method.data == 'composite_key' and not deduplication_fields:
                flash('Composite key deduplication requires at least one field to be specified', 'error')
                return render_template('database_form.html', form=form, action='new')
            
            # Validate batch size
            if form.batch_size.data < 1 or form.batch_size.data > 10000:
                flash('Batch size must be between 1 and 10000', 'error')
                return render_template('database_form.html', form=form, action='new')
            
            # Validate connection pool size
            if form.connection_pool_size.data < 1 or form.connection_pool_size.data > 50:
                flash('Connection pool size must be between 1 and 50', 'error')
                return render_template('database_form.html', form=form, action='new')
            
            # Test database connection before creating
            try:
                test_conn = psycopg2.connect(
                    host=host,
                    port=form.port.data,
                    database=database_name,
                    user=form.username.data,
                    password=form.password.data,
                    sslmode=form.ssl_mode.data,
                    connect_timeout=10
                )
                test_conn.close()
            except Exception as conn_error:
                flash(f'Cannot connect to database: {str(conn_error)}', 'error')
                return render_template('database_form.html', form=form, action='new')
            
            # Insert database
            execute_query("""
                INSERT INTO scraping_databases (
                    name, description, host, port, database_name, username, password, ssl_mode,
                    connection_pool_size, max_connections, connection_timeout_seconds,
                    target_table_prefix, create_schema_if_not_exists, batch_size,
                    deduplication_method, deduplication_fields, tags
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                form.name.data,
                form.description.data,
                host,
                form.port.data,
                database_name,
                form.username.data,
                form.password.data,  # In production, encrypt this
                form.ssl_mode.data,
                form.connection_pool_size.data,
                form.max_connections.data,
                form.connection_timeout_seconds.data,
                form.target_table_prefix.data,
                form.create_schema_if_not_exists.data,
                form.batch_size.data,
                form.deduplication_method.data,
                deduplication_fields,
                tags
            ), fetch=False, commit=True)
            
            flash('Database created successfully!', 'success')
            return redirect(url_for('list_databases'))
            
        except Exception as e:
            app.logger.error(f"Database creation error: {e}")
            if "unique constraint" in str(e).lower():
                flash('A database with this name already exists', 'error')
            else:
                flash(f'Error creating database: {str(e)}', 'error')
    
    return render_template('database_form.html', form=form, action='new')


# API routes for AJAX operations
@app.route('/api/workers/<int:worker_id>/history')
def worker_history(worker_id):
    """Get worker execution history as JSON"""
    try:
        history = execute_query("""
            SELECT *
            FROM worker_execution_history
            WHERE worker_id = %s
            ORDER BY execution_start DESC
            LIMIT 50
        """, (worker_id,), fetch=True)
        
        return jsonify({
            'success': True,
            'data': [dict(record) for record in history]
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/workers/<int:worker_id>/performance')
def worker_performance(worker_id):
    """Get worker performance stats as JSON"""
    try:
        stats = execute_query("""
            SELECT 
                COUNT(*) as total_executions,
                COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_executions,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_executions,
                AVG(duration_seconds) as avg_duration_seconds,
                AVG(jobs_inserted) as avg_jobs_per_run,
                SUM(jobs_inserted) as total_jobs_inserted,
                SUM(duplicates_found) as total_duplicates,
                MAX(execution_start) as last_execution
            FROM worker_execution_history
            WHERE worker_id = %s
        """, (worker_id,), fetch=True)
        
        if stats:
            stats = dict(stats[0])
            # Calculate success rate
            if stats['total_executions'] > 0:
                stats['success_rate'] = (stats['successful_executions'] * 100.0) / stats['total_executions']
            else:
                stats['success_rate'] = 0
        
        return jsonify({
            'success': True,
            'data': stats or {}
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/databases/<int:database_id>/test')
def test_database(database_id):
    """Test database connection"""
    try:
        db_data = execute_query("SELECT * FROM scraping_databases WHERE id = %s", (database_id,), fetch=True)
        if not db_data:
            return jsonify({'success': False, 'error': 'Database not found'})
        
        db_record = db_data[0]
        
        # Test connection
        import psycopg2
        test_conn = None
        try:
            test_conn = psycopg2.connect(
                host=db_record['host'],
                port=db_record['port'],
                database=db_record['database_name'],
                user=db_record['username'],
                password=db_record['password'],
                sslmode=db_record['ssl_mode'],
                connect_timeout=10
            )
            
            # Execute simple query
            cursor = test_conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            
            # Update database status
            execute_query("""
                UPDATE scraping_databases 
                SET 
                    connection_status = 'success',
                    last_connection_test = CURRENT_TIMESTAMP,
                    connection_error = NULL,
                    is_active = true
                WHERE id = %s
            """, (database_id,), fetch=False, commit=True)
            
            return jsonify({'success': True, 'message': 'Database connection successful'})
            
        except Exception as conn_error:
            # Update database status with error
            execute_query("""
                UPDATE scraping_databases 
                SET 
                    connection_status = 'failed',
                    last_connection_test = CURRENT_TIMESTAMP,
                    connection_error = %s
                WHERE id = %s
            """, (str(conn_error), database_id), fetch=False, commit=True)
            
            return jsonify({'success': False, 'error': str(conn_error)})
        
        finally:
            if test_conn:
                test_conn.close()
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/databases/<int:database_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_database(database_id):
    """Edit existing database"""
    # Fetch database record
    databases = execute_query("SELECT * FROM scraping_databases WHERE id = %s", (database_id,), fetch=True)
    
    if not databases:
        flash('Database not found', 'danger')
        return redirect(url_for('list_databases'))
    
    db_record = databases[0]
    form = DatabaseForm(obj=db_record)
    
    if form.validate_on_submit():
        # Validate inputs
        host = form.host.data.strip()
        database_name = form.database_name.data.strip()
        
        if not host:
            flash('Host is required', 'error')
            return render_template('database_form.html', form=form, action='edit')
        
        if not database_name:
            flash('Database name is required', 'error')
            return render_template('database_form.html', form=form, action='edit')
        
        # Validate batch size
        if form.batch_size.data < 1 or form.batch_size.data > 10000:
            flash('Batch size must be between 1 and 10000', 'error')
            return render_template('database_form.html', form=form, action='edit')
        
        # Validate connection pool size
        if form.connection_pool_size.data < 1 or form.connection_pool_size.data > 50:
            flash('Connection pool size must be between 1 and 50', 'error')
            return render_template('database_form.html', form=form, action='edit')
        
        # Test database connection before updating
        try:
            test_conn = psycopg2.connect(
                host=host,
                port=form.port.data,
                database=database_name,
                user=form.username.data,
                password=form.password.data,
                sslmode=form.ssl_mode.data,
                connect_timeout=10
            )
            test_conn.close()
        except Exception as conn_error:
            flash(f'Cannot connect to database: {str(conn_error)}', 'error')
            return render_template('database_form.html', form=form, action='edit')
        
        try:
            execute_query("""
                UPDATE scraping_databases SET
                    name = %s,
                    description = %s,
                    host = %s,
                    port = %s,
                    database_name = %s,
                    username = %s,
                    password = %s,
                    ssl_mode = %s,
                    is_active = %s
                WHERE id = %s
            """, (
                form.name.data,
                form.description.data,
                form.host.data,
                form.port.data,
                form.database_name.data,
                form.username.data,
                form.password.data,
                form.ssl_mode.data,
                form.is_active.data,
                database_id
            ))
            
            flash('Database updated successfully', 'success')
            return redirect(url_for('list_databases'))
            
        except Exception as e:
            if "unique constraint" in str(e).lower():
                flash('A database with this name already exists', 'error')
            elif "foreign key constraint" in str(e).lower():
                flash('Cannot update database: referenced by workers or other records', 'error')
            else:
                flash(f'Error updating database: {str(e)}', 'error')
            app.logger.error(f'Database update error: {e}')
            app.logger.error(f'Database update error type: {type(e).__name__}')
    
    return render_template('database_form.html', form=form, action='edit')


# Health check endpoint for monitoring
@app.route('/health')
def health_check():
    """Health check endpoint for monitoring"""
    try:
        # Test database connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        release_db_connection(conn)
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'database': 'connected',
            'app': 'job-scraper-admin'
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.utcnow().isoformat(),
            'database': 'disconnected',
            'error': str(e),
            'app': 'job-scraper-admin'
        }), 500


# Database schema initialization function
def initialize_database_schema():
    """Initialize database schema using Flask app context"""
    print("="*50)
    print("SCHEMA INITIALIZATION STARTING")
    print("="*50)
    
    try:
        # First, check if we can connect to the database at all
        print("Testing database connection...")
        test_conn = get_db_connection()
        test_cursor = test_conn.cursor()
        test_cursor.execute("SELECT 1")
        result = test_cursor.fetchone()
        print(f"Database connection test successful: {result}")
        test_cursor.close()
        release_db_connection(test_conn)
        
        # Check if uuid-ossp extension is available (for gen_random_uuid)
        print("Checking uuid-ossp extension...")
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            conn.commit()
            cursor.close()
            release_db_connection(conn)
            print("uuid-ossp extension ensured")
        except Exception as ext_error:
            print(f"Warning: Could not create uuid-ossp extension: {ext_error}")
        
        # Create tables using the schema file
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'modular_schema.sql')
        print(f"Looking for schema file at: {schema_path}")
        
        # Check if schema file exists
        if not os.path.exists(schema_path):
            print(f"ERROR: Schema file does not exist at: {schema_path}")
            # List files in the directory to debug
            dir_path = os.path.dirname(schema_path)
            print(f"Contents of {dir_path}:")
            if os.path.exists(dir_path):
                for file in os.listdir(dir_path):
                    print(f"  - {file}")
            else:
                print(f"Directory {dir_path} does not exist")
            
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            print(f"Schema file read successfully, {len(schema_sql)} characters")
            
            # Log first few lines of schema for debugging
            schema_lines = schema_sql.split('\n')[:10]
            print("First 10 lines of schema:")
            for i, line in enumerate(schema_lines, 1):
                print(f"  {i}: {line}")
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # First, check if tables already exist
            try:
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'scraping_workers'")
                workers_table = cursor.fetchone()
                print(f"scraping_workers table exists: {workers_table is not None}")
                
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'scraping_databases'")
                databases_table = cursor.fetchone()
                print(f"scraping_databases table exists: {databases_table is not None}")
                
                if workers_table and databases_table:
                    print("Database tables already exist, skipping schema initialization")
                    cursor.close()
                    release_db_connection(conn)
                else:
                    print("Tables not found, proceeding with schema initialization...")
                    
                    # Execute the entire schema at once to handle PL/pgSQL functions properly
                    try:
                        cursor.execute(schema_sql)
                        conn.commit()
                        app.logger.info("Database schema initialized successfully (full execution)")
                        print("Database schema initialized successfully (full execution)")
                    except Exception as e:
                        conn.rollback()
                        print(f"Full schema execution failed, trying individual statements: {e}")
                        app.logger.warning(f"Full schema execution failed, trying individual statements: {e}")
                        
                        # Split schema into different types of statements
                        create_table_statements = []
                        create_function_statements = []
                        create_trigger_statements = []
                        create_index_statements = []
                        create_view_statements = []
                        insert_statements = []
                        
                        current_statement = []
                        in_function = False
                        in_trigger = False
                        
                        for line in schema_sql.split('\n'):
                            line = line.strip()
                            if line.startswith('--') or not line:
                                continue
                            
                            # Detect statement types
                            if line.startswith('CREATE OR REPLACE FUNCTION'):
                                in_function = True
                                current_statement.append(line)
                            elif line.startswith('CREATE TRIGGER'):
                                in_trigger = True
                                current_statement.append(line)
                            elif line.startswith('$$ language \'plpgsql\';'):
                                current_statement.append(line)
                                if in_function:
                                    create_function_statements.append('\n'.join(current_statement))
                                    in_function = False
                                elif in_trigger:
                                    create_trigger_statements.append('\n'.join(current_statement))
                                    in_trigger = False
                                current_statement = []
                            elif in_function or in_trigger:
                                current_statement.append(line)
                            elif ';' in line:
                                parts = line.split(';')
                                if parts[0]:
                                    current_statement.append(parts[0])
                                full_statement = '\n'.join(current_statement).strip()
                                
                                if full_statement:
                                    if full_statement.startswith('CREATE TABLE'):
                                        create_table_statements.append(full_statement)
                                    elif full_statement.startswith('CREATE INDEX'):
                                        create_index_statements.append(full_statement)
                                    elif full_statement.startswith('CREATE VIEW'):
                                        create_view_statements.append(full_statement)
                                    elif full_statement.startswith('INSERT INTO'):
                                        insert_statements.append(full_statement)
                                
                                current_statement = []
                            else:
                                current_statement.append(line)
                        
                        # Execute statements in order of dependency
                        execution_order = [
                            ("Tables", create_table_statements),
                            ("Functions", create_function_statements), 
                            ("Triggers", create_trigger_statements),
                            ("Indexes", create_index_statements),
                            ("Views", create_view_statements),
                            ("Sample Data", insert_statements)
                        ]
                        
                        for stmt_type, statements in execution_order:
                            print(f"Creating {stmt_type} ({len(statements)} statements)...")
                            app.logger.info(f"Creating {stmt_type}...")
                            for statement in statements:
                                try:
                                    cursor.execute(statement)
                                    conn.commit()
                                    print(f"  ✓ Created {stmt_type[:-1]} successfully")
                                except Exception as stmt_e:
                                    if 'already exists' in str(stmt_e).lower() or 'duplicate' in str(stmt_e).lower():
                                        print(f"  - Skipped existing {stmt_type.lower()[:-1]}")
                                        app.logger.info(f"Skipped existing {stmt_type.lower()[:-1]}")
                                    else:
                                        conn.rollback()
                                        print(f"  ✗ Error creating {stmt_type.lower()[:-1]}: {stmt_e}")
                                        app.logger.error(f"Error creating {stmt_type.lower()[:-1]}: {stmt_e}")
                        
                        print("Database schema initialized successfully (individual statements)")
                        app.logger.info("Database schema initialized successfully (individual statements)")
                    
                    cursor.close()
                    release_db_connection(conn)
            except Exception as schema_e:
                print(f"Error executing schema: {schema_e}")
                app.logger.error(f"Schema execution error: {schema_e}")
                raise
        
        app.logger.info("Database initialized successfully")
        print("="*50)
        print("SCHEMA INITIALIZATION COMPLETED")
        print("="*50)
        
    except Exception as e:
        print("="*50)
        print("SCHEMA INITIALIZATION FAILED")
        print(f"ERROR: {e}")
        print(f"ERROR TYPE: {type(e).__name__}")
        import traceback
        print(f"TRACEBACK:")
        traceback.print_exc()
        print("="*50)
        app.logger.error(f"Database initialization error: {e}")
        app.logger.error(f"Database initialization error type: {type(e).__name__}")
        app.logger.error(f"Database initialization traceback: {traceback.format_exc()}")

# Initialize database schema when Flask app starts
# Note: before_first_request is deprecated in Flask 2.3+, using with_app_context instead
def initialize_schema_on_first_request():
    """Initialize database schema on first request"""
    print("First request detected, initializing database schema...")
    initialize_database_schema()

# Register a before_request handler to initialize schema on first request
@app.before_request
def ensure_schema_initialized():
    """Ensure database schema is initialized before serving requests"""
    if not hasattr(g, 'schema_initialized'):
        try:
            # Check if tables exist, if not initialize
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'scraping_workers')")
            tables_exist = cursor.fetchone()[0]
            cursor.close()
            release_db_connection(conn)
            
            if not tables_exist:
                print("Database tables not found, initializing schema...")
                initialize_database_schema()
            
            g.schema_initialized = True
        except Exception as e:
            print(f"Error checking schema initialization: {e}")
            g.schema_initialized = False

# Also try to initialize on startup for development server
try:
    print("Attempting database schema initialization on startup...")
    initialize_database_schema()
except Exception as e:
    print(f"Startup schema initialization failed (will retry on first request): {e}")

@app.route('/databases/<int:database_id>/delete')
@login_required
def delete_database(database_id):
    """Delete database"""
    try:
        execute_query("DELETE FROM scraping_databases WHERE id = %s", (database_id,), fetch=False, commit=True)
        flash('Database deleted successfully!', 'success')
        return redirect(url_for('list_databases'))
    
    except Exception as e:
        app.logger.error(f"Database deletion error: {e}")
        flash(f'Error deleting database: {str(e)}', 'error')
        return redirect(url_for('list_databases'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)


