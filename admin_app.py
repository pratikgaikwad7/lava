from flask import Flask, render_template, redirect, url_for, request, flash, send_file, jsonify, session
from datetime import datetime, timedelta, time, date
import os
import pymysql
import pandas as pd
import re
from qr_handler import QRHandler
from utils import Config, Constants, get_db_connection, load_training_data, format_program_dates, process_eor_excel, process_training_excel
from attendance_app import attendance_bp
from target import target_bp
from user_technician import user_tech_bp
from flask import send_from_directory
from tni_shared import tni_shared_bp
from feedback_form import feedback_bp
from ciro import ciro_bp  # Import the blueprint

from cd_data_store import bp as cd_data_bp
from factory_data import factory_bp
from user_routes import user_bp
from user_auth import user_auth

# Initialize Flask app
app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Change this to a secure secret key

# Register blueprints
app.register_blueprint(attendance_bp, url_prefix='/attendance')
app.register_blueprint(target_bp)
app.register_blueprint(tni_shared_bp)
app.register_blueprint(factory_bp)
app.register_blueprint(user_bp)
app.register_blueprint(feedback_bp, url_prefix='/feedback')
app.register_blueprint(user_tech_bp, url_prefix='/user_tech')
app.register_blueprint(cd_data_bp)
app.register_blueprint(ciro_bp, url_prefix='/ciro')

app.register_blueprint(user_auth, url_prefix='/auth')

# Set configuration from utils
app.config.update({
    'DB_HOST': Config.DB_HOST,
    'DB_USER': Config.DB_USER,
    'DB_PASSWORD': Config.DB_PASSWORD,
    'DB_NAME': Config.DB_NAME,
    'PROGRAM_DATA_FILE': Config.PROGRAM_DATA_FILE,
    'QR_FOLDER': Config.QR_FOLDER,
    'EOR_FILENAME': Config.EOR_FILENAME
})

# Initialize QR Handler
qr_handler = QRHandler(app)
attendance_bp.qr_handler = qr_handler

# Authentication helper functions
def is_logged_in():
    return 'logged_in' in session and session['logged_in']

def has_role(role_name):
    return is_logged_in() and session.get('role') == role_name

def get_current_user():
    if is_logged_in():
        return {
            'id': session.get('user_id'),
            'username': session.get('username'),
            'role': session.get('role'),
            'factory_location': session.get('factory_location')
        }
    return None

# Login check before each request
@app.before_request
def require_login():
    """
    Allow access if:
    - Endpoint is in allowed_routes
    - Endpoint belongs to attendance blueprint
    - Endpoint is feedback.clubbed_form (public access)
    Otherwise, redirect to login.
    """
    allowed_routes = ['user_auth.login', 'user_auth.logout', 'static', 'home', 'feedback.clubbed_form', 'feedback.feedback_form', 'feedback.submit_feedback', 'feedback.submit_clubbed_feedback', 'feedback.verify_employee', 'feedback.success']

    # Allow all attendance blueprint routes
    if request.endpoint and request.endpoint.startswith('attendance.'):
        return

    # Allow explicitly allowed routes
    if request.endpoint and request.endpoint in allowed_routes:
        return

    # Allow feedback form routes (public access)
    if request.endpoint and request.endpoint.startswith('feedback.'):
        return

    # Redirect to login if not logged in
    if not is_logged_in():
        flash('You must be logged in to access this page', 'error')
        return redirect(url_for('user_auth.login'))

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'xlsx'}

@app.route('/')
def home():
    """Simple homepage with link to admin portal"""
    return render_template("homepage.html")

@app.route('/admin')
def admin_home():
    """Admin dashboard showing recent programs"""
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to access the admin dashboard', 'error')
        return redirect(url_for('home'))
    
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return render_template('admin/admin_home.html')
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, training_name, program_type, location_hall,
                       start_date, end_date, start_time, end_time, duration_days,
                       DATE_FORMAT(start_date, '%%d/%%m/%%Y') as formatted_start_date,
                       DATE_FORMAT(end_date, '%%d/%%m/%%Y') as formatted_end_date,
                       TIME_FORMAT(start_time, '%%H:%%i') as formatted_start_time,
                       TIME_FORMAT(end_time, '%%H:%%i') as formatted_end_time
                FROM training_programs
                ORDER BY start_date DESC, start_time DESC
                LIMIT 5
            """)
            recent_programs = cursor.fetchall()
        return render_template("admin/admin_home.html",
                             recent_programs=recent_programs,
                             current_date=datetime.now().date(),
                             user=get_current_user())
    except Exception as e:
        print(f"Database error: {e}")
        flash('Error fetching recent programs', 'error')
        return render_template('admin/admin_home.html', user=get_current_user())
    finally:
        conn.close()

@app.route('/get_training_names')
def get_training_names():
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    tni_status = request.args.get('tni_status', 'TNI')
    training_data = load_training_data(tni_status)
    
    return jsonify(training_data)
@app.route('/dashboard')
def dashboard():
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to access the dashboard', 'error')
        return redirect(url_for('home'))
    
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('admin_home'))
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, location_hall,
                       DATE_FORMAT(start_date, '%%d/%%m/%%Y') as start_date,
                       DATE_FORMAT(end_date, '%%d/%%m/%%Y') as end_date,
                       TIME_FORMAT(start_time, '%%H:%%i') as start_time,
                       TIME_FORMAT(end_time, '%%H:%%i') as end_time,
                       learning_hours, duration_days, program_type, tni_status,
                       faculty_1, faculty_2, faculty_3, qr_code_path
                FROM training_programs
                ORDER BY start_date DESC, start_time DESC
                LIMIT 10
            """)
            programs_data = cursor.fetchall()
        return render_template("admin/admin_home.html", 
                            programs=programs_data,
                            current_month=datetime.now().strftime('%B'),
                            user=get_current_user())
    except Exception as e:
        print(f"Database error: {e}")
        flash('Error fetching records', 'error')
        return redirect(url_for('admin_home'))
    finally:
        conn.close()

@app.route('/schedule_program', methods=['GET', 'POST'])
def schedule_program():
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to schedule programs', 'error')
        return redirect(url_for('home'))
    
    tni_status = request.form.get('tni_status', 'TNI') if request.method == 'POST' else 'TNI'
    training_data = load_training_data(tni_status)
    
    if request.method == 'POST':
        required_fields = [
            'training_name', 'location_hall', 'start_date', 
            'start_time', 'end_time', 'program_type', 
            'tni_status'
        ]
        
        if not all(request.form.get(field) for field in required_fields):
            flash('Please fill all required fields', 'error')
            return redirect(url_for('schedule_program'))
        try:
            # Get the selected training to fetch its actual duration
            selected_training = next(
                (t for t in training_data if t['training_name'] == request.form['training_name']),
                None
            )
            
            if not selected_training:
                flash('Selected training not found', 'error')
                return redirect(url_for('schedule_program'))
                
            # Get actual learning hours from Excel
            learning_hours = float(selected_training['learning_hours'])
            
            # Calculate duration_days based on actual hours (max 3 days)
            duration_days = min(3, max(1, round(learning_hours / 8)))
            
            start_datetime = datetime.strptime(
                f"{request.form['start_date']} {request.form['start_time']}", 
                "%Y-%m-%d %H:%M"
            )
            end_time = datetime.strptime(request.form['end_time'], "%H:%M").time()
            
            # Calculate end date based on duration_days
            end_date = (start_datetime + timedelta(days=duration_days-1)).date()
            qr_valid_from = start_datetime - timedelta(minutes=Config.QR_BUFFER_MINUTES)
            qr_valid_to = datetime.combine(end_date, end_time)
        except Exception as e:
            flash(f'Error calculating program times: {str(e)}', 'error')
            return redirect(url_for('schedule_program'))
       
        conn = get_db_connection()
        if not conn:
            flash('Database connection error', 'error')
            return redirect(url_for('schedule_program'))
            
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                INSERT INTO training_programs (
                    training_name, pmo_training_category, pl_category,
                    brsr_sq_123_category, location_hall, start_date,
                    end_date, start_time, end_time, learning_hours,
                    program_type, tni_status, faculty_1, faculty_2,
                    faculty_3, faculty_4, created_at,
                    qr_valid_from, qr_valid_to, qr_active, duration_days
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, TRUE, %s)
                """, (
                    request.form['training_name'],
                    request.form.get('pmo_training_category', ''),
                    request.form.get('pl_category', ''),
                    request.form.get('brsr_sq_123_category', ''),
                    request.form['location_hall'],
                    request.form['start_date'],
                    end_date.strftime('%Y-%m-%d'),
                    request.form['start_time'],
                    request.form['end_time'],
                    learning_hours,  # Actual hours from Excel
                    request.form['program_type'],
                    request.form['tni_status'],
                    request.form.get('faculty_1', ''),
                    request.form.get('faculty_2', ''),
                    request.form.get('faculty_3', ''),
                    request.form.get('faculty_4', ''), 
                    qr_valid_from,
                    qr_valid_to,
                    duration_days  # Calculated based on learning_hours
                ))
                program_id = cursor.lastrowid
                
                # Generate only attendance QR code
                qr_filename = qr_handler.generate_attendance_qr_code(
                    program_id=program_id,
                    training_name=request.form['training_name'],
                    location_hall=request.form['location_hall'],
                    start_datetime=start_datetime,
                    end_datetime=datetime.combine(start_datetime.date(), end_time),
                    duration_days=duration_days
                )
                
                # Update database with attendance QR code path
                cursor.execute("""
                UPDATE training_programs 
                SET qr_code_path = %s
                WHERE id = %s
                """, (qr_filename, program_id))
                
                conn.commit()
                flash('Training program scheduled successfully with attendance QR code!', 'success')
                return redirect(url_for('view_program', program_id=program_id))
                
        except Exception as e:
            conn.rollback()
            flash(f'Error scheduling program: {str(e)}', 'error')
            return render_template('admin/schedule_program.html',
                               training_data=training_data,
                               location_halls=Constants.LOCATION_HALLS,
                               program_types=Constants.PROGRAM_TYPES,
                               tni_options=Constants.TNI_OPTIONS,
                               duration_options=[1, 2, 3],
                               time_slots=Constants.TIME_SLOTS,
                               form_data=request.form,
                               user=get_current_user())
        finally:
            conn.close()
    
    return render_template('admin/schedule_program.html',
                         training_data=training_data,
                         location_halls=Constants.LOCATION_HALLS,
                         program_types=Constants.PROGRAM_TYPES,
                         tni_options=Constants.TNI_OPTIONS,
                         duration_options=[1, 2, 3],
                         time_slots=Constants.TIME_SLOTS,
                         form_data=request.form if request.method == 'POST' else None,
                         user=get_current_user())

@app.route('/program/<int:program_id>')
def view_program(program_id):
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to view program details', 'error')
        return redirect(url_for('home'))
    
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('dashboard'))
        
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT *, 
                DATE_FORMAT(start_date, '%%d/%%m/%%Y') as formatted_start_date,
                DATE_FORMAT(end_date, '%%d/%%m/%%Y') as formatted_end_date,
                TIME_FORMAT(start_time, '%%H:%%i') as formatted_start_time,
                TIME_FORMAT(end_time, '%%H:%%i') as formatted_end_time,
                DATE_FORMAT(qr_valid_from, '%%d/%%m/%%Y %%H:%%i') as qr_valid_from,
                DATE_FORMAT(qr_valid_to, '%%d/%%m/%%Y %%H:%%i') as qr_valid_to,
                qr_active, duration_days
                FROM training_programs WHERE id = %s
            """, (program_id,))
            program = cursor.fetchone()
            
            if not program:
                flash('Program not found', 'error')
                return redirect(url_for('dashboard'))
            
            return render_template('admin/view_program.html',
                                program=program,
                                location_halls=Constants.LOCATION_HALLS,
                                program_types=Constants.PROGRAM_TYPES,
                                tni_options=Constants.TNI_OPTIONS,
                                time_slots=Constants.TIME_SLOTS,
                                duration_options=[1, 2, 3],
                                user=get_current_user())
    except Exception as e:
        print(f"Database error: {e}")
        flash('Error fetching program details', 'error')
        return redirect(url_for('dashboard'))
    finally:
        conn.close()

@app.route('/program/<int:program_id>/toggle_qr', methods=['POST'])
def toggle_qr_status(program_id):
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to toggle QR status', 'error')
        return redirect(url_for('view_program', program_id=program_id))
    
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('view_program', program_id=program_id))
    try:
        with conn.cursor() as cursor:
            # Get current status and validity period
            cursor.execute("""
                SELECT qr_active, qr_valid_from, qr_valid_to 
                FROM training_programs 
                WHERE id = %s
            """, (program_id,))
            program = cursor.fetchone()
            
            if not program:
                flash('Program not found', 'error')
                return redirect(url_for('dashboard'))
            
            # Check if current time is within QR valid period
            now = datetime.now()
            if now < program['qr_valid_from'] or now > program['qr_valid_to']:
                flash('Cannot toggle QR status outside of valid period', 'error')
                return redirect(url_for('view_program', program_id=program_id))
            
            # Toggle the status
            new_status = not program['qr_active']
            cursor.execute("""
                UPDATE training_programs 
                SET qr_active = %s 
                WHERE id = %s
            """, (new_status, program_id))
            conn.commit()
            
            status_msg = "activated" if new_status else "deactivated"
            flash(f'QR code {status_msg} successfully!', 'success')
            
    except Exception as e:
        conn.rollback()
        print(f"Error toggling QR status: {e}")
        flash('Error updating QR status', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('view_program', program_id=program_id))

@app.route('/qrcode/<int:program_id>')
def get_qrcode(program_id):
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to view QR codes', 'error')
        return redirect(url_for('home'))
    
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('dashboard'))
        
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT qr_code_path as qr_path, location_hall 
                FROM training_programs 
                WHERE id = %s
            """, (program_id,))
            result = cursor.fetchone()
            
            if not result or not result['qr_path']:
                flash('Attendance QR Code not found for this program', 'error')
                return redirect(url_for('dashboard'))
            
            filepath = os.path.join(app.config['QR_FOLDER'], result['qr_path'])
            
            if not os.path.exists(filepath):
                flash('Attendance QR Code file not found', 'error')
                return redirect(url_for('dashboard'))
            
            return send_file(filepath, mimetype='image/png')
    except Exception as e:
        print(f"Database error: {e}")
        flash('Error fetching QR code', 'error')
        return redirect(url_for('dashboard'))
    finally:
        conn.close()

@app.route('/attendance/<int:program_id>', methods=['GET', 'POST'])
def submit_attendance(program_id):
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('admin_home'))
        
    try:
        with conn.cursor() as cursor:
            # Get program details with time validation info
            cursor.execute("""
                SELECT *, 
                DATE_FORMAT(start_date, '%Y-%%m-%%d') as start_date_str,
                DATE_FORMAT(end_date, '%Y-%%m-%%d') as end_date_str,
                TIME_FORMAT(start_time, '%%H:%%i') as start_time_str,
                TIME_FORMAT(end_time, '%%H:%%i') as end_time_str,
                qr_valid_from, qr_valid_to, qr_active
                FROM training_programs WHERE id = %s
            """, (program_id,))
            program = cursor.fetchone()
            
            if not program:
                flash('Invalid program ID', 'error')
                return redirect(url_for('admin_home'))
            
            now = datetime.now()
            
            # Create datetime objects for comparison
            start_datetime = datetime.strptime(
                f"{program['start_date_str']} {program['start_time_str']}", 
                "%Y-%m-%d %H:%M"
            )
            end_datetime = datetime.strptime(
                f"{program['end_date_str']} {program['end_time_str']}", 
                "%Y-%m-%d %H:%M"
            )
            
            # Check if QR code is valid based on time
            if now < program['qr_valid_from']:
                return render_template('admin/attendance_closed.html', 
                                    program=program,
                                    status='not_started',
                                    message='Attendance not open yet',
                                    valid_from=program['qr_valid_from'].strftime('%d/%m/%Y %H:%M'))
            elif now > program['qr_valid_to']:
                return render_template('admin/attendance_closed.html', 
                                    program=program,
                                    status='ended',
                                    message='Attendance period has ended',
                                    valid_to=program['qr_valid_to'].strftime('%d/%m/%Y %H:%M'))
            
            # Check if admin has manually deactivated the QR code
            if not program['qr_active']:
                return render_template('admin/attendance_closed.html', 
                                    program=program,
                                    status='deactivated',
                                    message='Attendance is currently disabled by administrator')
            
            if request.method == 'POST':
                # Process attendance (unchanged)
                pass
            
            # Format times for display
            program['formatted_start'] = start_datetime.strftime('%d/%m/%Y %H:%M')
            program['formatted_end'] = end_datetime.strftime('%d/%m/%Y %H:%M')
            program['formatted_valid_from'] = program['qr_valid_from'].strftime('%d/%m/%Y %H:%M')
            program['formatted_valid_to'] = program['qr_valid_to'].strftime('%d/%m/%Y %H:%M')
            
            return render_template('admin/submit_attendance.html', program=program)
    except Exception as e:
        conn.rollback()
        print(f"Error in attendance submission: {e}")
        flash('Error submitting attendance', 'error')
        return redirect(url_for('admin_home'))
    finally:
        conn.close()
        
@app.route('/programs')
def training_programs():
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to view training programs', 'error')
        return redirect(url_for('home'))

    conn = get_db_connection()
    if not conn:
        print("ERROR: Database connection failed")  # Debug print
        flash('Database connection error', 'error')
        return redirect(url_for('admin_home'))

    try:
        # Get filter parameters - FIXED: removed trailing commas
        location = request.args.get('location', '')
        status = request.args.get('status', '')  # Fixed: removed trailing comma
        search = request.args.get('search', '')  # Fixed: removed trailing comma
        page = request.args.get('page', 1, type=int)
        per_page = 10
        
        print(f"DEBUG: Filters - location: {location}, status: {status}, search: {search}")  # Debug print

        with conn.cursor() as cursor:
            # Base query - Fix: escape % characters by doubling them (%%)
            base_sql = """
                SELECT id, training_name, program_type, location_hall,
                       start_date, end_date, start_time, end_time,
                       learning_hours,
                       DATE_FORMAT(start_date, '%%d/%%m/%%Y') as formatted_start_date,
                       DATE_FORMAT(end_date, '%%d/%%m/%%Y') as formatted_end_date,
                       TIME_FORMAT(start_time, '%%H:%%i') as formatted_start_time,
                       TIME_FORMAT(end_time, '%%H:%%i') as formatted_end_time
                FROM training_programs
                WHERE 1=1
            """
            params = []

            # Apply filters
            if location:
                base_sql += " AND location_hall = %s"
                params.append(location)

            if status == 'scheduled':
                base_sql += " AND end_date >= CURDATE()"
            elif status == 'completed':
                base_sql += " AND end_date < CURDATE()"

            if search:
                base_sql += " AND (training_name LIKE %s OR location_hall LIKE %s)"
                params.extend([f"%{search}%", f"%{search}%"])

            # Count total records for pagination
            count_sql = "SELECT COUNT(*) as total FROM (" + base_sql + ") AS subquery"
            cursor.execute(count_sql, params)
            total = cursor.fetchone()['total']
            
            print(f"DEBUG: Total records found: {total}")  # Debug print

            # Add sorting and pagination
            paginated_sql = base_sql + " ORDER BY start_date DESC, start_time DESC LIMIT %s OFFSET %s"
            paginated_params = params + [per_page, (page - 1) * per_page]

            print(f"DEBUG: Executing SQL: {paginated_sql}")  # Debug print
            print(f"DEBUG: With params: {paginated_params}")  # Debug print
            
            cursor.execute(paginated_sql, paginated_params)
            programs = cursor.fetchall()
            
            print(f"DEBUG: Programs fetched: {len(programs)}")  # Debug print
            for program in programs:
                print(f"DEBUG: Program - {program['training_name']}")  # Debug print

        # Create pagination object
        pagination = {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page,
            'has_prev': page > 1,
            'has_next': page * per_page < total,
            'prev_num': page - 1,
            'next_num': page + 1,
            'iter_pages': lambda: range(1, ((total + per_page - 1) // per_page) + 1)
        }

        return render_template(
            "admin/training_programs.html",
            programs=programs,
            pagination=pagination,
            location_halls=Constants.LOCATION_HALLS,
            current_date=datetime.now().date(),
            user=get_current_user()
        )

    except pymysql.Error as e:
        print(f"Database error: {e}")
        flash('Error fetching training programs', 'error')
        return redirect(url_for('dashboard'))

    finally:
        conn.close()
@app.route('/program/<int:program_id>/delete', methods=['POST'])
def delete_program(program_id):
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to delete programs', 'error')
        return redirect(url_for('training_programs'))
    
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('training_programs'))
    try:
        with conn.cursor() as cursor:
            # First get the QR code path to delete the file
            cursor.execute("SELECT qr_code_path FROM training_programs WHERE id = %s", (program_id,))
            result = cursor.fetchone()
            
            if result and result['qr_code_path']:
                try:
                    os.remove(os.path.join(app.config['QR_FOLDER'], result['qr_code_path']))
                except OSError:
                    pass  # File might not exist, but we'll proceed with DB deletion
            
            # Delete from database
            cursor.execute("DELETE FROM training_programs WHERE id = %s", (program_id,))
            conn.commit()
            
        flash('Training program deleted successfully', 'success')
    except Exception as e:
        conn.rollback()
        print(f"Error deleting program: {e}")
        flash('Error deleting training program', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('training_programs'))

@app.route('/upload_eor', methods=['GET', 'POST'])
def upload_eor():
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to upload files', 'error')
        return redirect(url_for('home'))
    
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        file_type = request.form.get('file_type')
        
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            try:
                if file_type == 'eor':
                    success, message = process_eor_excel(file.stream)
                elif file_type == 'program_data':
                    success, message = process_training_excel(file.stream)
                else:
                    flash('Invalid file type', 'error')
                    return redirect(request.url)
                
                if success:
                    flash(message, 'success')
                else:
                    flash(message, 'error')
                    
                return redirect(url_for('dashboard'))
            except Exception as e:
                flash(f'Error processing file: {str(e)}', 'error')
                return redirect(request.url)
        else:
            flash('Only .xlsx files are allowed', 'error')
            return redirect(request.url)
    
    # GET request - render the upload form
    return render_template('admin_upload_files.html', user=get_current_user())
# Add these imports at the top if not already present
import json
from werkzeug.utils import secure_filename

# Add this route to render the feedback QR generator page
@app.route('/feedback_qr_generator')
def feedback_qr_generator():
    """Admin page for generating feedback QR codes"""
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to access this page', 'error')
        return redirect(url_for('home'))
    
    return render_template('feedback_qr.html', user=get_current_user())

# Add this route to get programs by date
@app.route('/get_programs_by_date')
def get_programs_by_date():
    """Get training programs scheduled for a specific date"""
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    # Get date from query parameters
    date_str = request.args.get('date')
    if not date_str:
        return jsonify({'error': 'Date parameter is required'}), 400
    
    try:
        # Parse the date
        selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection error'}), 500
    
    try:
        with conn.cursor() as cursor:
            # Fetch programs for the selected date
            cursor.execute("""
                SELECT 
                    id, 
                    training_name, 
                    program_type, 
                    location_hall,
                    DATE_FORMAT(start_date, '%%Y-%%m-%%d') as start_date,
                    DATE_FORMAT(end_date, '%%Y-%%m-%%d') as end_date,
                    TIME_FORMAT(start_time, '%%H:%%i') as start_time,
                    TIME_FORMAT(end_time, '%%H:%%i') as end_time,
                    faculty_1, faculty_2, faculty_3, faculty_4
                FROM training_programs 
                WHERE DATE(start_date) = %s
                ORDER BY start_time
            """, (selected_date,))
            
            programs = cursor.fetchall()
            
            # Format the response
            formatted_programs = []
            for program in programs:
                # Collect all trainers (non-empty faculty fields)
                trainers = []
                for i in range(1, 5):
                    faculty_field = f'faculty_{i}'
                    if program.get(faculty_field):
                        trainers.append(program[faculty_field])
                
                formatted_programs.append({
                    'id': program['id'],
                    'name': program['training_name'],
                    'type': program['program_type'],
                    'location': program['location_hall'],
                    'date': program['start_date'],
                    'start_time': program['start_time'],
                    'end_time': program['end_time'],
                    'trainers': trainers
                })
            
            return jsonify({
                'date': date_str,
                'programs': formatted_programs
            })
            
    except Exception as e:
        print(f"Database error: {e}")
        return jsonify({'error': 'Error fetching programs'}), 500
    finally:
        conn.close()

# Add this route to generate feedback QR code for a single program
@app.route('/generate_feedback_qr', methods=['POST'])
def generate_feedback_qr():
    """Generate feedback QR code for a single program"""
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    program_id = request.form.get('program_id')
    if not program_id:
        return jsonify({'error': 'Program ID is required'}), 400
    
    try:
        program_id = int(program_id)
    except ValueError:
        return jsonify({'error': 'Invalid Program ID'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection error'}), 500
    
    try:
        with conn.cursor() as cursor:
            # Get program details
            cursor.execute("""
                SELECT training_name, location_hall, start_date, start_time
                FROM training_programs 
                WHERE id = %s
            """, (program_id,))
            
            program = cursor.fetchone()
            if not program:
                return jsonify({'error': 'Program not found'}), 404
        
        # Generate QR code
        qr_filename = qr_handler.generate_feedback_qr_code(program_id)
        
        return jsonify({
            'success': True,
            'qr_filename': qr_filename,
            'qr_url': url_for('get_feedback_qr', program_id=program_id),
            'program_name': program['training_name']
        })
        
    except Exception as e:
        print(f"Error generating QR code: {e}")
        return jsonify({'error': 'Error generating QR code'}), 500
    finally:
        conn.close()

# Add this route to generate clubbed feedback QR code for multiple programs
@app.route('/generate_clubbed_feedback_qr', methods=['POST'])
def generate_clubbed_feedback_qr():
    """Generate clubbed feedback QR code for multiple programs"""
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    program_ids_str = request.form.get('program_ids')
    if not program_ids_str:
        return jsonify({'error': 'Program IDs are required'}), 400
    
    try:
        program_ids = [int(pid) for pid in program_ids_str.split(',')]
    except ValueError:
        return jsonify({'error': 'Invalid Program IDs'}), 400
    
    if len(program_ids) < 2:
        return jsonify({'error': 'At least 2 programs are required for clubbed QR code'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection error'}), 500
    
    try:
        with conn.cursor() as cursor:
            # Get program details
            placeholders = ', '.join(['%s'] * len(program_ids))
            cursor.execute(f"""
                SELECT id, training_name
                FROM training_programs 
                WHERE id IN ({placeholders})
            """, program_ids)
            
            programs = cursor.fetchall()
            if len(programs) != len(program_ids):
                return jsonify({'error': 'Some programs not found'}), 404
        
        # Generate QR code
        qr_filename = qr_handler.generate_clubbed_feedback_qr_code(program_ids)
        
        return jsonify({
            'success': True,
            'qr_filename': qr_filename,
            'qr_url': url_for('get_clubbed_feedback_qr', filename=qr_filename),
            'program_names': ', '.join([p['training_name'] for p in programs])
        })
        
    except Exception as e:
        print(f"Error generating clubbed QR code: {e}")
        return jsonify({'error': 'Error generating QR code'}), 500
    finally:
        conn.close()
@app.route('/program/<int:program_id>/edit', methods=['GET', 'POST'])
def edit_program(program_id):
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to edit programs', 'error')
        return redirect(url_for('home'))
    
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('view_program', program_id=program_id))
    
    try:
        with conn.cursor() as cursor:
            if request.method == 'POST':
                # Get form data
                required_fields = [
                    'training_name', 'location_hall', 'start_date', 
                    'start_time', 'end_time', 'program_type', 
                    'tni_status'
                ]
                
                if not all(request.form.get(field) for field in required_fields):
                    flash('Please fill all required fields', 'error')
                    return redirect(url_for('edit_program', program_id=program_id))
                
                try:
                    # Get the selected training to fetch its actual duration
                    tni_status = request.form.get('tni_status', 'TNI')
                    training_data = load_training_data(tni_status)
                    selected_training = next(
                        (t for t in training_data if t['training_name'] == request.form['training_name']),
                        None
                    )
                    
                    if not selected_training:
                        flash('Selected training not found', 'error')
                        return redirect(url_for('edit_program', program_id=program_id))
                        
                    # Get actual learning hours from Excel
                    learning_hours = float(selected_training['learning_hours'])
                    
                    # Calculate duration_days based on actual hours (max 3 days)
                    duration_days = min(3, max(1, round(learning_hours / 8)))
                    
                    start_datetime = datetime.strptime(
                        f"{request.form['start_date']} {request.form['start_time']}", 
                        "%Y-%m-%d %H:%M"
                    )
                    end_time = datetime.strptime(request.form['end_time'], "%H:%M").time()
                    
                    # Calculate end date based on duration_days
                    end_date = (start_datetime + timedelta(days=duration_days-1)).date()
                    qr_valid_from = start_datetime - timedelta(minutes=Config.QR_BUFFER_MINUTES)
                    qr_valid_to = datetime.combine(end_date, end_time)
                except Exception as e:
                    flash(f'Error calculating program times: {str(e)}', 'error')
                    return redirect(url_for('edit_program', program_id=program_id))
                
                # Update the program in the database
                cursor.execute("""
                    UPDATE training_programs SET
                        training_name = %s,
                        pmo_training_category = %s,
                        pl_category = %s,
                        brsr_sq_123_category = %s,
                        location_hall = %s,
                        start_date = %s,
                        end_date = %s,
                        start_time = %s,
                        end_time = %s,
                        learning_hours = %s,
                        program_type = %s,
                        tni_status = %s,
                        faculty_1 = %s,
                        faculty_2 = %s,
                        faculty_3 = %s,
                        faculty_4 = %s,
                        qr_valid_from = %s,
                        qr_valid_to = %s,
                        duration_days = %s
                    WHERE id = %s
                """, (
                    request.form['training_name'],
                    request.form.get('pmo_training_category', ''),
                    request.form.get('pl_category', ''),
                    request.form.get('brsr_sq_123_category', ''),
                    request.form['location_hall'],
                    request.form['start_date'],
                    end_date.strftime('%Y-%m-%d'),
                    request.form['start_time'],
                    request.form['end_time'],
                    learning_hours,
                    request.form['program_type'],
                    request.form['tni_status'],
                    request.form.get('faculty_1', ''),
                    request.form.get('faculty_2', ''),
                    request.form.get('faculty_3', ''),
                    request.form.get('faculty_4', ''),
                    qr_valid_from,
                    qr_valid_to,
                    duration_days,
                    program_id
                ))
                
                # Regenerate QR code if schedule changed
                qr_filename = qr_handler.generate_attendance_qr_code(
                    program_id=program_id,
                    training_name=request.form['training_name'],
                    location_hall=request.form['location_hall'],
                    start_datetime=start_datetime,
                    end_datetime=datetime.combine(start_datetime.date(), end_time),
                    duration_days=duration_days
                )
                
                # Update the QR code path in the database
                cursor.execute("""
                    UPDATE training_programs 
                    SET qr_code_path = %s
                    WHERE id = %s
                """, (qr_filename, program_id))
                
                conn.commit()
                flash('Training program updated successfully!', 'success')
                return redirect(url_for('view_program', program_id=program_id))
                
            else:  # GET request
                # Fixed: Escaped % characters in DATE_FORMAT
                cursor.execute("""
                    SELECT *,
                    DATE_FORMAT(start_date, '%%Y-%%m-%%d') as start_date,
                    DATE_FORMAT(end_date, '%%Y-%%m-%%d') as end_date,
                    TIME_FORMAT(start_time, '%%H:%%i') as start_time,
                    TIME_FORMAT(end_time, '%%H:%%i') as end_time
                    FROM training_programs WHERE id = %s
                """, (program_id,))
                program = cursor.fetchone()
                
                if not program:
                    flash('Program not found', 'error')
                    return redirect(url_for('dashboard'))
                
                # Load training data for the form
                training_data = load_training_data(program['tni_status'])
                
                return render_template('admin/edit_program.html',
                                     program=program,
                                     training_data=training_data,
                                     location_halls=Constants.LOCATION_HALLS,
                                     program_types=Constants.PROGRAM_TYPES,
                                     tni_options=Constants.TNI_OPTIONS,
                                     duration_options=[1, 2, 3],
                                     time_slots=Constants.TIME_SLOTS,
                                     form_data=program,  # Pre-fill the form with current data
                                     user=get_current_user())
    
    except Exception as e:
        conn.rollback()
        print(f"Error editing program: {e}")
        flash('Error updating program', 'error')
        return redirect(url_for('view_program', program_id=program_id))
    finally:
        conn.close()
# Add this route to serve feedback QR code for a program
@app.route('/feedback_qr/<int:program_id>')
def get_feedback_qr(program_id):
    """Serve feedback QR code for a program"""
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to view QR codes', 'error')
        return redirect(url_for('home'))
    
    filepath = qr_handler.get_feedback_qr_path(program_id)
    
    if not filepath:
        flash('Feedback QR Code not found for this program', 'error')
        return redirect(url_for('feedback_qr_generator'))
    
    return send_file(filepath, mimetype='image/png')

# Add this route to serve clubbed feedback QR code
@app.route('/clubbed_feedback_qr/<filename>')
def get_clubbed_feedback_qr(filename):
    """Serve clubbed feedback QR code"""
    # Check if user is logged in and has Admin role
    if not has_role('Admin'):
        flash('You do not have permission to view QR codes', 'error')
        return redirect(url_for('home'))
    
    filepath = os.path.join(app.config['QR_FOLDER'], filename)
    
    if not os.path.exists(filepath):
        flash('Clubbed Feedback QR Code not found', 'error')
        return redirect(url_for('feedback_qr_generator'))
    
    return send_file(filepath, mimetype='image/png')
    

# Add this route to serve CSS files from the style folder
@app.route('/style/<path:filename>')
def style_files(filename):
    return send_from_directory('style', filename)

@app.route('/image/<path:filename>')
def serve_image(filename):
    return send_from_directory('image', filename)

if __name__ == '__main__':
    from view_master_data import view_bp
    app.register_blueprint(view_bp)
    app.run(host='0.0.0.0', port=5003, debug=True)