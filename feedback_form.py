from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify, session
from datetime import datetime
import pymysql
from utils import get_db_connection, Config
import os
import pandas as pd
import uuid

feedback_bp = Blueprint('feedback', __name__, template_folder='templates/admin')

@feedback_bp.route('/form/<int:program_id>', methods=['GET'])
def feedback_form(program_id):
    conn = None
    program_title = None
    program_date = None
    trainers = []
    error = None
    # New fields
    pmo_training_category = None
    pl_category = None
    brsr_sq_123_category = None
    # Added fields
    tni_status = None
    learning_hours = None

    try:
        conn = get_db_connection()
        
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            # Updated query to include new fields
            cursor.execute("""
                SELECT 
                    id, training_name as program_title, 
                    DATE(start_date) as program_date,
                    faculty_1, faculty_2, faculty_3, faculty_4,
                    pmo_training_category, pl_category, brsr_sq_123_category,
                    tni_status, learning_hours
                FROM training_programs 
                WHERE id = %s
            """, (program_id,))
            program = cursor.fetchone()

            if not program:
                flash('Program not found', 'error')
                return redirect(url_for('admin_home'))

            program_title = program['program_title']
            program_date = program['program_date'].strftime('%Y-%m-%d') if program['program_date'] else None
            # Get new field values
            pmo_training_category = program.get('pmo_training_category', '')
            pl_category = program.get('pl_category', '')
            brsr_sq_123_category = program.get('brsr_sq_123_category', '')
            # Get added field values
            tni_status = program.get('tni_status', '')
            learning_hours = program.get('learning_hours', '')

            # Collect all trainers (non-empty faculty fields)
            for i in range(1, 5):
                faculty_field = f'faculty_{i}'
                if program.get(faculty_field) and program.get(faculty_field).strip():
                    trainers.append({
                        'name': program[faculty_field].strip(),
                        'num': i
                    })

            if not trainers:
                flash('No trainers found for this program', 'warning')

    except Exception as e:
        current_app.logger.error(f"Error loading feedback form: {str(e)}")
        flash(f'Error loading form: {str(e)}', 'error')
        return redirect(url_for('feedback.feedback_form', program_id=program_id))
    finally:
        if conn:
            conn.close()

    # Store program_id in session for later use
    session['current_program_id'] = program_id

    # Create a programs list with one program to use the same template as clubbed form
    programs = [{
        'id': program_id,
        'training_name': program_title,
        'start_date': program_date,
        'end_date': None,
        'trainers': trainers,
        'pmo_training_category': pmo_training_category,
        'pl_category': pl_category,
        'brsr_sq_123_category': brsr_sq_123_category,
        'tni_status': tni_status,
        'learning_hours': learning_hours
    }]
    
    return render_template(
        'admin/clubbed_feedback_form.html',
        programs=programs,
        program_ids_str=str(program_id)
    )

@feedback_bp.route('/verify_employee', methods=['POST'])
def verify_employee():
    per_no = request.form.get('per_no')
    if not per_no:
        return jsonify({'success': False, 'error': 'PER number is required'})
    
    employee = get_employee_details(per_no)
    if not employee:
        return jsonify({'success': False, 'error': 'Employee not found'})
    
    return jsonify({
        'success': True,
        'employee': employee
    })

@feedback_bp.route('/submit_feedback', methods=['POST'])
def submit_feedback():
    conn = None
    try:
        # Get program_id from form or session
        program_id = request.form.get('program_id') or session.get('current_program_id')
        if not program_id:
            flash('Program ID is missing', 'error')
            return redirect(url_for('feedback.feedback_form', program_id=1))

        # Validate required fields
        required_fields = [
            'program_title', 'program_date', 'per_no',
            'participants_name', 'senior_name', 'phone'
        ]
        missing_fields = [f for f in required_fields if not request.form.get(f)]
        if missing_fields:
            flash(f'Missing required fields: {", ".join(missing_fields)}', 'error')
            return redirect(url_for('feedback.feedback_form', program_id=program_id))

        # Parse program date
        try:
            program_date = datetime.strptime(request.form['program_date'], '%Y-%m-%d').date()
        except ValueError:
            flash('Invalid program date format', 'error')
            return redirect(url_for('feedback.feedback_form', program_id=program_id))

        # Get employee details
        employee = get_employee_details(request.form['per_no'])
        if not employee:
            flash('Employee verification failed', 'error')
            return redirect(url_for('feedback.feedback_form', program_id=program_id))

        # Validate participant name matches EOR
        if request.form['participants_name'].strip().upper() != employee['participants_name'].strip().upper():
            flash('Participant name does not match employee records', 'error')
            return redirect(url_for('feedback.feedback_form', program_id=program_id))

        # Helper to parse int safely
        def get_int(name):
            val = request.form.get(name)
            return int(val) if val and val.isdigit() else None
        
        # Helper to parse decimal safely
        def get_decimal(name):
            val = request.form.get(name)
            return float(val) if val else None

        # Generate a unique session ID for this feedback
        clubbed_session_id = str(uuid.uuid4())
        
        # Prepare base data
        data = {
            'program_id': program_id,
            'program_title': request.form['program_title'].strip(),
            'program_date': program_date,
            'per_no': request.form['per_no'].strip(),
            'participants_name': request.form['participants_name'].strip(),
            'bc_no': employee['bc_no'].strip(),
            'gender': employee['gender'].strip(),
            'employee_group': employee['employee_group'].strip(),
            'department': employee['department'].strip(),
            'factory': employee['factory'].strip(),
            'phone': request.form['phone'].strip(),
            'senior_name': request.form['senior_name'].strip(),
            'clubbed_session_id': clubbed_session_id,
            # Section ratings - now common for all programs
            'sec1_q1': get_int('sec1_q1'),
            'sec1_q2': get_int('sec1_q2'),
            'sec2_q1': get_int('sec2_q1'),
            'sec2_q2': get_int('sec2_q2'),
            'sec2_q3': get_int('sec2_q3'),
            'sec3_q1': get_int('sec3_q1'),
            'sec5_q1': get_int('sec5_q1'),
            'sec5_q2': get_int('sec5_q2'),
            'sec6_q1': get_int('sec6_q1'),
            'sec6_q2': get_int('sec6_q2'),
            'sec7_q1': get_int('sec7_q1'),
            'sec7_q2': get_int('sec7_q2'),
            # Text fields - not mandatory
            'sec7_q3_text': request.form.get('sec7_q3_text', '').strip() or None,
            'sec7_q4_text': request.form.get('sec7_q4_text', '').strip() or None,
            # Common suggestions field
            'suggestions': request.form.get('suggestions', '').strip() or None,
            # New fields - get from form fields for this specific program
            'pmo_training_category': request.form.get(f'program_{program_id}_pmo_training_category', '').strip() or None,
            'pl_category': request.form.get(f'program_{program_id}_pl_category', '').strip() or None,
            'brsr_sq_123_category': request.form.get(f'program_{program_id}_brsr_sq_123_category', '').strip() or None,
            # Added fields
            'tni_status': request.form.get(f'program_{program_id}_tni_status', '').strip() or None,
            'learning_hours': get_decimal(f'program_{program_id}_learning_hours'),
        }

        # Default all trainer fields to None
        for i in range(1, 5):
            data.update({
                f'trainer{i}_name': None,
                f'trainer{i}_q1': None,
                f'trainer{i}_q2': None,
                f'trainer{i}_q3': None,
                f'trainer{i}_q4': None
            })

        # Fill trainer data if provided
        trainer_count = 0
        for i in range(1, 5):
            tname = request.form.get(f'trainer{i}_name', '').strip()
            if tname:
                trainer_count += 1
                data[f'trainer{i}_name'] = tname
                data[f'trainer{i}_q1'] = get_int(f'trainer{i}_q1')
                data[f'trainer{i}_q2'] = get_int(f'trainer{i}_q2')
                data[f'trainer{i}_q3'] = get_int(f'trainer{i}_q3')
                data[f'trainer{i}_q4'] = get_int(f'trainer{i}_q4')

        if trainer_count == 0:
            flash('At least one trainer evaluation is required', 'error')
            return redirect(url_for('feedback.feedback_form', program_id=program_id))

        conn = get_db_connection()
        with conn.cursor() as cursor:
            columns = [
                'program_id', 'program_title', 'program_date', 'per_no', 'participants_name',
                'bc_no', 'gender', 'employee_group', 'department', 'factory', 'phone', 'senior_name',
                'sec1_q1', 'sec1_q2', 'sec2_q1', 'sec2_q2', 'sec2_q3', 'sec3_q1',
                'sec5_q1', 'sec5_q2', 'sec6_q1', 'sec6_q2', 'sec7_q1', 'sec7_q2',
                'sec7_q3_text', 'sec7_q4_text', 'suggestions', 'clubbed_session_id',
                'pmo_training_category', 'pl_category', 'brsr_sq_123_category',  # New fields
                'tni_status', 'learning_hours',  # Added fields
                'trainer1_name', 'trainer1_q1', 'trainer1_q2', 'trainer1_q3', 'trainer1_q4',
                'trainer2_name', 'trainer2_q1', 'trainer2_q2', 'trainer2_q3', 'trainer2_q4',
                'trainer3_name', 'trainer3_q1', 'trainer3_q2', 'trainer3_q3', 'trainer3_q4',
                'trainer4_name', 'trainer4_q1', 'trainer4_q2', 'trainer4_q3', 'trainer4_q4'
            ]
            values = [data.get(col) for col in columns]

            query = f"""
            INSERT INTO feedback_responses ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            """

            cursor.execute(query, values)
            conn.commit()

        flash('Feedback submitted successfully!', 'success')
        return redirect(url_for('feedback.success'))

    except pymysql.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"Database error: {e}")
        flash('Database error occurred. Please try again.', 'error')
    except Exception as e:
        current_app.logger.error(f"Error submitting feedback: {e}")
        current_app.logger.error(f"Form data: {request.form}")
        flash(f'Error submitting feedback: {e}', 'error')
    finally:
        if conn: conn.close()

    return redirect(url_for('feedback.feedback_form', program_id=program_id))

@feedback_bp.route('/success')
def success():
    return render_template('admin/ciro_success.html')

def load_eor_data():
    """Load EOR data from database"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM eor_data")
            eor_data = cursor.fetchall()
        return eor_data
    except Exception as e:
        print(f"Error loading EOR data: {str(e)}")
        return []
    finally:
        if conn:
            conn.close()

def get_employee_details(per_no):
    """Get employee details from EOR database"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT per_no, participants_name, factory, department, gender, 
                       employee_group, employee_subgroup, bc_no
                FROM eor_data 
                WHERE per_no = %s
            """, (str(per_no).strip(),))
            emp = cursor.fetchone()
            
        if emp:
            return {
                'participants_name': emp.get('participants_name', ''),
                'bc_no': emp.get('bc_no', ''),
                'gender': emp.get('gender', ''),
                'employee_group': emp.get('employee_group', ''),
                'department': emp.get('department', ''),
                'factory': emp.get('factory', '')
            }
        return None
        
    except Exception as e:
        print(f"Error fetching employee {per_no}: {e}")
        return None
    finally:
        if conn:
            conn.close()

@feedback_bp.route('/clubbed_form', methods=['GET'])
def clubbed_feedback_form():
    programs_str = request.args.get('programs')
    if not programs_str:
        flash('No programs specified', 'error')
        return redirect(url_for('feedback.feedback_form', program_id=1))
    
    try:
        program_ids = [int(pid) for pid in programs_str.split(',')]
    except ValueError:
        flash('Invalid program IDs', 'error')
        return redirect(url_for('feedback.feedback_form', program_id=1))
    
    # Fetch program details for each ID
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('feedback.feedback_form', program_id=1))
    
    try:
        programs = []
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            # Updated query to include new fields
            placeholders = ', '.join(['%s'] * len(program_ids))
            cursor.execute(f"""
                SELECT id, training_name, start_date, end_date, 
                       faculty_1, faculty_2, faculty_3, faculty_4,
                       pmo_training_category, pl_category, brsr_sq_123_category,
                       tni_status, learning_hours
                FROM training_programs 
                WHERE id IN ({placeholders})
                ORDER BY start_date
            """, program_ids)
            
            for program in cursor.fetchall():
                # Debug: Log program data
                current_app.logger.info(f"Program data: {program}")
                
                # Collect all trainers (non-empty faculty fields)
                trainers = []
                for i in range(1, 5):
                    faculty_field = f'faculty_{i}'
                    faculty_name = program.get(faculty_field)
                    current_app.logger.info(f"Faculty {i}: {faculty_name}")
                    if faculty_name and faculty_name.strip():
                        trainers.append({
                            'name': faculty_name.strip(),
                            'num': i
                        })
                
                current_app.logger.info(f"Program {program['id']} trainers: {trainers}")
                
                programs.append({
                    'id': program['id'],
                    'training_name': program['training_name'],
                    'start_date': program['start_date'],
                    'end_date': program['end_date'],
                    'trainers': trainers,
                    # Add new fields
                    'pmo_training_category': program.get('pmo_training_category', ''),
                    'pl_category': program.get('pl_category', ''),
                    'brsr_sq_123_category': program.get('brsr_sq_123_category', ''),
                    # Add the missing fields
                    'tni_status': program.get('tni_status', ''),
                    'learning_hours': program.get('learning_hours', '')
                })
        
        if not programs:
            flash('No valid programs found', 'error')
            return redirect(url_for('feedback.feedback_form', program_id=1))
        
        # Store all program IDs in session for later use
        session['clubbed_program_ids'] = program_ids
        
        # Render the clubbed feedback form template
        return render_template(
            'admin/clubbed_feedback_form.html',
            programs=programs,
            program_ids_str=programs_str
        )
    
    except Exception as e:
        current_app.logger.error(f"Error in clubbed feedback form: {e}")
        flash('Error loading programs', 'error')
        return redirect(url_for('feedback.feedback_form', program_id=1))
    finally:
        if conn:
            conn.close()

@feedback_bp.route('/submit_clubbed_feedback', methods=['POST'])
def submit_clubbed_feedback():
    conn = None
    try:
        # Get all program IDs from form
        program_ids_str = request.form.get('program_ids')
        
        # Fallback: Get program IDs from session if form field is empty
        if not program_ids_str:
            program_ids = session.get('clubbed_program_ids', [])
            if program_ids:
                program_ids_str = ','.join(str(pid) for pid in program_ids)
                current_app.logger.info(f"Using program IDs from session: {program_ids_str}")
        
        if not program_ids_str:
            flash('Program IDs are missing', 'error')
            return redirect(url_for('feedback.clubbed_feedback_form', programs=''))
        
        program_ids = [int(pid) for pid in program_ids_str.split(',')]
        
        # Validate required fields
        required_fields = [
            'per_no', 'participants_name', 'senior_name', 'phone'
        ]
        missing_fields = [f for f in required_fields if not request.form.get(f)]
        if missing_fields:
            flash(f'Missing required fields: {", ".join(missing_fields)}', 'error')
            return redirect(url_for('feedback.clubbed_feedback_form', programs=program_ids_str))

        # Get employee details
        employee = get_employee_details(request.form['per_no'])
        if not employee:
            flash('Employee verification failed', 'error')
            return redirect(url_for('feedback.clubbed_feedback_form', programs=program_ids_str))

        # Validate participant name matches EOR
        if request.form['participants_name'].strip().upper() != employee['participants_name'].strip().upper():
            flash('Participant name does not match employee records', 'error')
            return redirect(url_for('feedback.clubbed_feedback_form', programs=program_ids_str))

        # Helper to parse int safely
        def get_int(name):
            val = request.form.get(name)
            return int(val) if val and val.isdigit() else None
        
        # Helper to parse decimal safely
        def get_decimal(name):
            val = request.form.get(name)
            return float(val) if val else None

        # Generate a unique session ID for this clubbed feedback
        clubbed_session_id = str(uuid.uuid4())
        
        conn = get_db_connection()
        
        # Get common feedback data (same for all programs)
        common_data = {
            'per_no': request.form['per_no'].strip(),
            'participants_name': request.form['participants_name'].strip(),
            'bc_no': employee['bc_no'].strip(),
            'gender': employee['gender'].strip(),
            'employee_group': employee['employee_group'].strip(),
            'department': employee['department'].strip(),
            'factory': employee['factory'].strip(),
            'phone': request.form['phone'].strip(),
            'senior_name': request.form['senior_name'].strip(),
            'clubbed_session_id': clubbed_session_id,
            # Common sections (same for all programs)
            'sec1_q1': get_int('sec1_q1'),
            'sec1_q2': get_int('sec1_q2'),
            'sec2_q1': get_int('sec2_q1'),
            'sec2_q2': get_int('sec2_q2'),
            'sec2_q3': get_int('sec2_q3'),
            'sec3_q1': get_int('sec3_q1'),
            'sec5_q1': get_int('sec5_q1'),
            'sec5_q2': get_int('sec5_q2'),
            'sec6_q1': get_int('sec6_q1'),
            'sec6_q2': get_int('sec6_q2'),
            'sec7_q1': get_int('sec7_q1'),
            'sec7_q2': get_int('sec7_q2'),
            # Text fields (not mandatory)
            'sec7_q3_text': request.form.get('sec7_q3_text', '').strip() or None,
            'sec7_q4_text': request.form.get('sec7_q4_text', '').strip() or None,
            # Common suggestions field
            'suggestions': request.form.get('suggestions', '').strip() or None,
        }
        
        # Insert feedback for each program
        for program_id in program_ids:
            # Prepare data for this program
            data = {
                'program_id': program_id,
                'program_title': request.form.get(f'program_{program_id}_title', ''),
                'program_date': request.form.get(f'program_{program_id}_date', ''),
                # New fields for this program
                'pmo_training_category': request.form.get(f'program_{program_id}_pmo_training_category', '').strip() or None,
                'pl_category': request.form.get(f'program_{program_id}_pl_category', '').strip() or None,
                'brsr_sq_123_category': request.form.get(f'program_{program_id}_brsr_sq_123_category', '').strip() or None,
                # Added fields for this program
                'tni_status': request.form.get(f'program_{program_id}_tni_status', '').strip() or None,
                'learning_hours': get_decimal(f'program_{program_id}_learning_hours'),
                **common_data  # Include all common data
            }

            # Default all trainer fields to None
            for i in range(1, 5):
                data.update({
                    f'trainer{i}_name': None,
                    f'trainer{i}_q1': None,
                    f'trainer{i}_q2': None,
                    f'trainer{i}_q3': None,
                    f'trainer{i}_q4': None
                })

            # Fill trainer data if provided (for this program)
            for i in range(1, 5):
                tname = request.form.get(f'program_{program_id}_trainer{i}_name', '').strip()
                if tname:
                    data[f'trainer{i}_name'] = tname
                    data[f'trainer{i}_q1'] = get_int(f'program_{program_id}_trainer{i}_q1')
                    data[f'trainer{i}_q2'] = get_int(f'program_{program_id}_trainer{i}_q2')
                    data[f'trainer{i}_q3'] = get_int(f'program_{program_id}_trainer{i}_q3')
                    data[f'trainer{i}_q4'] = get_int(f'program_{program_id}_trainer{i}_q4')

            with conn.cursor() as cursor:
                columns = [
                    'program_id', 'program_title', 'program_date', 'per_no', 'participants_name',
                    'bc_no', 'gender', 'employee_group', 'department', 'factory', 'phone', 'senior_name',
                    'sec1_q1', 'sec1_q2', 'sec2_q1', 'sec2_q2', 'sec2_q3', 'sec3_q1',
                    'sec5_q1', 'sec5_q2', 'sec6_q1', 'sec6_q2', 'sec7_q1', 'sec7_q2',
                    'sec7_q3_text', 'sec7_q4_text', 'suggestions', 'clubbed_session_id',
                    'pmo_training_category', 'pl_category', 'brsr_sq_123_category',  # New fields
                    'tni_status', 'learning_hours',  # Added fields
                    'trainer1_name', 'trainer1_q1', 'trainer1_q2', 'trainer1_q3', 'trainer1_q4',
                    'trainer2_name', 'trainer2_q1', 'trainer2_q2', 'trainer2_q3', 'trainer2_q4',
                    'trainer3_name', 'trainer3_q1', 'trainer3_q2', 'trainer3_q3', 'trainer3_q4',
                    'trainer4_name', 'trainer4_q1', 'trainer4_q2', 'trainer4_q3', 'trainer4_q4'
                ]
                values = [data.get(col) for col in columns]

                query = f"""
                INSERT INTO feedback_responses ({", ".join(columns)})
                VALUES ({", ".join(["%s"] * len(columns))})
                """

                cursor.execute(query, values)
                conn.commit()

        flash('Feedback submitted successfully for all programs!', 'success')
        return redirect(url_for('feedback.success'))

    except pymysql.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"Database error: {e}")
        flash('Database error occurred. Please try again.', 'error')
    except Exception as e:
        current_app.logger.error(f"Error submitting feedback: {e}")
        current_app.logger.error(f"Form data: {request.form}")
        flash(f'Error submitting feedback: {e}', 'error')
    finally:
        if conn: conn.close()

    return redirect(url_for('feedback.clubbed_feedback_form', programs=program_ids_str))