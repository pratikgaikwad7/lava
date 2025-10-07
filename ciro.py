from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file
from datetime import datetime
from io import BytesIO
import pandas as pd
import numpy as np
from utils import get_db_connection
import pymysql.cursors

# Create the blueprint with explicit name and url_prefix
ciro_bp = Blueprint('ciro', __name__, 
                   template_folder='templates/admin',
                   url_prefix='/ciro')  # Add url_prefix to avoid conflicts

# Context processor to make 'now' available in all templates
@ciro_bp.context_processor
def inject_now():
    return {'now': datetime.now()}

def test_db_connection():
    try:
        conn = get_db_connection()
        # Disable ONLY_FULL_GROUP_BY mode
        with conn.cursor() as cursor:
            cursor.execute("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))")
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"Database connection successful: {result}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        return False

# Modified root route to redirect to dashboard
@ciro_bp.route('/')
def root():
    return redirect(url_for('ciro.dashboard'))

# Form submission route (now accessible at /form)
@ciro_bp.route('/form')
def form():
    return render_template('admin/ciro_form.html')

@ciro_bp.route('/success')
def success():
    return render_template('admin/ciro_success.html')

# Dashboard routes
@ciro_bp.route('/dashboard')
def dashboard():
    try:
        if not test_db_connection():
            flash("Database connection failed", "danger")
            return render_template('admin/ciro_dashboard.html', sessions=[], years=[], overall_avg_score=None, trainers=[], programs=[])

        conn = get_db_connection()
        # Disable ONLY_FULL_GROUP_BY mode
        with conn.cursor() as cursor:
            cursor.execute("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))")
        
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Get filter parameters
        month = request.args.get('month')
        year = request.args.get('year')
        trainer = request.args.get('trainer')
        search = request.args.get('search')
        feedback_type = request.args.get('feedback_type')
        
        # Get all unique training programs for dropdown
        cursor.execute("""
            SELECT DISTINCT program_title 
            FROM feedback_responses 
            ORDER BY program_title
        """)
        programs = [row['program_title'] for row in cursor.fetchall()]
        
        # Get all unique trainers for dropdown
        cursor.execute("""
            SELECT DISTINCT trainer_name 
            FROM (
                SELECT trainer1_name as trainer_name FROM feedback_responses WHERE trainer1_name IS NOT NULL AND trainer1_name != ''
                UNION
                SELECT trainer2_name as trainer_name FROM feedback_responses WHERE trainer2_name IS NOT NULL AND trainer2_name != ''
                UNION
                SELECT trainer3_name as trainer_name FROM feedback_responses WHERE trainer3_name IS NOT NULL AND trainer3_name != ''
                UNION
                SELECT trainer4_name as trainer_name FROM feedback_responses WHERE trainer4_name IS NOT NULL AND trainer4_name != ''
            ) as all_trainers
            ORDER BY trainer_name
        """)
        trainers = [row['trainer_name'] for row in cursor.fetchall()]
        
        # Base query for training sessions with grouping
        query = """
        SELECT 
            fr.program_title,
            fr.program_date,
            COUNT(DISTINCT fr.id) as response_count,
            fr.pmo_training_category,
            fr.pl_category,
            fr.brsr_sq_123_category,
            (
                SELECT GROUP_CONCAT(DISTINCT t.name SEPARATOR ', ')
                FROM (
                    SELECT trainer1_name as name FROM feedback_responses 
                    WHERE program_title = fr.program_title AND program_date = fr.program_date AND trainer1_name != ''
                    UNION
                    SELECT trainer2_name as name FROM feedback_responses 
                    WHERE program_title = fr.program_title AND program_date = fr.program_date AND trainer2_name != ''
                    UNION
                    SELECT trainer3_name as name FROM feedback_responses 
                    WHERE program_title = fr.program_title AND program_date = fr.program_date AND trainer3_name != ''
                    UNION
                    SELECT trainer4_name as name FROM feedback_responses 
                    WHERE program_title = fr.program_title AND program_date = fr.program_date AND trainer4_name != ''
                ) as t
                WHERE t.name IS NOT NULL AND t.name != ''
            ) as trainer_names,
            AVG((sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 + 
                sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2)/12.0) as csi,
            (
                SELECT AVG(avg_score)
                FROM (
                    SELECT (AVG(trainer1_q1) + AVG(trainer1_q2) + AVG(trainer1_q3) + AVG(trainer1_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer1_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer2_q1) + AVG(trainer2_q2) + AVG(trainer2_q3) + AVG(trainer2_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer2_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer3_q1) + AVG(trainer3_q2) + AVG(trainer3_q3) + AVG(trainer3_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer3_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer4_q1) + AVG(trainer4_q2) + AVG(trainer4_q3) + AVG(trainer4_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer4_name IS NOT NULL
                ) as trainer_avgs
            ) as tfi,
            AVG(
                (sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 +
                 COALESCE(trainer1_q1,0) + COALESCE(trainer1_q2,0) + COALESCE(trainer1_q3,0) + COALESCE(trainer1_q4,0) +
                 COALESCE(trainer2_q1,0) + COALESCE(trainer2_q2,0) + COALESCE(trainer2_q3,0) + COALESCE(trainer2_q4,0) +
                 COALESCE(trainer3_q1,0) + COALESCE(trainer3_q2,0) + COALESCE(trainer3_q3,0) + COALESCE(trainer3_q4,0) +
                 COALESCE(trainer4_q1,0) + COALESCE(trainer4_q2,0) + COALESCE(trainer4_q3,0) + COALESCE(trainer4_q4,0) +
                 sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2) /
                (12 + 
                 CASE WHEN trainer1_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer2_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer3_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer4_q1 IS NOT NULL THEN 4 ELSE 0 END)
            ) as avg_score
        FROM feedback_responses fr
        WHERE 1=1
        """
        params = []
        
        # Apply filters
        if month:
            query += " AND MONTH(fr.program_date) = %s"
            params.append(month)
        if year:
            query += " AND YEAR(fr.program_date) = %s"
            params.append(year)
        if trainer:
            query += " AND (fr.trainer1_name LIKE %s OR fr.trainer2_name LIKE %s OR fr.trainer3_name LIKE %s OR fr.trainer4_name LIKE %s)"
            params.extend([f"%{trainer}%"] * 4)
        if search:
            query += " AND (fr.program_title LIKE %s OR fr.participants_name LIKE %s)"
            params.extend([f"%{search}%"] * 2)
        if feedback_type:
            if feedback_type == 'individual':
                query += " AND fr.clubbed_session_id IS NULL"
            elif feedback_type == 'clubbed':
                query += " AND fr.clubbed_session_id IS NOT NULL"
        
        # Group by program and date only
        query += " GROUP BY fr.program_title, fr.program_date ORDER BY fr.program_date DESC"
        
        cursor.execute(query, params)
        sessions = cursor.fetchall()
        
        # Calculate overall average score
        overall_avg_score = None
        if sessions:
            avg_scores = [s['avg_score'] for s in sessions if s['avg_score'] is not None]
            if avg_scores:
                overall_avg_score = sum(avg_scores) / len(avg_scores)
        
        # Get unique years for filter dropdown
        cursor.execute("SELECT DISTINCT YEAR(program_date) as year FROM feedback_responses ORDER BY year DESC")
        years = [str(row['year']) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return render_template('admin/ciro_dashboard.html', sessions=sessions, years=years,
                              selected_month=month, selected_year=year,
                              selected_trainer=trainer, search_query=search,
                              selected_feedback_type=feedback_type,
                              overall_avg_score=overall_avg_score,
                              programs=programs, trainers=trainers)
    
    except Exception as e:
        print(f"Error in dashboard: {str(e)}")
        flash(f"An error occurred: {str(e)}", "danger")
        return render_template('admin/ciro_dashboard.html', sessions=[], years=[], overall_avg_score=None, programs=[], trainers=[])

@ciro_bp.route('/training/<program_title>/<program_date>')
def training_detail(program_title, program_date):
    conn = get_db_connection()
    # Disable ONLY_FULL_GROUP_BY mode
    with conn.cursor() as cursor:
        cursor.execute("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))")
    
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    
    # Get all training programs for the dropdown
    cursor.execute("""
        SELECT DISTINCT program_title, program_date
        FROM feedback_responses
        ORDER BY program_date DESC
    """)
    all_programs = cursor.fetchall()
    
    # Get session summary
    cursor.execute("""
    SELECT 
        program_title,
        program_date,
        pmo_training_category,
        pl_category,
        brsr_sq_123_category,
        COUNT(DISTINCT id) as response_count,
        AVG(sec1_q1) as sec1_q1_avg,
        AVG(sec1_q2) as sec1_q2_avg,
        AVG(sec2_q1) as sec2_q1_avg,
        AVG(sec2_q2) as sec2_q2_avg,
        AVG(sec2_q3) as sec2_q3_avg,
        AVG(sec3_q1) as sec3_q1_avg,
        AVG(sec5_q1) as sec5_q1_avg,
        AVG(sec5_q2) as sec5_q2_avg,
        AVG(sec6_q1) as sec6_q1_avg,
        AVG(sec6_q2) as sec6_q2_avg,
        AVG(sec7_q1) as sec7_q1_avg,
        AVG(sec7_q2) as sec7_q2_avg,
        AVG((sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 + sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2)/12.0) as csi,
        (
            SELECT AVG(avg_score)
            FROM (
                SELECT (AVG(trainer1_q1) + AVG(trainer1_q2) + AVG(trainer1_q3) + AVG(trainer1_q4))/4 as avg_score
                FROM feedback_responses 
                WHERE program_title = %s 
                AND program_date = %s
                AND trainer1_name IS NOT NULL
                UNION ALL
                SELECT (AVG(trainer2_q1) + AVG(trainer2_q2) + AVG(trainer2_q3) + AVG(trainer2_q4))/4 as avg_score
                FROM feedback_responses 
                WHERE program_title = %s 
                AND program_date = %s
                AND trainer2_name IS NOT NULL
                UNION ALL
                SELECT (AVG(trainer3_q1) + AVG(trainer3_q2) + AVG(trainer3_q3) + AVG(trainer3_q4))/4 as avg_score
                FROM feedback_responses 
                WHERE program_title = %s 
                AND program_date = %s
                AND trainer3_name IS NOT NULL
                UNION ALL
                SELECT (AVG(trainer4_q1) + AVG(trainer4_q2) + AVG(trainer4_q3) + AVG(trainer4_q4))/4 as avg_score
                FROM feedback_responses 
                WHERE program_title = %s 
                AND program_date = %s
                AND trainer4_name IS NOT NULL
            ) as trainer_avgs
        ) as tfi,
        AVG(
            (sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 +
             COALESCE(trainer1_q1,0) + COALESCE(trainer1_q2,0) + COALESCE(trainer1_q3,0) + COALESCE(trainer1_q4,0) +
             COALESCE(trainer2_q1,0) + COALESCE(trainer2_q2,0) + COALESCE(trainer2_q3,0) + COALESCE(trainer2_q4,0) +
             COALESCE(trainer3_q1,0) + COALESCE(trainer3_q2,0) + COALESCE(trainer3_q3,0) + COALESCE(trainer3_q4,0) +
             COALESCE(trainer4_q1,0) + COALESCE(trainer4_q2,0) + COALESCE(trainer4_q3,0) + COALESCE(trainer4_q4,0) +
             sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2) /
            (12 + 
             CASE WHEN trainer1_q1 IS NOT NULL THEN 4 ELSE 0 END +
             CASE WHEN trainer2_q1 IS NOT NULL THEN 4 ELSE 0 END +
             CASE WHEN trainer3_q1 IS NOT NULL THEN 4 ELSE 0 END +
             CASE WHEN trainer4_q1 IS NOT NULL THEN 4 ELSE 0 END)
        ) as avg_score
    FROM feedback_responses fr
    WHERE program_title = %s AND program_date = %s
    GROUP BY program_title, program_date, pmo_training_category, pl_category, brsr_sq_123_category
    """, (program_title, program_date, program_title, program_date, program_title, program_date, program_title, program_date, program_title, program_date))
    
    session = cursor.fetchone()
    
    # Get trainer feedback
    cursor.execute("""
        SELECT 
            trainer_name,
            AVG(q1) as q1_avg,
            AVG(q2) as q2_avg,
            AVG(q3) as q3_avg,
            AVG(q4) as q4_avg
        FROM (
            SELECT trainer1_name as trainer_name, trainer1_q1 as q1, trainer1_q2 as q2, trainer1_q3 as q3, trainer1_q4 as q4
            FROM feedback_responses
            WHERE program_title = %s AND program_date = %s AND trainer1_name IS NOT NULL
            
            UNION ALL
            
            SELECT trainer2_name as trainer_name, trainer2_q1 as q1, trainer2_q2 as q2, trainer2_q3 as q3, trainer2_q4 as q4
            FROM feedback_responses
            WHERE program_title = %s AND program_date = %s AND trainer2_name IS NOT NULL
            
            UNION ALL
            
            SELECT trainer3_name as trainer_name, trainer3_q1 as q1, trainer3_q2 as q2, trainer3_q3 as q3, trainer3_q4 as q4
            FROM feedback_responses
            WHERE program_title = %s AND program_date = %s AND trainer3_name IS NOT NULL
            
            UNION ALL
            
            SELECT trainer4_name as trainer_name, trainer4_q1 as q1, trainer4_q2 as q2, trainer4_q3 as q3, trainer4_q4 as q4
            FROM feedback_responses
            WHERE program_title = %s AND program_date = %s AND trainer4_name IS NOT NULL
        ) as all_trainers
        GROUP BY trainer_name
    """, (program_title, program_date, program_title, program_date, program_title, program_date, program_title, program_date))
    
    trainers = cursor.fetchall()
    
    # Get all individual responses with all questions
    cursor.execute("""
        SELECT *,
            (sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 + 
             sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2)/12.0 as csi_score,
            (
                COALESCE(trainer1_q1,0) + COALESCE(trainer1_q2,0) + COALESCE(trainer1_q3,0) + COALESCE(trainer1_q4,0) +
                COALESCE(trainer2_q1,0) + COALESCE(trainer2_q2,0) + COALESCE(trainer2_q3,0) + COALESCE(trainer2_q4,0) +
                COALESCE(trainer3_q1,0) + COALESCE(trainer3_q2,0) + COALESCE(trainer3_q3,0) + COALESCE(trainer3_q4,0) +
                COALESCE(trainer4_q1,0) + COALESCE(trainer4_q2,0) + COALESCE(trainer4_q3,0) + COALESCE(trainer4_q4,0)
            ) / NULLIF(
                (CASE WHEN trainer1_q1 IS NOT NULL THEN 4 ELSE 0 END +
                CASE WHEN trainer2_q1 IS NOT NULL THEN 4 ELSE 0 END +
                CASE WHEN trainer3_q1 IS NOT NULL THEN 4 ELSE 0 END +
                CASE WHEN trainer4_q1 IS NOT NULL THEN 4 ELSE 0 END), 0) as tfi_score
    FROM feedback_responses
    WHERE program_title = %s AND program_date = %s
    ORDER BY created_at ASC  -- Changed from DESC to ASC to show data in entry order
""", (program_title, program_date))
    
    individual_responses = cursor.fetchall()
    
    # Get text feedback
    cursor.execute("""
        SELECT 
            participants_name,
            sec7_q3_text,
            sec7_q4_text,
            suggestions
        FROM feedback_responses
        WHERE program_title = %s AND program_date = %s
        AND (sec7_q3_text != '' OR sec7_q4_text != '' OR suggestions != '')
        ORDER BY created_at ASC  -- Changed from DESC to ASC to show data in entry order
    """, (program_title, program_date))
    
    text_feedback = cursor.fetchall()
    
    # Get clubbed session info if applicable
    clubbed_info = None
    cursor.execute("""
        SELECT DISTINCT clubbed_session_id
        FROM feedback_responses
        WHERE program_title = %s AND program_date = %s AND clubbed_session_id IS NOT NULL
    """, (program_title, program_date))
    clubbed_session_ids = cursor.fetchall()
    
    if clubbed_session_ids:
        clubbed_info = []
        for session_id in clubbed_session_ids:
            cursor.execute("""
                SELECT 
                    program_title,
                    program_date,
                    COUNT(*) as program_count
                FROM feedback_responses
                WHERE clubbed_session_id = %s
                GROUP BY clubbed_session_id
            """, (session_id['clubbed_session_id'],))
            clubbed_info.extend(cursor.fetchall())
    
    cursor.close()
    conn.close()
    
    return render_template('admin/ciro_training_detail.html', 
                          session=session, 
                          trainers=trainers, 
                          individual_responses=individual_responses, 
                          text_feedback=text_feedback,
                          program_title=program_title, 
                          program_date=program_date,
                          clubbed_info=clubbed_info,
                          all_programs=all_programs)

def safe_max_len(series, col_name):
    """Safely calculate the maximum length of values in a series for column width adjustment"""
    if series.empty:
        return len(col_name)
    
    # Convert to string and handle NaN values
    str_series = series.astype(str)
    # Replace 'nan' strings with empty strings to avoid counting them
    str_series = str_series.replace('nan', '')
    
    # Get the length of each string
    len_series = str_series.apply(len)
    
    # If all values are NaN or empty, return the column name length
    if len_series.empty or len_series.max() == 0:
        return len(col_name)
    
    return max(len_series.max(), len(col_name))

@ciro_bp.route('/export/summary')
def export_summary():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        # Disable ONLY_FULL_GROUP_BY mode
        with conn.cursor() as cursor:
            cursor.execute("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))")
        
        conn.begin()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Get filter parameters
        month = request.args.get('month')
        year = request.args.get('year')
        trainer = request.args.get('trainer')
        search = request.args.get('search')
        feedback_type = request.args.get('feedback_type')
        
        # Base query (same as dashboard)
        query = """
        SELECT 
            fr.program_title,
            fr.program_date,
            fr.pmo_training_category,
            fr.pl_category,
            fr.brsr_sq_123_category,
            COUNT(DISTINCT fr.id) as response_count,
            AVG((sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 + 
                 sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2)/12.0) as csi,
            (
                SELECT AVG(avg_score)
                FROM (
                    SELECT (AVG(trainer1_q1) + AVG(trainer1_q2) + AVG(trainer1_q3) + AVG(trainer1_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer1_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer2_q1) + AVG(trainer2_q2) + AVG(trainer2_q3) + AVG(trainer2_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer2_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer3_q1) + AVG(trainer3_q2) + AVG(trainer3_q3) + AVG(trainer3_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer3_name IS NOT NULL
                    UNION ALL
                    SELECT (AVG(trainer4_q1) + AVG(trainer4_q2) + AVG(trainer4_q3) + AVG(trainer4_q4))/4 as avg_score
                    FROM feedback_responses 
                    WHERE program_title = fr.program_title 
                    AND program_date = fr.program_date
                    AND trainer4_name IS NOT NULL
                ) as trainer_avgs
            ) as tfi,
            AVG(
                (sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 +
                 COALESCE(trainer1_q1,0) + COALESCE(trainer1_q2,0) + COALESCE(trainer1_q3,0) + COALESCE(trainer1_q4,0) +
                 COALESCE(trainer2_q1,0) + COALESCE(trainer2_q2,0) + COALESCE(trainer2_q3,0) + COALESCE(trainer2_q4,0) +
                 COALESCE(trainer3_q1,0) + COALESCE(trainer3_q2,0) + COALESCE(trainer3_q3,0) + COALESCE(trainer3_q4,0) +
                 COALESCE(trainer4_q1,0) + COALESCE(trainer4_q2,0) + COALESCE(trainer4_q3,0) + COALESCE(trainer4_q4,0) +
                 sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2) /
                (12 + 
                 CASE WHEN trainer1_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer2_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer3_q1 IS NOT NULL THEN 4 ELSE 0 END +
                 CASE WHEN trainer4_q1 IS NOT NULL THEN 4 ELSE 0 END)
            ) as avg_score
        FROM feedback_responses fr
        WHERE 1=1
        """
        
        params = []
        
        # Apply filters
        if month:
            query += " AND MONTH(fr.program_date) = %s"
            params.append(month)
        if year:
            query += " AND YEAR(fr.program_date) = %s"
            params.append(year)
        if trainer:
            query += " AND (fr.trainer1_name LIKE %s OR fr.trainer2_name LIKE %s OR fr.trainer3_name LIKE %s OR fr.trainer4_name LIKE %s)"
            params.extend([f"%{trainer}%"] * 4)
        if search:
            query += " AND (fr.program_title LIKE %s OR fr.participants_name LIKE %s)"
            params.extend([f"%{search}%"] * 2)
        if feedback_type:
            if feedback_type == 'individual':
                query += " AND fr.clubbed_session_id IS NULL"
            elif feedback_type == 'clubbed':
                query += " AND fr.clubbed_session_id IS NOT NULL"
        
        # Group by program and date only
        query += " GROUP BY fr.program_title, fr.program_date, fr.pmo_training_category, fr.pl_category, fr.brsr_sq_123_category ORDER BY fr.program_date DESC"
        
        # Execute query and fetch data
        cursor.execute(query, params)
        data = cursor.fetchall()
        conn.commit()
        
        # Create DataFrame from the fetched data
        df = pd.DataFrame(data)
        
        if df.empty:
            flash("No data found for the selected filters", "warning")
            return redirect(url_for('ciro.dashboard'))
        
        # Rename columns
        df = df.rename(columns={
            'program_title': 'Training Name',
            'program_date': 'Program Date',
            'pmo_training_category': 'PMO Training Category',
            'pl_category': 'PL Category',
            'brsr_sq_123_category': 'BRSR SQ 123 Category',
            'response_count': 'Response Count',
            'csi': 'CSI',
            'tfi': 'TFI',
            'avg_score': 'Average Score'
        })
        
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Summary']
            
            # Add header format
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#003366',
                'font_color': 'white',
                'border': 1
            })
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust columns' width using safe function
            for i, col in enumerate(df.columns):
                max_len = safe_max_len(df[col], col)
                worksheet.set_column(i, i, max_len + 2)
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='CIRO_Feedback_Summary.xlsx'
        )
    
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error in export_summary: {str(e)}")
        flash(f"An error occurred during export: {str(e)}", "danger")
        return redirect(url_for('ciro.dashboard'))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@ciro_bp.route('/export/detail/<program_title>/<program_date>')
def export_detail(program_title, program_date):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        # Disable ONLY_FULL_GROUP_BY mode
        with conn.cursor() as cursor:
            cursor.execute("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))")
        
        conn.begin()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Get all responses for this session, including all columns
        query = """
            SELECT 
                id, program_date, program_title, pmo_training_category, pl_category, 
                brsr_sq_123_category, tni_status, learning_hours,
                per_no, participants_name, bc_no, gender, employee_group, 
                department, factory, senior_name,
                sec1_q1, sec1_q2, sec2_q1, sec2_q2, sec2_q3, sec3_q1,
                trainer1_name, trainer1_q1, trainer1_q2, trainer1_q3, trainer1_q4,
                trainer2_name, trainer2_q1, trainer2_q2, trainer2_q3, trainer2_q4,
                trainer3_name, trainer3_q1, trainer3_q2, trainer3_q3, trainer3_q4,
                trainer4_name, trainer4_q1, trainer4_q2, trainer4_q3, trainer4_q4,
                sec5_q1, sec5_q2, sec6_q1, sec6_q2, sec7_q1, sec7_q2, 
                sec7_q3_text, sec7_q4_text, suggestions
            FROM feedback_responses 
            WHERE program_title = %s AND program_date = %s
            ORDER BY created_at ASC  -- Changed from DESC to ASC to show data in entry order
        """
        
        cursor.execute(query, (program_title, program_date))
        data = cursor.fetchall()
        conn.commit()
        
        # Create DataFrame from the fetched data
        df = pd.DataFrame(data)
        
        if df.empty:
            flash("No data found for this training session", "warning")
            return redirect(url_for('ciro.dashboard'))
        
        # Add Sr. no column
        df.insert(0, 'Sr. no', range(1, len(df) + 1))
        
        # Define the complete column mapping
        column_mapping = {
            'Sr. no': 'sr no',
            'program_date': 'program date',
            'program_title': 'Training Name',
            'pmo_training_category': 'PMO Training Category',
            'pl_category': 'PL Category',
            'brsr_sq_123_category': 'BRSR SQ 1,2,3 Category',
            'tni_status': 'TNI / NON TNI',
            'learning_hours': 'Learning hours',
            'participants_name': 'Participants Name',
            'bc_no': 'BC No',
            'gender': 'Gender',
            'employee_group': 'Employee group',
            'department': 'Department',
            'factory': 'Factory',
            'sec1_q1': '1.1 प्रशिक्षणार्थ्याची पूर्वतयारी - मला या विषयाची जुजबी माहिती कार्यक्रमाला येण्याअगोदर होती',
            'sec1_q2': '1.2 प्रशिक्षणार्थ्याची पूर्वतयारी - मला या कार्यक्रमाचे उद्दिष्ट समजले आहे',
            'sec2_q1': '2.1 कार्यक्रमात समाविष्ट करण्यात आलेली माहिती - माझ्या कामाशी संबंधित होती',
            'sec2_q2': '2.2 कार्यक्रमात समाविष्ट करण्यात आलेली माहिती - मूळ विषय समजण्याच्या दृष्टीने पुरेशी आहे',
            'sec2_q3': '2.3 कार्यक्रमात समाविष्ट करण्यात आलेली माहिती - माझ्या कामात सुधारणा करण्यास उपयुक्त आहे',
            'sec3_q1': '3 कामामध्ये वापर - मी मिळालेल्या ज्ञानाचा कामात उपयोग करू शकेन',
            'trainer1_name': 'Trainer 1 Name',
            'trainer1_q1': '4.1 व्याख्यात्याला विषयाचे सखोल ज्ञान आहे',
            'trainer1_q2': '4.2 व्याख्यात्याकडे समजावून सांगण्याचे कौशल्य आहे',
            'trainer1_q3': '4.3 व्याख्याता शंका समाधान उत्तमरित्या करू शकतो',
            'trainer1_q4': '4.4 आपले व्याख्यात्या बद्दल सर्वसाधारण मत (पेहराव, सहभाग, सादरीकरण इ.) चांगले आहे',
            'trainer2_name': 'Trainer 2 Name',
            'trainer2_q1': '4.1 व्याख्यात्याला विषयाचे सखोल ज्ञान आहे',
            'trainer2_q2': '4.2 व्याख्यात्याकडे समजावून सांगण्याचे कौशल्य आहे',
            'trainer2_q3': '4.3 व्याख्याता शंका समाधान उत्तमरित्या करू शकतो',
            'trainer2_q4': '4.4 आपले व्याख्यात्या बद्दल सर्वसाधारण मत (पेहराव, सहभाग, सादरीकरण इ.) चांगले आहे',
            'trainer3_name': 'Trainer 3 Name',
            'trainer3_q1': '4.1 व्याख्यात्याला विषयाचे सखोल ज्ञान आहे',
            'trainer3_q2': '4.2 व्याख्यात्याकडे समजावून सांगण्याचे कौशल्य आहे',
            'trainer3_q3': '4.3 व्याख्याता शंका समाधान उत्तमरित्या करू शकतो',
            'trainer3_q4': '4.4 आपले व्याख्यात्या बद्दल सर्वसाधारण मत (पेहराव, सहभाग, सादरीकरण इ.) चांगले आहे',
            'trainer4_name': 'Trainer 4 Name',
            'trainer4_q1': '4.1 व्याख्यात्याला विषयाचे सखोल ज्ञान आहे',
            'trainer4_q2': '4.2 व्याख्यात्याकडे समजावून सांगण्याचे कौशल्य आहे',
            'trainer4_q3': '4.3 व्याख्याता शंका समाधान उत्तमरित्या करू शकतो',
            'trainer4_q4': '4.4 आपले व्याख्यात्या बद्दल सर्वसाधारण मत (पेहराव, सहभाग, सादरीकरण इ.) चांगले आहे',
            'sec5_q1': '5.1 कार्यक्रमाचे संयोजन व इतर व्यवस्था - कार्यक्रमाचे संयोजन व इतर व्यवस्था समाधानकारक होती',
            'sec5_q2': '5.2 कार्यक्रमाचे संयोजन व इतर व्यवस्था - इतर पूरक व्यवस्था (चहा, नाष्टा इ.) समाधानकारक होती',
            'sec6_q1': '6.1 कार्यक्रमा बाबत - कार्यक्रमाची गती',
            'sec6_q2': '6.2 कार्यक्रमा बाबत - कार्यक्रमाचा कालावधी',
            'sec7_q1': '7.1 कार्यक्रमाचे सर्वसाधारण मूल्यमापन - उपयोगाच्या दृष्टीकोनातून या कार्यक्रमाचे मूल्यमापन करा',
            'sec7_q2': '7.2 कार्यक्रमाचे सर्वसाधारण मूल्यमापन - तुमच्या इतर सहकाऱ्यांनी सुध्दा या कार्यक्रमात यावे असे वाटते का',
            'sec7_q3_text': '7.3 कार्यक्रमाचे सर्वसाधारण मूल्यमापन - यातील कोणता विषय तुम्हाला सर्वात जास्त संबंधीत वाटला',
            'sec7_q4_text': '7.4 कार्यक्रमाचे सर्वसाधारण मूल्यमापन - या कार्यक्रमामध्ये एखाद्या विषयाचा समावेश झाला नाही असे आपणांस वाटते का? असल्यास तो विषय कोणता',
            'suggestions': '7.5 सुधारणेबाबत सूचना - या कार्यक्रमाच्या सुधारणेबाबत काही सूचना असल्यास खाली लिहा',
            'senior_name': 'senor name'
        }
        
        # Select only the columns we need in the desired order
        desired_columns = [
            'Sr. no', 'program_date', 'program_title', 'pmo_training_category', 
            'pl_category', 'brsr_sq_123_category', 'tni_status', 'learning_hours',
            'participants_name', 'bc_no', 'gender', 'employee_group', 
            'department', 'factory', 'sec1_q1', 'sec1_q2', 'sec2_q1', 
            'sec2_q2', 'sec2_q3', 'sec3_q1', 'trainer1_name', 'trainer1_q1', 
            'trainer1_q2', 'trainer1_q3', 'trainer1_q4', 'trainer2_name', 
            'trainer2_q1', 'trainer2_q2', 'trainer2_q3', 'trainer2_q4', 
            'trainer3_name', 'trainer3_q1', 'trainer3_q2', 'trainer3_q3', 
            'trainer3_q4', 'trainer4_name', 'trainer4_q1', 'trainer4_q2', 
            'trainer4_q3', 'trainer4_q4', 'sec5_q1', 'sec5_q2', 'sec6_q1', 
            'sec6_q2', 'sec7_q1', 'sec7_q2', 'sec7_q3_text', 'sec7_q4_text', 
            'suggestions', 'senior_name'
        ]
        
        # Filter the DataFrame to include only the desired columns
        df_filtered = df[desired_columns]
        
        # Rename columns using the mapping
        df_renamed = df_filtered.rename(columns=column_mapping)
        
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_renamed.to_excel(writer, sheet_name='Responses', index=False)
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Responses']
            
            # Add header format
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#003366',
                'font_color': 'white',
                'border': 1
            })
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(df_renamed.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust columns' width using safe function
            for i, col in enumerate(df_renamed.columns):
                max_len = safe_max_len(df_renamed[col], col)
                worksheet.set_column(i, i, max_len + 2)
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'CIRO_Feedback_{program_title}_{program_date}.xlsx'
        )
    
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error in export_detail: {str(e)}")
        flash(f"An error occurred during export: {str(e)}", "danger")
        return redirect(url_for('ciro.dashboard'))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@ciro_bp.route('/export/individual/<int:response_id>')
def export_individual(response_id):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        # Disable ONLY_FULL_GROUP_BY mode
        with conn.cursor() as cursor:
            cursor.execute("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))")
        
        conn.begin()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Get the specific response, including all columns
        query = """
            SELECT 
                id, program_date, program_title, pmo_training_category, pl_category, 
                brsr_sq_123_category, tni_status, learning_hours,
                per_no, participants_name, bc_no, gender, employee_group, 
                department, factory, senior_name,
                sec1_q1, sec1_q2, sec2_q1, sec2_q2, sec2_q3, sec3_q1,
                trainer1_name, trainer1_q1, trainer1_q2, trainer1_q3, trainer1_q4,
                trainer2_name, trainer2_q1, trainer2_q2, trainer2_q3, trainer2_q4,
                trainer3_name, trainer3_q1, trainer3_q2, trainer3_q3, trainer3_q4,
                trainer4_name, trainer4_q1, trainer4_q2, trainer4_q3, trainer4_q4,
                sec5_q1, sec5_q2, sec6_q1, sec6_q2, sec7_q1, sec7_q2, 
                sec7_q3_text, sec7_q4_text, suggestions
            FROM feedback_responses 
            WHERE id = %s
        """
        
        cursor.execute(query, (response_id,))
        data = cursor.fetchall()
        conn.commit()
        
        # Create DataFrame from the fetched data
        df = pd.DataFrame(data)
        
        if df.empty:
            flash("No data found for this response ID", "warning")
            return redirect(url_for('ciro.dashboard'))
        
        # Add Sr. no column
        df.insert(0, 'Sr. no', 1)
        
        # Define the complete column mapping
        column_mapping = {
            'Sr. no': 'sr no',
            'program_date': 'program date',
            'program_title': 'Training Name',
            'pmo_training_category': 'PMO Training Category',
            'pl_category': 'PL Category',
            'brsr_sq_123_category': 'BRSR SQ 1,2,3 Category',
            'tni_status': 'TNI / NON TNI',
            'learning_hours': 'Learning hours',
            'participants_name': 'Participants Name',
            'bc_no': 'BC No',
            'gender': 'Gender',
            'employee_group': 'Employee group',
            'department': 'Department',
            'factory': 'Factory',
            'sec1_q1': '1.1 प्रशिक्षणार्थ्याची पूर्वतयारी - मला या विषयाची जुजबी माहिती कार्यक्रमाला येण्याअगोदर होती',
            'sec1_q2': '1.2 प्रशिक्षणार्थ्याची पूर्वतयारी - मला या कार्यक्रमाचे उद्दिष्ट समजले आहे',
            'sec2_q1': '2.1 कार्यक्रमात समाविष्ट करण्यात आलेली माहिती - माझ्या कामाशी संबंधित होती',
            'sec2_q2': '2.2 कार्यक्रमात समाविष्ट करण्यात आलेली माहिती - मूळ विषय समजण्याच्या दृष्टीने पुरेशी आहे',
            'sec2_q3': '2.3 कार्यक्रमात समाविष्ट करण्यात आलेली माहिती - माझ्या कामात सुधारणा करण्यास उपयुक्त आहे',
            'sec3_q1': '3 कामामध्ये वापर - मी मिळालेल्या ज्ञानाचा कामात उपयोग करू शकेन',
            'trainer1_name': 'Trainer 1 Name',
            'trainer1_q1': '4.1 व्याख्यात्याला विषयाचे सखोल ज्ञान आहे',
            'trainer1_q2': '4.2 व्याख्यात्याकडे समजावून सांगण्याचे कौशल्य आहे',
            'trainer1_q3': '4.3 व्याख्याता शंका समाधान उत्तमरित्या करू शकतो',
            'trainer1_q4': '4.4 आपले व्याख्यात्या बद्दल सर्वसाधारण मत (पेहराव, सहभाग, सादरीकरण इ.) चांगले आहे',
            'trainer2_name': 'Trainer 2 Name',
            'trainer2_q1': '4.1 व्याख्यात्याला विषयाचे सखोल ज्ञान आहे',
            'trainer2_q2': '4.2 व्याख्यात्याकडे समजावून सांगण्याचे कौशल्य आहे',
            'trainer2_q3': '4.3 व्याख्याता शंका समाधान उत्तमरित्या करू शकतो',
            'trainer2_q4': '4.4 आपले व्याख्यात्या बद्दल सर्वसाधारण मत (पेहराव, सहभाग, सादरीकरण इ.) चांगले आहे',
            'trainer3_name': 'Trainer 3 Name',
            'trainer3_q1': '4.1 व्याख्यात्याला विषयाचे सखोल ज्ञान आहे',
            'trainer3_q2': '4.2 व्याख्यात्याकडे समजावून सांगण्याचे कौशल्य आहे',
            'trainer3_q3': '4.3 व्याख्याता शंका समाधान उत्तमरित्या करू शकतो',
            'trainer3_q4': '4.4 आपले व्याख्यात्या बद्दल सर्वसाधारण मत (पेहराव, सहभाग, सादरीकरण इ.) चांगले आहे',
            'trainer4_name': 'Trainer 4 Name',
            'trainer4_q1': '4.1 व्याख्यात्याला विषयाचे सखोल ज्ञान आहे',
            'trainer4_q2': '4.2 व्याख्यात्याकडे समजावून सांगण्याचे कौशल्य आहे',
            'trainer4_q3': '4.3 व्याख्याता शंका समाधान उत्तमरित्या करू शकतो',
            'trainer4_q4': '4.4 आपले व्याख्यात्या बद्दल सर्वसाधारण मत (पेहराव, सहभाग, सादरीकरण इ.) चांगले आहे',
            'sec5_q1': '5.1 कार्यक्रमाचे संयोजन व इतर व्यवस्था - कार्यक्रमाचे संयोजन व इतर व्यवस्था समाधानकारक होती',
            'sec5_q2': '5.2 कार्यक्रमाचे संयोजन व इतर व्यवस्था - इतर पूरक व्यवस्था (चहा, नाष्टा इ.) समाधानकारक होती',
            'sec6_q1': '6.1 कार्यक्रमा बाबत - कार्यक्रमाची गती',
            'sec6_q2': '6.2 कार्यक्रमा बाबत - कार्यक्रमाचा कालावधी',
            'sec7_q1': '7.1 कार्यक्रमाचे सर्वसाधारण मूल्यमापन - उपयोगाच्या दृष्टीकोनातून या कार्यक्रमाचे मूल्यमापन करा',
            'sec7_q2': '7.2 कार्यक्रमाचे सर्वसाधारण मूल्यमापन - तुमच्या इतर सहकाऱ्यांनी सुध्दा या कार्यक्रमात यावे असे वाटते का',
            'sec7_q3_text': '7.3 कार्यक्रमाचे सर्वसाधारण मूल्यमापन - यातील कोणता विषय तुम्हाला सर्वात जास्त संबंधीत वाटला',
            'sec7_q4_text': '7.4 कार्यक्रमाचे सर्वसाधारण मूल्यमापन - या कार्यक्रमामध्ये एखाद्या विषयाचा समावेश झाला नाही असे आपणांस वाटते का? असल्यास तो विषय कोणता',
            'suggestions': '7.5 सुधारणेबाबत सूचना - या कार्यक्रमाच्या सुधारणेबाबत काही सूचना असल्यास खाली लिहा',
            'senior_name': 'senor name'
        }
        
        # Select only the columns we need in the desired order
        desired_columns = [
            'Sr. no', 'program_date', 'program_title', 'pmo_training_category', 
            'pl_category', 'brsr_sq_123_category', 'tni_status', 'learning_hours',
            'participants_name', 'bc_no', 'gender', 'employee_group', 
            'department', 'factory', 'sec1_q1', 'sec1_q2', 'sec2_q1', 
            'sec2_q2', 'sec2_q3', 'sec3_q1', 'trainer1_name', 'trainer1_q1', 
            'trainer1_q2', 'trainer1_q3', 'trainer1_q4', 'trainer2_name', 
            'trainer2_q1', 'trainer2_q2', 'trainer2_q3', 'trainer2_q4', 
            'trainer3_name', 'trainer3_q1', 'trainer3_q2', 'trainer3_q3', 
            'trainer3_q4', 'trainer4_name', 'trainer4_q1', 'trainer4_q2', 
            'trainer4_q3', 'trainer4_q4', 'sec5_q1', 'sec5_q2', 'sec6_q1', 
            'sec6_q2', 'sec7_q1', 'sec7_q2', 'sec7_q3_text', 'sec7_q4_text', 
            'suggestions', 'senior_name'
        ]
        
        # Filter the DataFrame to include only the desired columns
        df_filtered = df[desired_columns]
        
        # Rename columns using the mapping
        df_renamed = df_filtered.rename(columns=column_mapping)
        
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_renamed.to_excel(writer, sheet_name='Response', index=False)
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Response']
            
            # Add header format
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#003366',
                'font_color': 'white',
                'border': 1
            })
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(df_renamed.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust columns' width using safe function
            for i, col in enumerate(df_renamed.columns):
                max_len = safe_max_len(df_renamed[col], col)
                worksheet.set_column(i, i, max_len + 2)
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'CIRO_Feedback_Individual_{response_id}.xlsx'
        )
    
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error in export_individual: {str(e)}")
        flash(f"An error occurred during export: {str(e)}", "danger")
        return redirect(url_for('ciro.dashboard'))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@ciro_bp.route('/export/summary-report/<program_title>/<program_date>')
def export_summary_report(program_title, program_date):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        # Disable ONLY_FULL_GROUP_BY mode
        with conn.cursor() as cursor:
            cursor.execute("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))")
        
        conn.begin()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # Create Excel file with multiple sheets
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Sheet 1: Summary
            summary_query = """
                SELECT 
                    program_title,
                    program_date,
                    pmo_training_category,
                    pl_category,
                    brsr_sq_123_category,
                    COUNT(DISTINCT id) as response_count,
                    AVG((sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 + 
                        sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2)/12.0) as csi,
                    (
                        SELECT AVG(avg_score)
                        FROM (
                            SELECT (AVG(trainer1_q1) + AVG(trainer1_q2) + AVG(trainer1_q3) + AVG(trainer1_q4))/4 as avg_score
                                FROM feedback_responses 
                                WHERE program_title = fr.program_title 
                                AND program_date = fr.program_date
                                AND trainer1_name IS NOT NULL
                            UNION ALL
                            SELECT (AVG(trainer2_q1) + AVG(trainer2_q2) + AVG(trainer2_q3) + AVG(trainer2_q4))/4 as avg_score
                                FROM feedback_responses 
                                WHERE program_title = fr.program_title 
                                AND program_date = fr.program_date
                                AND trainer2_name IS NOT NULL
                            UNION ALL
                            SELECT (AVG(trainer3_q1) + AVG(trainer3_q2) + AVG(trainer3_q3) + AVG(trainer3_q4))/4 as avg_score
                                FROM feedback_responses 
                                WHERE program_title = fr.program_title 
                                AND program_date = fr.program_date
                                AND trainer3_name IS NOT NULL
                            UNION ALL
                            SELECT (AVG(trainer4_q1) + AVG(trainer4_q2) + AVG(trainer4_q3) + AVG(trainer4_q4))/4 as avg_score
                                FROM feedback_responses 
                                WHERE program_title = fr.program_title 
                                AND program_date = fr.program_date
                                AND trainer4_name IS NOT NULL
                        ) as trainer_avgs
                    ) as tfi,
                    AVG(
                        (sec1_q1 + sec1_q2 + sec2_q1 + sec2_q2 + sec2_q3 + sec3_q1 +
                         COALESCE(trainer1_q1,0) + COALESCE(trainer1_q2,0) + COALESCE(trainer1_q3,0) + COALESCE(trainer1_q4,0) +
                         COALESCE(trainer2_q1,0) + COALESCE(trainer2_q2,0) + COALESCE(trainer2_q3,0) + COALESCE(trainer2_q4,0) +
                         COALESCE(trainer3_q1,0) + COALESCE(trainer3_q2,0) + COALESCE(trainer3_q3,0) + COALESCE(trainer3_q4,0) +
                         COALESCE(trainer4_q1,0) + COALESCE(trainer4_q2,0) + COALESCE(trainer4_q3,0) + COALESCE(trainer4_q4,0) +
                         sec5_q1 + sec5_q2 + sec6_q1 + sec6_q2 + sec7_q1 + sec7_q2) /
                        (12 + 
                         CASE WHEN trainer1_q1 IS NOT NULL THEN 4 ELSE 0 END +
                         CASE WHEN trainer2_q1 IS NOT NULL THEN 4 ELSE 0 END +
                         CASE WHEN trainer3_q1 IS NOT NULL THEN 4 ELSE 0 END +
                         CASE WHEN trainer4_q1 IS NOT NULL THEN 4 ELSE 0 END)
                    ) as avg_score,
                    AVG(sec1_q1) as sec1_q1_avg,
                    AVG(sec1_q2) as sec1_q2_avg,
                    AVG(sec2_q1) as sec2_q1_avg,
                    AVG(sec2_q2) as sec2_q2_avg,
                    AVG(sec2_q3) as sec2_q3_avg,
                    AVG(sec3_q1) as sec3_q1_avg,
                    AVG(sec5_q1) as sec5_q1_avg,
                    AVG(sec5_q2) as sec5_q2_avg,
                    AVG(sec6_q1) as sec6_q1_avg,
                    AVG(sec6_q2) as sec6_q2_avg,
                    AVG(sec7_q1) as sec7_q1_avg,
                    AVG(sec7_q2) as sec7_q2_avg
                FROM feedback_responses fr
                WHERE program_title = %s AND program_date = %s
                GROUP BY program_title, program_date, pmo_training_category, pl_category, brsr_sq_123_category
            """
            
            cursor.execute(summary_query, (program_title, program_date))
            summary_data = cursor.fetchall()
            summary_df = pd.DataFrame(summary_data)
            
            if summary_df.empty:
                flash("No summary data found for this training session", "warning")
                return redirect(url_for('ciro.dashboard'))
            
            # Rename columns in summary sheet
            summary_df = summary_df.rename(columns={
                'program_title': 'Training Name',
                'program_date': 'Program Date',
                'pmo_training_category': 'PMO Training Category',
                'pl_category': 'PL Category',
                'brsr_sq_123_category': 'BRSR SQ 123 Category',
                'response_count': 'Response Count',
                'csi': 'CSI',
                'tfi': 'TFI',
                'avg_score': 'Average Score'
            })
            
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Get workbook and worksheet objects for summary sheet
            workbook = writer.book
            worksheet = writer.sheets['Summary']
            
            # Add header format
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#003366',
                'font_color': 'white',
                'border': 1
            })
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(summary_df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust columns' width using safe function
            for i, col in enumerate(summary_df.columns):
                max_len = safe_max_len(summary_df[col], col)
                worksheet.set_column(i, i, max_len + 2)
            
            # Sheet 2: Trainer Feedback
            trainer_query = """
                SELECT 
                    trainer_name,
                    AVG(q1) as knowledge_avg,
                    AVG(q2) as presentation_avg,
                    AVG(q3) as query_handling_avg,
                    AVG(q4) as overall_avg,
                    COUNT(*) as response_count
                FROM (
                    SELECT trainer1_name as trainer_name, trainer1_q1 as q1, trainer1_q2 as q2, trainer1_q3 as q3, trainer1_q4 as q4
                    FROM feedback_responses
                    WHERE program_title = %s AND program_date = %s AND trainer1_name IS NOT NULL
                    
                    UNION ALL
                    
                    SELECT trainer2_name as trainer_name, trainer2_q1 as q1, trainer2_q2 as q2, trainer2_q3 as q3, trainer2_q4 as q4
                    FROM feedback_responses
                    WHERE program_title = %s AND program_date = %s AND trainer2_name IS NOT NULL
                    
                    UNION ALL
                    
                    SELECT trainer3_name as trainer_name, trainer3_q1 as q1, trainer3_q2 as q2, trainer3_q3 as q3, trainer3_q4 as q4
                    FROM feedback_responses
                    WHERE program_title = %s AND program_date = %s AND trainer3_name IS NOT NULL
                    
                    UNION ALL
                    
                    SELECT trainer4_name as trainer_name, trainer4_q1 as q1, trainer4_q2 as q2, trainer4_q3 as q3, trainer4_q4 as q4
                    FROM feedback_responses
                    WHERE program_title = %s AND program_date = %s AND trainer4_name IS NOT NULL
                ) as all_trainers
                GROUP BY trainer_name
            """
            
            cursor.execute(trainer_query, (
                program_title, program_date, program_title, program_date, 
                program_title, program_date, program_title, program_date
            ))
            trainer_data = cursor.fetchall()
            trainer_df = pd.DataFrame(trainer_data)
            
            # Rename columns in trainer feedback sheet
            trainer_df = trainer_df.rename(columns={
                'trainer_name': 'Trainer Name',
                'knowledge_avg': 'Knowledge Average',
                'presentation_avg': 'Presentation Average',
                'query_handling_avg': 'Query Handling Average',
                'overall_avg': 'Overall Average',
                'response_count': 'Response Count'
            })
            
            trainer_df.to_excel(writer, sheet_name='Trainer Feedback', index=False)
            
            # Get worksheet for trainer feedback
            worksheet = writer.sheets['Trainer Feedback']
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(trainer_df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust columns' width using safe function
            for i, col in enumerate(trainer_df.columns):
                max_len = safe_max_len(trainer_df[col], col)
                worksheet.set_column(i, i, max_len + 2)
            
            # Sheet 3: Section Averages
            if not summary_df.empty:
                section_df = pd.DataFrame({
                    'Section': [
                        'Section 1: प्रशिक्षणार्थ्याची पूर्वतयारी (Participant Preparation)',
                        'Section 2: कार्यक्रमात समाविष्ट करण्यात आलेली माहिती (Program Content)',
                        'Section 3: कामामध्ये वापर (Use in Work)',
                        'Section 5: कार्यक्रमाचे संयोजन व इतर व्यवस्था (Program Organization)',
                        'Section 6: कार्यक्रमा बाबत (Program Details)',
                        'Section 7: कार्यक्रमाचे सर्वसाधारण मूल्यमापन (Overall Evaluation)'
                    ],
                    'Average Score': [
                        (summary_df.iloc[0]['sec1_q1_avg'] + summary_df.iloc[0]['sec1_q2_avg'])/2,
                        (summary_df.iloc[0]['sec2_q1_avg'] + summary_df.iloc[0]['sec2_q2_avg'] + summary_df.iloc[0]['sec2_q3_avg'])/3,
                        summary_df.iloc[0]['sec3_q1_avg'],
                        (summary_df.iloc[0]['sec5_q1_avg'] + summary_df.iloc[0]['sec5_q2_avg'])/2,
                        (summary_df.iloc[0]['sec6_q1_avg'] + summary_df.iloc[0]['sec6_q2_avg'])/2,
                        (summary_df.iloc[0]['sec7_q1_avg'] + summary_df.iloc[0]['sec7_q2_avg'])/2
                    ]
                })
                
                section_df.to_excel(writer, sheet_name='Section Averages', index=False)
                
                # Get worksheet for section averages
                worksheet = writer.sheets['Section Averages']
                
                # Write the column headers with the defined format
                for col_num, value in enumerate(section_df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Auto-adjust columns' width using safe function
                for i, col in enumerate(section_df.columns):
                    max_len = safe_max_len(section_df[col], col)
                    worksheet.set_column(i, i, max_len + 2)
            
            # Sheet 4: Text Feedback
            text_query = """
                SELECT 
                    participants_name as Participants_Name,
                    sec7_q3_text as Most_Relevant_Topic,
                    sec7_q4_text as Missing_Topics,
                    suggestions as Suggestions
                FROM feedback_responses
                WHERE program_title = %s AND program_date = %s
                AND (sec7_q3_text != '' OR sec7_q4_text != '' OR suggestions != '')
                ORDER BY created_at ASC  -- Changed from DESC to ASC to show data in entry order
            """
            
            cursor.execute(text_query, (program_title, program_date))
            text_data = cursor.fetchall()
            text_df = pd.DataFrame(text_data)
            
            # Format column names to capitalize first letter and remove underscores
            text_df.columns = [col.replace('_', ' ').title() for col in text_df.columns]
            
            text_df.to_excel(writer, sheet_name='Text Feedback', index=False)
            
            # Get worksheet for text feedback
            worksheet = writer.sheets['Text Feedback']
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(text_df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust columns' width using safe function
            for i, col in enumerate(text_df.columns):
                max_len = safe_max_len(text_df[col], col)
                worksheet.set_column(i, i, max_len + 2)
        
        conn.commit()
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'CIRO_Summary_Report_{program_title}_{program_date}.xlsx'
        )
    
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error in export_summary_report: {str(e)}")
        flash(f"An error occurred during export: {str(e)}", "danger")
        return redirect(url_for('ciro.dashboard'))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()