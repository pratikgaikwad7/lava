# cd_data_store.py
from flask import Blueprint, request, jsonify, render_template, flash, redirect, url_for
import pandas as pd
from utils import get_db_connection
from datetime import datetime

bp = Blueprint('cd_data_store', __name__, url_prefix='/cd_data_store')

# Flexible table configuration
TABLE_CONFIGS = {
    'induction': {
        'columns': [
            'sr_no', 'ticket_no', 'name', 'gender', 'employee_category',
            'plant_location', 'joined_year', 'date_from', 'date_to', 'shift',
            'learning_hours', 'training_name', 'batch_number', 'training_venue_name',
            'faculty_name', 'subject_name', 'remark'
        ],
        'required_columns': ['ticket_no', 'name'],
        'display_name': 'Induction Data'
    },
    'fta': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'FTA Data'
    },
    'jta': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'JTA Data'
    },
    'kaushalya': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'Kaushalya Data'
    },
    'pragati': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'Pragati Data'
    },
    'lakshya': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'Lakshya Data'
    },
    'live_trainer': {
        'columns': ['name', 'gender', 'ticket_no', 'doj'],
        'required_columns': ['name', 'ticket_no'],
        'display_name': 'Live Trainer Data'
    },
    'fst': {
        'columns': [
            'sr_no', 'ticket_no', 'name', 'gender', 'employee_category',
            'plant_location', 'joined_year', 'date_from', 'date_to', 'shift',
            'learning_hours', 'training_name', 'batch_number', 'training_venue_name',
            'faculty_name', 'fst_cell_name', 'remark'
        ],
        'required_columns': ['ticket_no', 'name'],
        'display_name': 'FST Data'
    },
    'master_data': {
        'columns': [
            'calendar_month', 'month_report_pmo_21_20', 'month_cd_key_26_25', 
            'start_date', 'end_date', 'start_time', 'end_time', 'learning_hours', 
            'training_name', 'pmo_training_category', 'pl_category', 
            'brsr_sq_123_category', 'calendar_need_base_reschedule', 'tni_non_tni', 
            'location_hall', 'faculty_1', 'faculty_2', 'faculty_3', 'faculty_4', 
            'per_no', 'participants_name', 'bc_no', 'gender', 'employee_group', 
            'department', 'factory', 'nomination_received_from', 'mobile_no', 
            'email', 'cordi_name', 'submission_timestamp', 'program_id', 
            'day_1_attendance', 'day_2_attendance', 'day_3_attendance', 'last_updated'
        ],
        'required_columns': ['training_name', 'participants_name'],
        'display_name': 'Master Training Data'
    }
}

# Excel headers mapping to DB columns
COLUMN_MAPPING = {
    'sr no': 'sr_no',
    'ticket number': 'ticket_no',
    'name': 'name',
    'gender': 'gender',
    'employee category': 'employee_category',
    'plant location': 'plant_location',
    'year': 'year',
    'date(from)': 'date_from',
    'date(to)': 'date_to',
    'shift': 'shift',
    'learning hours': 'learning_hours',
    'training name': 'training_name',
    'batch number': 'batch_number',
    'training venue name': 'training_venue_name',
    'faculty name': 'faculty_name',
    'subject name': 'subject_name',
    'remark': 'remark',
    # FST specific mapping
    'fst cell name': 'fst_cell_name',
    # Master data mappings
    'calender month': 'calendar_month',
    'month report pmo 21-20': 'month_report_pmo_21_20',
    'month cd/key 26-25': 'month_cd_key_26_25',
    'start date': 'start_date',
    'end date': 'end_date',
    'start time': 'start_time',
    'end time': 'end_time',
    'pmo training category': 'pmo_training_category',
    'pl category': 'pl_category',
    'brsr sq 1,2,3 category': 'brsr_sq_123_category',
    'calendar/need base/reschedule': 'calendar_need_base_reschedule',
    'tni / non tni': 'tni_non_tni',
    'location hall': 'location_hall',
    'faculty 1': 'faculty_1',
    'faculty 2': 'faculty_2',
    'faculty 3': 'faculty_3',
    'faculty 4': 'faculty_4',
    'per. no': 'per_no',
    'participants name': 'participants_name',
    'bc no': 'bc_no',
    'employee group': 'employee_group',
    'department': 'department',
    'factory': 'factory',
    'nomination recieved from': 'nomination_received_from'
}

def validate_file(file):
    if not file or file.filename == '':
        return False, 'No file selected'
    if not file.filename.endswith(('.xlsx', '.xls')):
        return False, 'Only Excel files are allowed'
    return True, 'File validated'

def clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return str(value)
    return str(value).strip()

def parse_date(date_val):
    if pd.isna(date_val):
        return None
    try:
        return pd.to_datetime(date_val, errors='coerce').date()
    except:
        return None

def parse_time(time_val):
    if pd.isna(time_val):
        return None
    try:
        # Try to parse as time
        return pd.to_datetime(time_val, errors='coerce').time()
    except:
        return None

def process_data(df, table_config):
    processed = []
    errors = []

    # Normalize Excel column names
    df.columns = [str(col).strip().lower() for col in df.columns]

    for idx, row in df.iterrows():
        try:
            # Initialize data_row with None for all columns
            data_row = {col: None for col in table_config['columns']}
            
            for excel_col, db_col in COLUMN_MAPPING.items():
                if db_col in table_config['columns']:
                    if excel_col in df.columns:
                        value = row[excel_col]
                        if 'date' in db_col:
                            data_row[db_col] = parse_date(value)
                        elif 'time' in db_col:
                            data_row[db_col] = parse_time(value)
                        else:
                            data_row[db_col] = clean_value(value)

            # Check required fields
            missing_req = [req for req in table_config['required_columns'] if not data_row.get(req)]
            if missing_req:
                errors.append(f"Row {idx+2}: Missing {', '.join(missing_req)}")
                continue

            processed.append(data_row)
        except Exception as e:
            errors.append(f"Row {idx+2}: Error - {str(e)}")
    return processed, errors

# ✅ Updated insert_data with upsert logic
def insert_data(table_name, data):
    """
    Inserts new records or updates existing ones based on ticket_no.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        columns = TABLE_CONFIGS[table_name]['columns']
        insert_columns = [col for col in columns if col != 'sr_no']  # skip auto-increment
        placeholders = ', '.join(['%s'] * len(insert_columns))
        columns_str = ', '.join(insert_columns)

        # Update only relevant columns (exclude sr_no and ticket_no)
        update_cols = [col for col in insert_columns if col != 'ticket_no']
        update_str = ', '.join([f"{col} = VALUES({col})" for col in update_cols])

        sql = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_str}
        """

        values_list = [[row.get(col) for col in insert_columns] for row in data]
        cursor.executemany(sql, values_list)
        conn.commit()
        return True, f"Processed {len(data)} records into {table_name} (inserted/updated)"
    except Exception as e:
        conn.rollback()
        return False, f"Database error: {str(e)}"
    finally:
        cursor.close()
        conn.close()

@bp.route('/upload_page')
def upload_page():
    return render_template('admin_upload_files.html', table_configs=TABLE_CONFIGS)

@bp.route('/upload', methods=['POST'])
def upload_data():
    try:
        table_name = request.form.get('table_name')
        if not table_name or table_name not in TABLE_CONFIGS:
            flash('Invalid table selected', 'danger')
            return redirect(url_for('cd_data_store.upload_page'))

        if 'file' not in request.files:
            flash('No file provided', 'danger')
            return redirect(url_for('cd_data_store.upload_page'))

        file = request.files['file']
        is_valid, msg = validate_file(file)
        if not is_valid:
            flash(msg, 'danger')
            return redirect(url_for('cd_data_store.upload_page'))

        try:
            df = pd.read_excel(file)
        except Exception as e:
            flash(f'Error reading Excel: {str(e)}', 'danger')
            return redirect(url_for('cd_data_store.upload_page'))

        processed_data, errors = process_data(df, TABLE_CONFIGS[table_name])
        if errors:
            flash(f'Found {len(errors)} errors in data. First error: {errors[0]}', 'warning')
            return redirect(url_for('cd_data_store.upload_page'))
        if not processed_data:
            flash('No valid data to process', 'warning')
            return redirect(url_for('cd_data_store.upload_page'))

        success, msg = insert_data(table_name, processed_data)
        flash(msg, 'success' if success else 'danger')
        return redirect(url_for('cd_data_store.upload_page'))
    except Exception as e:
        flash(f'Unexpected error: {str(e)}', 'danger')
        return redirect(url_for('cd_data_store.upload_page'))

@bp.route('/api/upload/<table_name>', methods=['POST'])
def api_upload_data(table_name):
    try:
        if table_name not in TABLE_CONFIGS:
            return jsonify({'success': False, 'message': 'Invalid table name'}), 400
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file provided'}), 400
        file = request.files['file']
        is_valid, msg = validate_file(file)
        if not is_valid:
            return jsonify({'success': False, 'message': msg}), 400
        try:
            df = pd.read_excel(file)
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error reading Excel: {str(e)}'}), 400

        processed_data, errors = process_data(df, TABLE_CONFIGS[table_name])
        if errors:
            return jsonify({
                'success': False,
                'message': 'Data validation errors',
                'errors': errors[:5],
                'valid_records': len(processed_data)
            }), 400
        if not processed_data:
            return jsonify({'success': False, 'message': 'No valid data to process'}), 400

        success, msg = insert_data(table_name, processed_data)
        if success:
            return jsonify({
                'success': True,
                'message': msg,
                'records_processed': len(processed_data)
            }), 200
        else:
            return jsonify({'success': False, 'message': msg}), 500
    except Exception as e:
        return jsonify({'success': False, 'message': f'Unexpected error: {str(e)}'}), 500

@bp.route('/api/tables', methods=['GET'])
def api_get_tables():
    tables_info = {
        name: {
            'display_name': config['display_name'],
            'columns': config['columns'],
            'required_columns': config['required_columns']
        }
        for name, config in TABLE_CONFIGS.items()
    }
    return jsonify({'success': True, 'tables': tables_info}), 200