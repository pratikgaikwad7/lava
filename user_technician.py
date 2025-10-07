from flask import Blueprint, render_template, request, jsonify, send_file
from utils import get_db_connection
import json
import pandas as pd
from io import BytesIO

# Blueprint definition
user_tech_bp = Blueprint('user_tech_bp', __name__, url_prefix='/user_tech')

@user_tech_bp.route('/induction', methods=['GET'])
def induction_list():
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = "SELECT * FROM induction ORDER BY sr_no DESC"
            cursor.execute(sql)
            induction_data = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching induction data: {e}")
        induction_data = []
    finally:
        conn.close()

    return render_template("user/induction.html", induction_data=induction_data)

@user_tech_bp.route('/api/induction/filter-options', methods=['GET'])
def get_filter_options():
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Get unique values for each filter
            cursor.execute("SELECT DISTINCT plant_location FROM induction WHERE plant_location IS NOT NULL ORDER BY plant_location")
            plants = [row['plant_location'] for row in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT batch_number FROM induction WHERE batch_number IS NOT NULL ORDER BY batch_number")
            batches = [row['batch_number'] for row in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT learning_hours FROM induction WHERE learning_hours IS NOT NULL ORDER BY learning_hours")
            learning_hours = [str(int(row['learning_hours'])) for row in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT gender FROM induction WHERE gender IS NOT NULL ORDER BY gender")
            genders = [row['gender'] for row in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT employee_category FROM induction WHERE employee_category IS NOT NULL ORDER BY employee_category")
            categories = [row['employee_category'] for row in cursor.fetchall()]
            
            # Get distinct academic years from joined_year
            cursor.execute("SELECT DISTINCT joined_year FROM induction WHERE joined_year IS NOT NULL ORDER BY joined_year")
            academic_years = []
            for row in cursor.fetchall():
                year = row['joined_year']
                # Format as "YYYY/YY"
                formatted_year = f"{int(year)}/{str(int(year)+1)[2:]}"
                academic_years.append(formatted_year)
            
            # Get distinct faculty names
            cursor.execute("SELECT DISTINCT faculty_name FROM induction WHERE faculty_name IS NOT NULL ORDER BY faculty_name")
            faculties = [row['faculty_name'] for row in cursor.fetchall()]
            
            # Get distinct shifts
            cursor.execute("SELECT DISTINCT shift FROM induction WHERE shift IS NOT NULL ORDER BY shift")
            shifts = [row['shift'] for row in cursor.fetchall()]
            
        return jsonify({
            'plants': plants,
            'batches': batches,
            'learning_hours': learning_hours,
            'genders': genders,
            'categories': categories,
            'academic_years': academic_years,
            'faculties': faculties,
            'shifts': shifts
        })
    except Exception as e:
        print(f"Error fetching filter options: {e}")
        return jsonify({
            'plants': [],
            'batches': [],
            'learning_hours': [],
            'genders': [],
            'categories': [],
            'academic_years': [],
            'faculties': [],
            'shifts': []
        }), 500
    finally:
        conn.close()

@user_tech_bp.route('/api/induction/data', methods=['GET'])
def get_induction_data():
    try:
        # Get filter parameters from request
        plant = request.args.get('plant', 'all')
        batch = request.args.get('batch', 'all')
        hours = request.args.get('hours', 'all')
        gender = request.args.get('gender', 'all')
        category = request.args.get('category', 'all')
        academic_year = request.args.get('academic_year', 'all')
        faculty = request.args.get('faculty', 'all')
        shift = request.args.get('shift', 'all')
        ticket = request.args.get('ticket', '')
        start_date = request.args.get('startDate', '')
        end_date = request.args.get('endDate', '')
        
        # Pagination parameters
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Build WHERE clause based on filters
            where_conditions = []
            params = []
            
            if plant != 'all':
                where_conditions.append("plant_location = %s")
                params.append(plant)
            
            if batch != 'all':
                where_conditions.append("batch_number = %s")
                params.append(batch)
            
            if hours != 'all':
                where_conditions.append("learning_hours = %s")
                params.append(float(hours))
            
            if gender != 'all':
                where_conditions.append("gender = %s")
                params.append(gender)
            
            if category != 'all':
                where_conditions.append("employee_category = %s")
                params.append(category)
            
            # Handle academic year filter
            if academic_year != 'all':
                # Extract the base year from the formatted academic year (e.g., "2023/24" -> 2023)
                base_year = academic_year.split('/')[0]
                where_conditions.append("joined_year = %s")
                params.append(base_year)
            
            if faculty != 'all':
                where_conditions.append("faculty_name = %s")
                params.append(faculty)
            
            if shift != 'all':
                where_conditions.append("shift = %s")
                params.append(shift)
            
            if ticket:
                where_conditions.append("ticket_no LIKE %s")
                params.append(f"%{ticket}%")
            
            if start_date:
                where_conditions.append("date_from >= %s")
                params.append(start_date)
            
            if end_date:
                where_conditions.append("date_to <= %s")
                params.append(end_date)
            
            # Build the SQL query
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            sql = f"SELECT * FROM induction WHERE {where_clause} ORDER BY sr_no DESC LIMIT {limit} OFFSET {offset}"
            
            cursor.execute(sql, params)
            records = cursor.fetchall()
            
            # Get total count for pagination
            count_sql = f"SELECT COUNT(*) as total FROM induction WHERE {where_clause}"
            cursor.execute(count_sql, params)
            total_count = cursor.fetchone()['total']
            
            # Calculate statistics
            stats_sql = f"""
                SELECT 
                    COUNT(DISTINCT batch_number) as batch_count,
                    COUNT(*) as coverage_count,
                    COALESCE(SUM(learning_hours), 0) as learning_hours
                FROM induction 
                WHERE {where_clause}
            """
            cursor.execute(stats_sql, params)
            stats = cursor.fetchone()
            
        # Convert records to list of dictionaries for JSON serialization
        records_list = []
        for record in records:
            # Format academic year for display
            academic_year_display = ""
            if record['joined_year']:
                academic_year_display = f"{int(record['joined_year'])}/{str(int(record['joined_year'])+1)[2:]}"
            
            records_list.append({
                'sr_no': record['sr_no'],
                'ticket_no': record['ticket_no'],
                'name': record['name'],
                'gender': record['gender'],
                'employee_category': record['employee_category'],
                'plant_location': record['plant_location'],
                'academic_year': academic_year_display,
                'date_from': record['date_from'].strftime('%Y-%m-%d') if record['date_from'] else '',
                'date_to': record['date_to'].strftime('%Y-%m-%d') if record['date_to'] else '',
                'shift': record['shift'],
                'learning_hours': int(record['learning_hours']) if record['learning_hours'] else 0,
                'training_name': record['training_name'],
                'batch_number': record['batch_number'],
                'training_venue_name': record['training_venue_name'],
                'faculty_name': record['faculty_name'],
                'subject_name': record['subject_name'],
                'remark': record['remark']
            })
        
        return jsonify({
            'records': records_list,
            'stats': {
                'batch_count': stats['batch_count'] if stats else 0,
                'coverage_count': stats['coverage_count'] if stats else 0,
                'learning_hours': int(stats['learning_hours']) if stats else 0
            },
            'total_records': total_count
        })
        
    except Exception as e:
        print(f"Error fetching induction data: {e}")
        return jsonify({
            'records': [],
            'stats': {
                'batch_count': 0,
                'coverage_count': 0,
                'learning_hours': 0
            },
            'total_records': 0
        }), 500
    finally:
        conn.close()

@user_tech_bp.route('/api/induction/download', methods=['GET'])
def download_induction_data():
    try:
        # Get filter parameters from request
        plant = request.args.get('plant', 'all')
        batch = request.args.get('batch', 'all')
        hours = request.args.get('hours', 'all')
        gender = request.args.get('gender', 'all')
        category = request.args.get('category', 'all')
        academic_year = request.args.get('academic_year', 'all')
        faculty = request.args.get('faculty', 'all')
        shift = request.args.get('shift', 'all')
        ticket = request.args.get('ticket', '')
        start_date = request.args.get('startDate', '')
        end_date = request.args.get('endDate', '')
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Build WHERE clause based on filters
            where_conditions = []
            params = []
            
            if plant != 'all':
                where_conditions.append("plant_location = %s")
                params.append(plant)
            
            if batch != 'all':
                where_conditions.append("batch_number = %s")
                params.append(batch)
            
            if hours != 'all':
                where_conditions.append("learning_hours = %s")
                params.append(float(hours))
            
            if gender != 'all':
                where_conditions.append("gender = %s")
                params.append(gender)
            
            if category != 'all':
                where_conditions.append("employee_category = %s")
                params.append(category)
            
            # Handle academic year filter
            if academic_year != 'all':
                base_year = academic_year.split('/')[0]
                where_conditions.append("joined_year = %s")
                params.append(base_year)
            
            if faculty != 'all':
                where_conditions.append("faculty_name = %s")
                params.append(faculty)
            
            if shift != 'all':
                where_conditions.append("shift = %s")
                params.append(shift)
            
            if ticket:
                where_conditions.append("ticket_no LIKE %s")
                params.append(f"%{ticket}%")
            
            if start_date:
                where_conditions.append("date_from >= %s")
                params.append(start_date)
            
            if end_date:
                where_conditions.append("date_to <= %s")
                params.append(end_date)
            
            # Build the SQL query
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            sql = f"SELECT * FROM induction WHERE {where_clause} ORDER BY sr_no DESC"
            
            cursor.execute(sql, params)
            records = cursor.fetchall()
            
            # Convert records to DataFrame
            df = pd.DataFrame(records)
            
            # Rename columns to proper names
            column_mapping = {
                'sr_no': 'Sr No',
                'ticket_no': 'Ticket No',
                'name': 'Name',
                'gender': 'Gender',
                'employee_category': 'Employee Category',
                'plant_location': 'Plant Location',
                'date_from': 'Date From',
                'date_to': 'Date To',
                'shift': 'Shift',
                'learning_hours': 'Learning Hours',
                'training_name': 'Training Name',
                'batch_number': 'Batch Number',
                'training_venue_name': 'Training Venue Name',
                'faculty_name': 'Faculty Name',
                'subject_name': 'Subject Name',
                'remark': 'Remark'
            }
            
            # Rename the columns
            df = df.rename(columns=column_mapping)
            
            # Create Academic Year column from joined_year and format it
            if 'joined_year' in df.columns:
                df['Academic Year'] = df['joined_year'].apply(
                    lambda x: f"{int(x)}/{str(int(x)+1)[2:]}" if pd.notnull(x) else ''
                )
                # Drop the original joined_year column
                df = df.drop(columns=['joined_year'])
            
            # Format dates
            date_columns = ['Date From', 'Date To']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
            
            # Convert learning_hours to int
            if 'Learning Hours' in df.columns:
                df['Learning Hours'] = df['Learning Hours'].apply(
                    lambda x: int(x) if pd.notnull(x) else 0
                )
            
            # Reorder columns to match the desired order
            desired_columns = [
                'Sr No', 'Ticket No', 'Name', 'Gender', 'Employee Category', 
                'Plant Location', 'Academic Year', 'Date From', 
                'Date To', 'Shift', 'Learning Hours', 'Training Name', 
                'Batch Number', 'Training Venue Name', 'Faculty Name', 
                'Subject Name', 'Remark'
            ]
            
            # Filter to only include columns that exist in the dataframe
            columns_to_use = [col for col in desired_columns if col in df.columns]
            df = df[columns_to_use]
            
            # Create an Excel file in memory
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Induction Data')
                
                # Adjust column widths for better readability
                worksheet = writer.sheets['Induction Data']
                for idx, col in enumerate(df.columns):
                    # Set column width based on content length
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(col)
                    ) + 2  # Add some padding
                    worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)  # Cap width at 50
            
            output.seek(0)
            
            # Return the Excel file as a response
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='induction_data.xlsx'
            )
            
    except Exception as e:
        print(f"Error generating Excel file: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# FST Main Page
@user_tech_bp.route('/fst', methods=['GET'])
def fst_list():
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = "SELECT * FROM fst ORDER BY sr_no DESC"
            cursor.execute(sql)
            fst_data = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching FST data: {e}")
        fst_data = []
    finally:
        conn.close()

    return render_template("user/fst.html", fst_data=fst_data)

# FST Filter Options API
@user_tech_bp.route('/api/fst/filter-options', methods=['GET'])
def get_fst_filter_options():
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Get unique values for each filter
            cursor.execute("SELECT DISTINCT plant_location FROM fst WHERE plant_location IS NOT NULL ORDER BY plant_location")
            plants = [row['plant_location'] for row in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT batch_number FROM fst WHERE batch_number IS NOT NULL ORDER BY batch_number")
            batches = [row['batch_number'] for row in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT learning_hours FROM fst WHERE learning_hours IS NOT NULL ORDER BY learning_hours")
            learning_hours = [str(row['learning_hours']) for row in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT gender FROM fst WHERE gender IS NOT NULL ORDER BY gender")
            genders = [row['gender'] for row in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT employee_category FROM fst WHERE employee_category IS NOT NULL ORDER BY employee_category")
            categories = [row['employee_category'] for row in cursor.fetchall()]
            
            # Get distinct academic years from joined_year
            cursor.execute("SELECT DISTINCT joined_year FROM fst WHERE joined_year IS NOT NULL ORDER BY joined_year")
            academic_years = []
            for row in cursor.fetchall():
                year = row['joined_year']
                # Format as "YYYY/YY"
                formatted_year = f"{int(year)}/{str(int(year)+1)[2:]}"
                academic_years.append(formatted_year)
            
            # Get distinct faculty names
            cursor.execute("SELECT DISTINCT faculty_name FROM fst WHERE faculty_name IS NOT NULL ORDER BY faculty_name")
            faculties = [row['faculty_name'] for row in cursor.fetchall()]
            
            # Get distinct shifts
            cursor.execute("SELECT DISTINCT shift FROM fst WHERE shift IS NOT NULL ORDER BY shift")
            shifts = [row['shift'] for row in cursor.fetchall()]
            
            # Get distinct FST cell names
            cursor.execute("SELECT DISTINCT fst_cell_name FROM fst WHERE fst_cell_name IS NOT NULL ORDER BY fst_cell_name")
            fst_cells = [row['fst_cell_name'] for row in cursor.fetchall()]
            
        return jsonify({
            'plants': plants,
            'batches': batches,
            'learning_hours': learning_hours,
            'genders': genders,
            'categories': categories,
            'academic_years': academic_years,
            'faculties': faculties,
            'shifts': shifts,
            'fst_cells': fst_cells
        })
    except Exception as e:
        print(f"Error fetching FST filter options: {e}")
        return jsonify({
            'plants': [],
            'batches': [],
            'learning_hours': [],
            'genders': [],
            'categories': [],
            'academic_years': [],
            'faculties': [],
            'shifts': [],
            'fst_cells': []
        }), 500
    finally:
        conn.close()

# FST Data API
@user_tech_bp.route('/api/fst/data', methods=['GET'])
def get_fst_data():
    try:
        # Get filter parameters from request
        plant = request.args.get('plant', 'all')
        batch = request.args.get('batch', 'all')
        hours = request.args.get('hours', 'all')
        gender = request.args.get('gender', 'all')
        category = request.args.get('category', 'all')
        academic_year = request.args.get('academic_year', 'all')
        faculty = request.args.get('faculty', 'all')
        shift = request.args.get('shift', 'all')
        fst_cell = request.args.get('fst_cell', 'all')
        ticket = request.args.get('ticket', '')
        start_date = request.args.get('startDate', '')
        end_date = request.args.get('endDate', '')
        
        # Pagination parameters
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Build WHERE clause based on filters
            where_conditions = []
            params = []
            
            if plant != 'all':
                where_conditions.append("plant_location = %s")
                params.append(plant)
            
            if batch != 'all':
                where_conditions.append("batch_number = %s")
                params.append(batch)
            
            if hours != 'all':
                where_conditions.append("learning_hours = %s")
                params.append(hours)
            
            if gender != 'all':
                where_conditions.append("gender = %s")
                params.append(gender)
            
            if category != 'all':
                where_conditions.append("employee_category = %s")
                params.append(category)
            
            # Handle academic year filter
            if academic_year != 'all':
                # Extract the base year from the formatted academic year (e.g., "2023/24" -> 2023)
                base_year = academic_year.split('/')[0]
                where_conditions.append("joined_year = %s")
                params.append(base_year)
            
            if faculty != 'all':
                where_conditions.append("faculty_name = %s")
                params.append(faculty)
            
            if shift != 'all':
                where_conditions.append("shift = %s")
                params.append(shift)
            
            if fst_cell != 'all':
                where_conditions.append("fst_cell_name = %s")
                params.append(fst_cell)
            
            if ticket:
                where_conditions.append("ticket_no LIKE %s")
                params.append(f"%{ticket}%")
            
            if start_date:
                where_conditions.append("date_from >= %s")
                params.append(start_date)
            
            if end_date:
                where_conditions.append("date_to <= %s")
                params.append(end_date)
            
            # Build the SQL query
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            sql = f"SELECT * FROM fst WHERE {where_clause} ORDER BY sr_no DESC LIMIT {limit} OFFSET {offset}"
            
            cursor.execute(sql, params)
            records = cursor.fetchall()
            
            # Get total count for pagination
            count_sql = f"SELECT COUNT(*) as total FROM fst WHERE {where_clause}"
            cursor.execute(count_sql, params)
            total_count = cursor.fetchone()['total']
            
            # Calculate statistics
            stats_sql = f"""
                SELECT 
                    COUNT(DISTINCT batch_number) as batch_count,
                    COUNT(*) as coverage_count,
                    COALESCE(SUM(CAST(learning_hours AS SIGNED)), 0) as learning_hours
                FROM fst 
                WHERE {where_clause}
            """
            cursor.execute(stats_sql, params)
            stats = cursor.fetchone()
            
        # Convert records to list of dictionaries for JSON serialization
        records_list = []
        for record in records:
            # Format academic year for display
            academic_year_display = ""
            if record['joined_year']:
                academic_year_display = f"{int(record['joined_year'])}/{str(int(record['joined_year'])+1)[2:]}"
            
            records_list.append({
                'sr_no': record['sr_no'],
                'ticket_no': record['ticket_no'],
                'name': record['name'],
                'gender': record['gender'],
                'employee_category': record['employee_category'],
                'plant_location': record['plant_location'],
                'academic_year': academic_year_display,
                'date_from': record['date_from'].strftime('%Y-%m-%d') if record['date_from'] else '',
                'date_to': record['date_to'].strftime('%Y-%m-%d') if record['date_to'] else '',
                'shift': record['shift'],
                'learning_hours': record['learning_hours'],
                'training_name': record['training_name'],
                'batch_number': record['batch_number'],
                'training_venue_name': record['training_venue_name'],
                'faculty_name': record['faculty_name'],
                'fst_cell_name': record['fst_cell_name'],
                'remark': record['remark']
            })
        
        return jsonify({
            'records': records_list,
            'stats': {
                'batch_count': stats['batch_count'] if stats else 0,
                'coverage_count': stats['coverage_count'] if stats else 0,
                'learning_hours': int(stats['learning_hours']) if stats else 0
            },
            'total_records': total_count
        })
        
    except Exception as e:
        print(f"Error fetching FST data: {e}")
        return jsonify({
            'records': [],
            'stats': {
                'batch_count': 0,
                'coverage_count': 0,
                'learning_hours': 0
            },
            'total_records': 0
        }), 500
    finally:
        conn.close()

# FST Download Excel API
@user_tech_bp.route('/api/fst/download', methods=['GET'])
def download_fst_data():
    try:
        # Get filter parameters from request
        plant = request.args.get('plant', 'all')
        batch = request.args.get('batch', 'all')
        hours = request.args.get('hours', 'all')
        gender = request.args.get('gender', 'all')
        category = request.args.get('category', 'all')
        academic_year = request.args.get('academic_year', 'all')
        faculty = request.args.get('faculty', 'all')
        shift = request.args.get('shift', 'all')
        fst_cell = request.args.get('fst_cell', 'all')
        ticket = request.args.get('ticket', '')
        start_date = request.args.get('startDate', '')
        end_date = request.args.get('endDate', '')
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Build WHERE clause based on filters
            where_conditions = []
            params = []
            
            if plant != 'all':
                where_conditions.append("plant_location = %s")
                params.append(plant)
            
            if batch != 'all':
                where_conditions.append("batch_number = %s")
                params.append(batch)
            
            if hours != 'all':
                where_conditions.append("learning_hours = %s")
                params.append(hours)
            
            if gender != 'all':
                where_conditions.append("gender = %s")
                params.append(gender)
            
            if category != 'all':
                where_conditions.append("employee_category = %s")
                params.append(category)
            
            # Handle academic year filter
            if academic_year != 'all':
                base_year = academic_year.split('/')[0]
                where_conditions.append("joined_year = %s")
                params.append(base_year)
            
            if faculty != 'all':
                where_conditions.append("faculty_name = %s")
                params.append(faculty)
            
            if shift != 'all':
                where_conditions.append("shift = %s")
                params.append(shift)
            
            if fst_cell != 'all':
                where_conditions.append("fst_cell_name = %s")
                params.append(fst_cell)
            
            if ticket:
                where_conditions.append("ticket_no LIKE %s")
                params.append(f"%{ticket}%")
            
            if start_date:
                where_conditions.append("date_from >= %s")
                params.append(start_date)
            
            if end_date:
                where_conditions.append("date_to <= %s")
                params.append(end_date)
            
            # Build the SQL query
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            sql = f"SELECT * FROM fst WHERE {where_clause} ORDER BY sr_no DESC"
            
            cursor.execute(sql, params)
            records = cursor.fetchall()
            
            # Convert records to DataFrame
            df = pd.DataFrame(records)
            
            # Rename columns to proper names
            column_mapping = {
                'sr_no': 'Sr No',
                'ticket_no': 'Ticket No',
                'name': 'Name',
                'gender': 'Gender',
                'employee_category': 'Employee Category',
                'plant_location': 'Plant Location',
                'date_from': 'Date From',
                'date_to': 'Date To',
                'shift': 'Shift',
                'learning_hours': 'Learning Hours',
                'training_name': 'Training Name',
                'batch_number': 'Batch Number',
                'training_venue_name': 'Training Venue Name',
                'faculty_name': 'Faculty Name',
                'fst_cell_name': 'FST Cell Name',
                'remark': 'Remark'
            }
            
            # Rename the columns
            df = df.rename(columns=column_mapping)
            
            # Create Academic Year column from joined_year and format it
            if 'joined_year' in df.columns:
                df['Academic Year'] = df['joined_year'].apply(
                    lambda x: f"{int(x)}/{str(int(x)+1)[2:]}" if pd.notnull(x) else ''
                )
                # Drop the original joined_year column
                df = df.drop(columns=['joined_year'])
            
            # Format dates
            date_columns = ['Date From', 'Date To']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
            
            # Reorder columns to match the desired order
            desired_columns = [
                'Sr No', 'Ticket No', 'Name', 'Gender', 'Employee Category', 
                'Plant Location', 'Academic Year', 'Date From', 
                'Date To', 'Shift', 'Learning Hours', 'Training Name', 
                'Batch Number', 'Training Venue Name', 'Faculty Name', 
                'FST Cell Name', 'Remark'
            ]
            
            # Filter to only include columns that exist in the dataframe
            columns_to_use = [col for col in desired_columns if col in df.columns]
            df = df[columns_to_use]
            
            # Create an Excel file in memory
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='FST Data')
                
                # Adjust column widths for better readability
                worksheet = writer.sheets['FST Data']
                for idx, col in enumerate(df.columns):
                    # Set column width based on content length
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(col)
                    ) + 2  # Add some padding
                    worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)  # Cap width at 50
            
            output.seek(0)
            
            # Return the Excel file as a response
            return send_file(
                output,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name='fst_data.xlsx'
            )
            
    except Exception as e:
        print(f"Error generating FST Excel file: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()