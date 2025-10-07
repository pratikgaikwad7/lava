from flask import Blueprint, render_template, request, redirect, url_for, flash
import pandas as pd
from datetime import datetime
from collections import defaultdict
from utils import Config, get_db_connection  # Removed get_month_index and format_program_dates

target_bp = Blueprint('target', __name__, url_prefix='/target')

def get_month_index():
    """Get the current month index where April=1, May=2, ..., January=10"""
    current_month = datetime.now().month
    if current_month >= 4:
        return current_month - 3
    else:
        return min(current_month + 9, 10)

def get_available_years():
    """Get all available years from the database"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT target_year FROM training_targets ORDER BY target_year DESC")
            db_years = [year['target_year'] for year in cursor.fetchall()]
            return db_years
    except Exception as e:
        print(f"Error getting available years: {e}")
        return []
    finally:
        conn.close()

def initialize_new_year(target_year, conn, source_year=None):
    """Initialize a new year by copying structure from specified source year"""
    if source_year is None:
        # If no source year specified, use the latest available year
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(target_year) as latest_year FROM training_targets")
            result = cursor.fetchone()
            source_year = result['latest_year'] if result['latest_year'] else target_year - 1
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO training_targets (
                    training_name, pmo_category, pl_category, target_year,
                    target, batch_size
                )
                SELECT 
                    training_name, pmo_category, pl_category, %s,
                    0 as target, 0 as batch_size  -- Set defaults to 0
                FROM training_targets 
                WHERE target_year = %s
                ON DUPLICATE KEY UPDATE
                    target = VALUES(target),
                    batch_size = VALUES(batch_size)
            """, (target_year, source_year))
            conn.commit()
            return True
    except Exception as e:
        print(f"Error initializing new year: {e}")
        return False

def sync_training_data_from_master(target_year, conn):
    """Sync training data from training_names table to training_targets table"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    training_name, 
                    pmo_training_category, 
                    pl_category
                FROM training_names
                WHERE tni_status = 'TNI'
            """)
            training_data = cursor.fetchall()
            
            for training in training_data:
                cursor.execute("""
                    INSERT INTO training_targets 
                    (training_name, pmo_category, pl_category, target_year, target, batch_size)
                    VALUES (%s, %s, %s, %s, 0, 0)
                    ON DUPLICATE KEY UPDATE
                        target = VALUES(target),
                        batch_size = VALUES(batch_size)
                """, (
                    training['training_name'],
                    training['pmo_training_category'],
                    training['pl_category'],
                    target_year
                ))
            
            conn.commit()
            return True, f"Synced {len(training_data)} training records"
            
    except Exception as e:
        conn.rollback()
        return False, f"Error syncing training data: {str(e)}"

def normalize_training_name(name):
    """Normalize training name for comparison by removing extra spaces and special characters"""
    if not name:
        return ""
    # Convert to lowercase, remove extra spaces, and normalize special characters
    normalized = ' '.join(name.lower().split())
    # Replace common special characters with standard equivalents
    normalized = normalized.replace('&', 'and')
    normalized = normalized.replace('+', 'plus')
    normalized = normalized.replace('-', ' ')
    return normalized

from collections import defaultdict
from difflib import get_close_matches
from datetime import datetime

def update_training_completion_counts(conn, training_name=None, tni_status=None):
    """
    Update training completion counts in training_targets table based on master_data attendance.
    Uses advanced matching to handle variations in training names and categories.
    """
    try:
        with conn.cursor() as cursor:
            # Reset counts for current year
            current_year = datetime.now().year
            cursor.execute("""
                UPDATE training_targets 
                SET 
                    ytd_actual = 0,
                    april = 0,
                    may = 0,
                    june = 0,
                    july = 0,
                    august = 0,
                    september = 0,
                    october = 0,
                    november = 0,
                    december = 0,
                    january = 0,
                    february = 0,
                    march = 0
                WHERE target_year = %s
            """, (current_year,))

            # Fetch training_targets for the current year
            cursor.execute("""
                SELECT id, training_name, pmo_category, pl_category, target, batch_size
                FROM training_targets
                WHERE target_year = %s
            """, (current_year,))
            target_trainings = cursor.fetchall()

            # Build normalized mapping
            training_map = {}
            for training in target_trainings:
                normalized_name = normalize_training_name(training['training_name'])
                keys = [
                    (normalized_name, training['pmo_category'], training['pl_category']),
                    (normalized_name, training['pmo_category'], None),
                    (normalized_name, None, training['pl_category']),
                    (normalized_name, None, None)
                ]
                for key in keys:
                    training_map.setdefault(key, []).append(training)

            # Fetch master_data attendance
            query = """
                SELECT 
                    training_name,
                    pmo_training_category as pmo_category,
                    pl_category,
                    calendar_month,
                    COUNT(DISTINCT per_no) as attendance_count
                FROM master_data
                WHERE calendar_month IS NOT NULL
            """
            query_params = []
            if training_name:
                query += " AND training_name LIKE %s"
                query_params.append(f"%{training_name}%")
            if tni_status and tni_status != 'All':
                query += " AND tni_non_tni = %s"
                query_params.append(tni_status)
            query += """
                GROUP BY training_name, pmo_training_category, pl_category, calendar_month
            """
            cursor.execute(query, query_params)
            master_data_records = cursor.fetchall()
            if not master_data_records:
                print("No attendance records found matching criteria")
                return False

            # Organize master_data by training_id and month
            master_data_by_training = defaultdict(lambda: defaultdict(int))
            unmatched_records = []

            for record in master_data_records:
                master_name = normalize_training_name(record['training_name'])
                master_pmo = record['pmo_category'].strip() if record['pmo_category'] else None
                master_pl = record['pl_category'].strip() if record['pl_category'] else None
                month = record['calendar_month'].lower().strip() if record['calendar_month'] else None
                if not month:
                    continue

                matched = False
                match_attempts = [
                    (master_name, master_pmo, master_pl),
                    (master_name, master_pmo, None),
                    (master_name, None, master_pl),
                    (master_name, None, None)
                ]
                for key in match_attempts:
                    if key in training_map:
                        training_record = training_map[key][0]
                        training_id = training_record['id']
                        master_data_by_training[training_id][month] += record['attendance_count']
                        matched = True
                        break

                if not matched:
                    unmatched_records.append(record['training_name'])

            # Log unmatched records with optional fuzzy match suggestions
            if unmatched_records:
                unmatched_list = list(set(unmatched_records))
                print(f"Warning: {len(unmatched_list)} training names from master_data could not be matched:")
                for name in unmatched_list[:10]:
                    print(f"  - {name}")
                    # Suggest close matches
                    potential = get_close_matches(normalize_training_name(name),
                                                 [normalize_training_name(t['training_name']) for t in target_trainings],
                                                 n=2, cutoff=0.7)
                    if potential:
                        print(f"    -> Potential match(es): {', '.join(potential)}")
                if len(unmatched_list) > 10:
                    print(f"  ... and {len(unmatched_list) - 10} more")

            # Update training_targets table
            month_index = get_month_index()
            for training_id, month_counts in master_data_by_training.items():
                training_record = next((t for t in target_trainings if t['id'] == training_id), None)
                if not training_record:
                    continue

                ytd_actual = sum(month_counts.values())
                target = training_record['target'] or 0
                batch_size = training_record['batch_size'] or 0
                balance = max(target - ytd_actual, 0) if target else 0
                programs_to_run = round(balance / batch_size, 1) if batch_size > 0 else 0
                ytd_target = (target // 10) * month_index if target else 0

                update_query = """
                    UPDATE training_targets
                    SET 
                        ytd_actual=%s, balance=%s, programs_to_run=%s, ytd_target=%s,
                        april=%s, may=%s, june=%s, july=%s, august=%s, september=%s,
                        october=%s, november=%s, december=%s, january=%s, february=%s, march=%s
                    WHERE id=%s
                """
                params = (
                    ytd_actual, balance, programs_to_run, ytd_target,
                    month_counts.get('april', 0),
                    month_counts.get('may', 0),
                    month_counts.get('june', 0),
                    month_counts.get('july', 0),
                    month_counts.get('august', 0),
                    month_counts.get('september', 0),
                    month_counts.get('october', 0),
                    month_counts.get('november', 0),
                    month_counts.get('december', 0),
                    month_counts.get('january', 0),
                    month_counts.get('february', 0),
                    month_counts.get('march', 0),
                    training_id
                )
                cursor.execute(update_query, params)

            conn.commit()
            return True

    except Exception as e:
        print(f"Error updating training completion counts: {e}")
        conn.rollback()
        return False

def validate_training_name_mapping(conn):
    """Check for potential training name mismatches between tables"""
    try:
        with conn.cursor() as cursor:
            # Get all training names from both tables
            cursor.execute("SELECT DISTINCT training_name FROM training_names WHERE tni_status = 'TNI'")
            training_names = {normalize_training_name(row['training_name']) for row in cursor.fetchall()}
            
            cursor.execute("SELECT DISTINCT training_name FROM master_data")
            master_names = {normalize_training_name(row['training_name']) for row in cursor.fetchall()}
            
            # Find names in master_data that don't match anything in training_names
            unmatched = master_names - training_names
            
            # Try partial matching
            potential_matches = {}
            for name in unmatched:
                for target_name in training_names:
                    if (name in target_name or target_name in name) and len(name) > 3:
                        if name not in potential_matches:
                            potential_matches[name] = []
                        potential_matches[name].append(target_name)
            
            return {
                'unmatched_count': len(unmatched),
                'potential_matches': potential_matches,
                'fully_unmatched': [name for name in unmatched if name not in potential_matches]
            }
    except Exception as e:
        print(f"Error validating training name mapping: {e}")
        return None

def calculate_total_row(rows, category=None, is_category_total=False, is_grand_total=False):
    """Calculate a total row (category total or grand total) from a list of rows"""
    if not rows:
        return None

    # Sum all the numeric fields
    total = {
        'target': sum(row.get('target', 0) for row in rows),
        'batch_size': sum(row.get('batch_size', 0) for row in rows),
        'ytd_actual': sum(row.get('ytd_actual', 0) for row in rows),
        'april': sum(row.get('april', 0) for row in rows),
        'may': sum(row.get('may', 0) for row in rows),
        'june': sum(row.get('june', 0) for row in rows),
        'july': sum(row.get('july', 0) for row in rows),
        'august': sum(row.get('august', 0) for row in rows),
        'september': sum(row.get('september', 0) for row in rows),
        'october': sum(row.get('october', 0) for row in rows),
        'november': sum(row.get('november', 0) for row in rows),
        'december': sum(row.get('december', 0) for row in rows),
        'january': sum(row.get('january', 0) for row in rows),
        'february': sum(row.get('february', 0) for row in rows),
        'march': sum(row.get('march', 0) for row in rows),
    }

    # Calculate derived fields
    balance = max(total['target'] - total['ytd_actual'], 0)
    programs_to_run = round(balance / total['batch_size'], 1) if total['batch_size'] > 0 else 0
    month_index = get_month_index()
    ytd_target = (total['target'] // 10) * month_index

    # Create the row
    if is_category_total:
        training_name = f"{category} (Total)"
        pmo_category = ''
        pl_category = '—'
        is_total_row = True
        is_grand_total_flag = False
    elif is_grand_total:
        training_name = 'Grand Total'
        pmo_category = ''
        pl_category = '—'
        is_total_row = False
        is_grand_total_flag = True
    else:
        return None

    return {
        'id': None,
        'display_id': '',
        'training_name': training_name,
        'pmo_category': pmo_category,
        'pl_category': pl_category,
        'is_total_row': is_total_row,
        'is_grand_total': is_grand_total_flag,
        'target_year': rows[0].get('target_year'),
        'tni': 0,
        'target': total['target'],
        'batch_size': total['batch_size'],
        'ytd_target': ytd_target,
        'ytd_actual': total['ytd_actual'],
        'balance': balance,
        'programs_to_run': programs_to_run,
        'april': total['april'],
        'may': total['may'],
        'june': total['june'],
        'july': total['july'],
        'august': total['august'],
        'september': total['september'],
        'october': total['october'],
        'november': total['november'],
        'december': total['december'],
        'january': total['january'],
        'february': total['february'],
        'march': total['march'],
    }

def get_training_data(target_year):
    """Fetch and prepare training data for a given year, including calculated totals."""
    conn = get_db_connection()
    if not conn:
        print("❌ Failed to create database connection")
        return []
    
    try:
        with conn.cursor() as cursor:
            # Fetch rows for the selected year (excluding any old total rows)
            cursor.execute("""
                SELECT 
                    id, training_name, pmo_category, pl_category,
                    tni, batch_size, target, ytd_actual,
                    april, may, june, july, august, september,
                    october, november, december, january, february, march,
                    target_year
                FROM training_targets 
                WHERE target_year = %s 
                ORDER BY 
                    pmo_category ASC,
                    training_name ASC
            """, (target_year,))
            
            rows = cursor.fetchall()

            # Process each row to calculate derived fields
            processed_rows = []
            month_index = get_month_index()

            for row in rows:
                # Defensive defaults
                target = row.get('target') or 0
                ytd_actual = row.get('ytd_actual') or 0
                batch_size = row.get('batch_size') or 0

                # Calculations
                balance = max(target - ytd_actual, 0)
                programs_to_run = round(balance / batch_size, 1) if batch_size > 0 else 0
                ytd_target = (target // 10) * month_index

                processed_row = {
                    'id': row['id'],
                    'display_id': '',  # Will be set later
                    'training_name': row.get('training_name'),
                    'pmo_category': row.get('pmo_category'),
                    'pl_category': row.get('pl_category'),
                    'is_total_row': False,
                    'is_grand_total': False,
                    'target_year': row.get('target_year'),
                    'tni': row.get('tni') or 0,
                    'target': target,
                    'batch_size': batch_size,
                    'ytd_target': ytd_target,
                    'ytd_actual': ytd_actual,
                    'balance': balance,
                    'programs_to_run': programs_to_run,
                    'april': row.get('april') or 0,
                    'may': row.get('may') or 0,
                    'june': row.get('june') or 0,
                    'july': row.get('july') or 0,
                    'august': row.get('august') or 0,
                    'september': row.get('september') or 0,
                    'october': row.get('october') or 0,
                    'november': row.get('november') or 0,
                    'december': row.get('december') or 0,
                    'january': row.get('january') or 0,
                    'february': row.get('february') or 0,
                    'march': row.get('march') or 0,
                }
                processed_rows.append(processed_row)

            # Group by category
            categories = {}
            for row in processed_rows:
                category = row['pmo_category']
                if category not in categories:
                    categories[category] = []
                categories[category].append(row)

            # Prepare the final output
            output = []
            row_counter = 1

            # For each category in sorted order
            for category in sorted(categories.keys()):
                category_rows = categories[category]
                # Add each row in this category
                for row in category_rows:
                    row['display_id'] = row_counter
                    output.append(row)
                    row_counter += 1

                # Add category total row
                total_row = calculate_total_row(category_rows, category, is_category_total=True)
                if total_row:
                    output.append(total_row)

            # Add grand total row
            grand_total_row = calculate_total_row(processed_rows, None, is_grand_total=True)
            if grand_total_row:
                output.append(grand_total_row)

            return output

    except Exception as e:
        print(f"❌ Database error in get_training_data: {e}")
        return []

    finally:
        conn.close()

def get_year_range(start: int = 1900, end: int = 2100):
    """Return a list of years for dropdown (default: 1900–2100)."""
    return list(range(end, start - 1, -1))  # descending order for dropdown

def check_year_has_data(target_year):
    """Check if a year has any data in the database"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM training_targets WHERE target_year = %s", (target_year,))
            result = cursor.fetchone()
            return result['count'] > 0
    except Exception:
        return False
    finally:
        conn.close()

@target_bp.route('/edit', methods=['GET', 'POST'])
def edit_data():
    current_year = datetime.now().year
    target_year = request.args.get('target_year', default=current_year, type=int)

    # Get available years from database
    available_years = get_available_years()

    # Always include current year
    if current_year not in available_years:
        available_years.append(current_year)

    # Build full year range for dropdown (1900–2100)
    year_range = get_year_range()

    if request.method == 'POST':
        # Handle form submission for updates
        data = request.form
        try:
            target_year = int(data.get('target_year'))
        except (ValueError, TypeError):
            target_year = current_year
        
        conn = get_db_connection()
        if not conn:
            flash('Database connection error', 'error')
            return redirect(url_for('target.dashboard'))

        try:
            with conn.cursor() as cursor:
                training_ids = []
                for key in data.keys():
                    if (key.startswith('target_') or key.startswith('batch_size_')) and key.split('_')[-1].isdigit():
                        training_id = key.split('_')[-1]
                        if training_id not in training_ids:
                            training_ids.append(training_id)

                for training_id in training_ids:
                    def get_int_value(field_name):
                        try:
                            value = data.get(f'{field_name}_{training_id}')
                            return int(value) if value else 0
                        except (ValueError, TypeError):
                            return 0
                    
                    target = get_int_value('target')
                    batch_size = get_int_value('batch_size')
                    ytd_actual = get_int_value('ytd_actual')

                    balance = max(target - ytd_actual, 0)
                    programs_to_run = round(balance / batch_size, 1) if batch_size > 0 else 0
                    month_index = get_month_index()
                    ytd_target = (target // 10) * month_index

                    cursor.execute("""
                        UPDATE training_targets SET
                            target = %s,
                            batch_size = %s,
                            ytd_target = %s,
                            ytd_actual = %s,
                            balance = %s,
                            programs_to_run = %s
                        WHERE id = %s AND target_year = %s
                    """, (target, batch_size, ytd_target, ytd_actual, balance, programs_to_run, training_id, target_year))

                conn.commit()
                flash('Data updated successfully', 'success')
                return redirect(url_for('target.dashboard', target_year=target_year))

        except Exception as e:
            conn.rollback()
            flash(f'Database error: {str(e)}', 'error')
            return redirect(url_for('target.edit_data', target_year=target_year))
        finally:
            conn.close()

    else:
        conn = get_db_connection()
        if not conn:
            flash('Database connection error', 'error')
            return redirect(url_for('target.dashboard'))

        try:
            # Check if year exists, initialize if not
            if target_year not in available_years:
                source_year = max(available_years) if available_years else current_year
                if initialize_new_year(target_year, conn, source_year=source_year):
                    flash(f'Initialized year {target_year} from {source_year}', 'info')
                    if target_year not in available_years:
                        available_years.append(target_year)
                        available_years.sort(reverse=True)
                else:
                    flash('Failed to initialize year structure', 'error')
                    target_year = current_year
            
            # Only sync training data if the year was just initialized
            if target_year not in get_available_years():
                sync_training_data_from_master(target_year, conn)
            
            trainings = get_training_data(target_year)
            has_data = check_year_has_data(target_year)

            return render_template('admin/tni_edit.html',
                                   trainings=trainings,
                                   current_year=current_year,
                                   target_year=target_year,
                                   available_years=available_years,
                                   year_range=year_range,
                                   has_data=has_data)
        except Exception as e:
            flash('Database error occurred', 'error')
            return redirect(url_for('target.dashboard'))
        finally:
            conn.close()

@target_bp.route('/')
def dashboard():
    current_year = datetime.now().year
    target_year = request.args.get('target_year', default=current_year, type=int)

    # Get available years from DB + include current year
    available_years = get_available_years()
    if current_year not in available_years:
        available_years.append(current_year)

    # Build full year range for dropdown (1900–2100)
    year_range = get_year_range()

    if target_year == current_year:
        conn = get_db_connection()
        if conn:
            try:
                update_training_completion_counts(conn)
            except Exception as e:
                print(f"Error updating completion counts: {e}")
            finally:
                conn.close()

    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return render_template('admin/tni_dashboard.html',
                               trainings=[],
                               current_year=current_year,
                               target_year=target_year,
                               available_years=available_years,
                               year_range=year_range)

    try:
        if target_year not in available_years:
            source_year = max(available_years) if available_years else current_year
            if initialize_new_year(target_year, conn, source_year=source_year):
                flash(f'Initialized year {target_year} from {source_year}', 'info')
                if target_year not in available_years:
                    available_years.append(target_year)
                    available_years.sort(reverse=True)
            else:
                flash('Failed to initialize year structure', 'error')
                target_year = current_year

        # Only sync training data if the year was just initialized
        if target_year not in get_available_years():
            sync_training_data_from_master(target_year, conn)
        
        trainings = get_training_data(target_year)

        return render_template('admin/tni_dashboard.html',
                               trainings=trainings,
                               current_year=current_year,
                               target_year=target_year,
                               available_years=available_years,
                               year_range=year_range)

    except Exception as e:
        flash('Database error occurred', 'error')
        return render_template('admin/tni_dashboard.html',
                               trainings=[],
                               current_year=current_year,
                               target_year=current_year,
                               available_years=available_years,
                               year_range=year_range)
    finally:
        conn.close()

@target_bp.route('/sync_training_data', methods=['POST'])
def sync_training_data():
    """Sync training data from training_names table to training_targets"""
    target_year = request.form.get('target_year', type=int)
    if not target_year:
        target_year = datetime.now().year
    
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('target.edit_data', target_year=target_year))
    
    try:
        success, message = sync_training_data_from_master(target_year, conn)
        if success:
            flash(message, 'success')
        else:
            flash(message, 'error')
    except Exception as e:
        flash(f'Error syncing training data: {str(e)}', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('target.edit_data', target_year=target_year))

@target_bp.route('/update_completion_counts', methods=['POST'])
def update_completion_counts():
    training_name = request.form.get('training_name')
    tni_status = request.form.get('tni_status', 'All')
    
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('target.dashboard'))
    
    try:
        if update_training_completion_counts(conn, training_name, tni_status):
            flash('Successfully updated training completion counts', 'success')
        else:
            flash('No matching records found or error occurred', 'warning')
    except Exception as e:
        flash(f'Error updating completion counts: {str(e)}', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('target.dashboard'))

@target_bp.route('/validate_training_names', methods=['GET'])
def validate_training_names():
    """Validate training names between master_data and training_names tables"""
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('target.dashboard'))
    
    try:
        validation_results = validate_training_name_mapping(conn)
        if validation_results is None:
            flash('Error validating training names', 'error')
            return redirect(url_for('target.dashboard'))
        
        return render_template('admin/validate_training_names.html',
                               validation_results=validation_results)
    except Exception as e:
        flash(f'Error validating training names: {str(e)}', 'error')
        return redirect(url_for('target.dashboard'))
    finally:
        conn.close()

@target_bp.errorhandler(405)
def method_not_allowed(e):
    flash('Invalid request method - please use the provided forms', 'error')
    return redirect(url_for('target.dashboard'))

@target_bp.route('/initialize_year', methods=['POST'])
def initialize_year():
    """Initialize a new year with current training data"""
    target_year = request.form.get('target_year', type=int)
    if not target_year:
        flash('Invalid year specified', 'error')
        return redirect(url_for('target.edit_data'))
    
    conn = get_db_connection()
    if not conn:
        flash('Database connection error', 'error')
        return redirect(url_for('target.edit_data', target_year=target_year))
    
    try:
        # Initialize the year structure
        if initialize_new_year(target_year, conn):
            # Sync with current training names
            success, message = sync_training_data_from_master(target_year, conn)
            if success:
                flash(f'Successfully initialized {target_year} with current training list', 'success')
            else:
                flash(f'Initialized {target_year} but could not sync training data: {message}', 'warning')
        else:
            flash('Failed to initialize year', 'error')
    except Exception as e:
        flash(f'Error initializing year: {str(e)}', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('target.edit_data', target_year=target_year))