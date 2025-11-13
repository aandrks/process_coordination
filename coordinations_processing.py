import streamlit as st
import re
import json
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
import os
import traceback
import unicodedata
from rapidfuzz import fuzz, process
import xlsxwriter
import io

# Configuration
EMPLOYEE_DB_FILE = 'employee_database.json'
public_domains = {'mail', 'yandex', 'gmail', 'yahoo', 'hotmail', 'outlook'}
no_match_array = []
holidays = ['01-01', '02-01', '03-01', '04-01', '05-01', '06-01', '07-01', '23-02', '08-03', '01-05', '09-05', '12-06',
            '03-11', '04-11']
working_holidays = ['01-11']

# Initialize session state
if 'employee_db' not in st.session_state:
    st.session_state.employee_db = {'employees': [], 'companies': set()}
if 'processing_results' not in st.session_state:
    st.session_state.processing_results = None


def load_employee_db():
    """Load employee database from file"""
    try:
        if Path(EMPLOYEE_DB_FILE).exists():
            with open(EMPLOYEE_DB_FILE, 'r', encoding='utf-8') as f:
                db = json.load(f)
                db['companies'] = set(db['companies'])
                st.session_state.employee_db = db
                return db
    except Exception as e:
        st.error(f"⚠️ Error loading employee database: {e}")
    return {'employees': [], 'companies': set()}


def save_employee_db(db):
    """Save employee database to file"""
    try:
        db_to_save = {
            'employees': db['employees'],
            'companies': list(db['companies'])
        }
        with open(EMPLOYEE_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(db_to_save, f, ensure_ascii=False, indent=2)
        st.session_state.employee_db = db
        return True
    except Exception as e:
        st.error(f"⚠️ Error saving employee database: {e}")
        return False


def normalize_text(text):
    """Normalize text by removing accents, special characters, and converting to lowercase"""
    if not isinstance(text, str):
        return ""

    text = unicodedata.normalize('NFKD', text)
    text = ''.join([c for c in text if not unicodedata.combining(c)])
    text = re.sub(r'[^\w\s.]', '', text)
    return text.lower().strip()


def is_initial(part):
    """Check if a name part is an initial"""
    return len(part) <= 2 or (len(part) == 2 and part.endswith('.'))


def extract_name_components(name):
    """Extract surname and given names from a full name with proper handling of initials"""
    if not isinstance(name, str):
        return "", ""

    clean_name = re.sub(r'[^а-яА-ЯёЁa-zA-Z\s.]', '', name).strip()

    if ',' in clean_name:
        parts = [p.strip() for p in clean_name.split(',')]
        if len(parts) >= 2:
            return parts[0], parts[1]

    parts = [p for p in re.split(r'\s+', clean_name) if p]

    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""

    if is_initial(parts[-1]):
        return parts[0], parts[-1]

    if is_initial(parts[0]):
        return parts[-1], parts[0]

    return parts[-1], " ".join(parts[:-1])


def parse_company_person_data(uploaded_file, db):
    """Process company and employee data with enhanced name handling and team support"""
    company_person_map = defaultdict(list)
    new_employees = []
    seen_emails = {e['email'] for e in db['employees']}
    processing_log = []
    team_id_counter = 1

    # Read uploaded file
    if uploaded_file.name.endswith('.csv'):
        df = pd.read_csv(uploaded_file, sep=';', encoding='utf-8')
    else:
        df = pd.read_excel(uploaded_file, engine='openpyxl')

    file_content = '\n'.join(df.astype(str).values.flatten().tolist())

    lines = file_content.split('\n')
    combined_lines = []
    current_combined_line = ""

    for line in lines:
        line = line.strip()
        if not line:
            if current_combined_line:
                combined_lines.append(current_combined_line)
                current_combined_line = ""
            continue

        if current_combined_line:
            if current_combined_line.endswith('/'):
                current_combined_line = current_combined_line.rstrip('/') + ' ' + line
            else:
                combined_lines.append(current_combined_line)
                current_combined_line = line
        else:
            current_combined_line = line

    if current_combined_line:
        combined_lines.append(current_combined_line)

    manual_assignments = {}

    for line_num, line in enumerate(combined_lines, 1):
        if not line.strip():
            continue

        for block in re.findall(r'(?:\(| - )([^()]+?\s+[^\s@]+@[^\s/@]+(?:\s*/\s*[^()]+?\s+[^\s@]+@[^\s/@]+)*)', line):
            if '@' not in block:
                continue

            team_members = [p.strip() for p in block.split('/')]
            team_id = f"team_{team_id_counter}"
            team_id_counter += 1

            team_company = None
            team_emails = []

            for person in team_members:
                match = re.search(r'([^@]+)\s+([^\s@]+@[^\s@]+)', person)
                if not match:
                    continue

                name, email = match.group(1).strip(), match.group(2).strip()
                email = re.sub(r'[),.;]+$', '', email).strip()

                if email in seen_emails:
                    continue
                seen_emails.add(email)
                team_emails.append(email)

                domain = email.split('@')[-1].split('.')[0]
                surname, given_names = extract_name_components(name)
                normalized_name = normalize_text(name)

                if domain in public_domains:
                    # Store for manual assignment later
                    manual_assignments[email] = {
                        'name': name, 'email': email, 'normalized_name': normalized_name,
                        'surname': surname, 'given_names': given_names,
                        'team_id': team_id, 'team_emails': team_emails,
                        'line': line
                    }
                else:
                    if team_company is None:
                        team_company = domain
                        db['companies'].add(domain)

                    new_employees.append({
                        'name': name, 'email': email, 'normalized_name': normalized_name,
                        'surname': surname, 'given_names': given_names,
                        'company': domain, 'source': 'auto',
                        'team_id': team_id, 'team_emails': team_emails
                    })
                    company_person_map[domain].append({
                        'name': name, 'email': email, 'normalized_name': normalized_name,
                        'surname': surname, 'given_names': given_names,
                        'team_id': team_id, 'team_emails': team_emails
                    })

    # Handle manual assignments
    if manual_assignments:
        st.subheader("Manual Company Assignment Required")
        for email, data in manual_assignments.items():
            st.write(f"**Employee:** {data['name']} <{data['email']}>")
            st.write(f"**From line:** {data['line']}")

            company = st.text_input(f"Assign company for {data['name']}:", key=f"company_{email}")
            if company and len(company) >= 2:
                if team_company is None:
                    team_company = company
                    db['companies'].add(company)

                new_employees.append({
                    'name': data['name'], 'email': data['email'],
                    'normalized_name': data['normalized_name'],
                    'surname': data['surname'], 'given_names': data['given_names'],
                    'company': company, 'source': 'manual',
                    'team_id': data['team_id'], 'team_emails': data['team_emails']
                })
                company_person_map[company].append({
                    'name': data['name'], 'email': data['email'],
                    'normalized_name': data['normalized_name'],
                    'surname': data['surname'], 'given_names': data['given_names'],
                    'team_id': data['team_id'], 'team_emails': data['team_emails']
                })

    db['employees'].extend(new_employees)
    save_employee_db(db)

    return db, company_person_map


def find_best_match(target_name, candidates, debug_info=None):
    """Find the best match using fuzzy matching on full name"""
    if debug_info is None:
        debug_info = []

    target_surname, target_given = extract_name_components(target_name)
    target_possible_givens = [target_given]

    for candidate in candidates:
        if normalize_text(target_surname) != normalize_text(candidate['surname']):
            continue

        candidate_given_norm = normalize_text(candidate['given_names'])
        for possible_given in target_possible_givens:
            possible_given_norm = normalize_text(possible_given)
            if (candidate_given_norm.startswith(possible_given_norm) or
                    possible_given_norm.startswith(candidate_given_norm)):
                return candidate

    best_score = 0
    best_match = None
    for candidate in candidates:
        score = fuzz.token_set_ratio(normalize_text(target_name), candidate['normalized_name'])
        if score > 65 and score > best_score:
            best_score = score
            best_match = candidate

    return best_match


def add_working_days(start_date, working_days):
    """Add working days excluding weekends"""
    if working_days <= 0:
        return start_date

    current_date = start_date
    days_added = 0

    while days_added < working_days:
        current_date += timedelta(days=1)
        monthday = current_date.strftime("%d-%m")
        if (current_date.weekday() < 5) and (monthday not in holidays):
            days_added += 1
        elif monthday in working_holidays:
            days_added += 1

    return current_date


def load_spec_config():
    """Load specification configuration"""
    return {
        "2": {"раздела КР": 2},
        "3": {},
        "4": {}
    }


def get_working_days(step_text, workflow_text):
    """Calculate working days based on stage and specification keywords"""
    if "Утверждение" in step_text:
        return (2, 4, "Stage 4")

    stage_match = re.search(r'Шаг (\d+)', step_text)
    if not stage_match:
        return (0, 0, "No stage number found")

    step_number = int(stage_match.group(1))
    stage_number = step_number + 1

    spec_config = load_spec_config()
    default_days = {2: 3, 3: 5, 4: 2}

    if stage_number not in spec_config:
        days = default_days.get(stage_number, 0)
        return (days, stage_number, f"Stage {stage_number}: not configured, using default {days} days")

    stage_keywords = spec_config[stage_number]
    workflow_lower = workflow_text.lower()

    for keyword, days in stage_keywords.items():
        if keyword.lower() in workflow_lower:
            return (days, stage_number, f"Stage {stage_number}: keyword '{keyword}' → {days} days")

    days = default_days.get(stage_number, 0)
    return (days, stage_number, f"Stage {stage_number}: no keywords found, using default {days} days")


def extract_start_date_from_lifecycle(lifecycle_text, current_step_number):
    """Extract start date from lifecycle text"""
    if not lifecycle_text or pd.isna(lifecycle_text):
        return None

    current_stage = current_step_number + 1
    target_step = current_stage - 2

    if target_step < 0:
        return None

    step_pattern = rf'Шаг {target_step}.*?(\d{{2}}\.\d{{2}}\.\d{{2}} \d{{2}}:\d{{2}})'
    matches = re.findall(step_pattern, lifecycle_text, re.IGNORECASE | re.DOTALL)

    if matches:
        last_date_str = matches[-1]
        try:
            return datetime.strptime(last_date_str, '%d.%m.%y %H:%M')
        except ValueError:
            return None

    return None


def is_team_checked(approver_name, all_people, checked_approvers, matching_log):
    """Check if any team member is already checked"""
    best_match = find_best_match(approver_name, all_people, matching_log)
    if not best_match:
        return False

    team_id = best_match.get('team_id')
    team_emails = best_match.get('team_emails', [])

    if not team_id or len(team_emails) <= 1:
        return False

    team_members = []
    for person in all_people:
        if person.get('team_id') == team_id:
            team_members.append(person)

    for team_member in team_members:
        for checked_name in checked_approvers:
            if find_best_match(checked_name, [team_member], matching_log):
                return True

    return False


def process_coordinations(df, company_person_map, selected_date):
    """Process coordinations with disambiguation"""
    overdue_counts = defaultdict(int)
    overdue_emails = []
    overdue_coordination_ids = []
    coordination_details = []
    today = selected_date
    debug_info = []
    matching_log = []

    all_people = []
    for company, persons in company_person_map.items():
        for person in persons:
            person['company'] = company
            all_people.append(person)

    id_column = df.columns[0] if len(df.columns) > 0 else 'id'

    for idx, row in df.iterrows():
        if not all(col in row for col in ['Не проверили на текущем шаге', 'Шаг', 'Рабочий процесс']):
            continue

        coord_id = row.get(id_column, 'N/A')
        step_text = str(row['Шаг'])
        workflow_text = str(row['Рабочий процесс'])

        working_days, stage_number, days_explanation = get_working_days(step_text, workflow_text)

        try:
            start_date = None
            if 'Жизненный цикл' in row and row['Жизненный цикл']:
                lifecycle_text = str(row['Жизненный цикл'])
                if "Утверждение" in step_text:
                    current_step_number = 3
                    start_date = extract_start_date_from_lifecycle(lifecycle_text, current_step_number)
                else:
                    step_match = re.search(r'Шаг (\d+)', step_text)
                    if step_match:
                        current_step_number = int(step_match.group(1))
                        start_date = extract_start_date_from_lifecycle(lifecycle_text, current_step_number)

            if start_date is None:
                start_date_str = str(row['Дата и время создания согласования'])
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')

            deadline = add_working_days(start_date, working_days)

            if deadline.date() >= today:
                continue

        except Exception as e:
            continue

        not_checked_text = str(row['Не проверили на текущем шаге'])
        not_checked_approvers = [name.strip() for name in not_checked_text.split(',') if name.strip()]

        checked_text = str(row['Проверили на текущем шаге'])
        checked_approvers = [name.strip() for name in checked_text.split(',') if name.strip()]

        coord_emails = []
        coord_companies = set()

        for approver_name in not_checked_approvers:
            if is_team_checked(approver_name, all_people, checked_approvers, matching_log):
                continue

            best_match = find_best_match(approver_name, all_people, matching_log)
            if best_match:
                coord_emails.append(best_match['email'])
                coord_companies.add(best_match['company'])
            else:
                no_match_array.append(approver_name)

        for company in coord_companies:
            overdue_counts[company] += 1
        overdue_emails.extend(coord_emails)
        overdue_coordination_ids.append(coord_id)

        coordination_details.append({
            'id': coord_id, 'company': ', '.join(coord_companies),
            'start_date': start_date.date(), 'deadline': deadline.date(),
            'working_days': working_days, 'not_checked_count': len(not_checked_approvers),
            'explanation': days_explanation, 'emails': coord_emails
        })

    return overdue_counts, overdue_emails, overdue_coordination_ids, coordination_details


def main():
    st.set_page_config(page_title="Coordination Processing System", layout="wide")
    st.title("🎯 Coordination Processing System")

    # Load employee database
    db = load_employee_db()

    # Sidebar for navigation
    st.sidebar.title("Navigation")
    mode = st.sidebar.radio("Select Mode:",
                            ["Data Loading", "Data Matching", "View Database"])

    if mode == "Data Loading":
        st.header("📥 Data Loading Mode")
        st.write("Process employee data and update database")

        uploaded_file = st.file_uploader("Upload company and employee data file (CSV or Excel)",
                                         type=['csv', 'xlsx'])

        if uploaded_file and st.button("Process Employee Data"):
            with st.spinner("Processing employee data..."):
                db, company_person_map = parse_company_person_data(uploaded_file, db)
                st.success(
                    f"✅ Data loading completed! Database now contains {len(db['employees'])} employees and {len(db['companies'])} companies")

                # Show summary
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Employees", len(db['employees']))
                with col2:
                    st.metric("Total Companies", len(db['companies']))

                # Show companies
                st.subheader("Companies in Database:")
                st.write(list(db['companies']))

    elif mode == "Data Matching":
        st.header("🔍 Data Matching Mode")
        st.write("Process coordination data using existing database")

        if not db['employees']:
            st.warning("⚠️ No employee data found. Please run Data Loading mode first.")
            return

        uploaded_file = st.file_uploader("Upload coordination data file (CSV or Excel)",
                                         type=['csv', 'xlsx'])
        selected_date = st.date_input("Select reference date for overdue calculation",
                                      value=datetime.today().date())

        if uploaded_file and st.button("Process Coordinations"):
            with st.spinner("Processing coordinations..."):
                # Read coordination file
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file, sep=';', encoding='utf-8')
                else:
                    df = pd.read_excel(uploaded_file, engine='openpyxl')

                # Convert database to company_person_map
                company_person_map = defaultdict(list)
                for employee in db['employees']:
                    company = employee['company']
                    company_person_map[company].append({
                        'name': employee['name'], 'email': employee['email'],
                        'normalized_name': employee['normalized_name'],
                        'surname': employee['surname'], 'given_names': employee['given_names'],
                        'team_id': employee.get('team_id', ''),
                        'team_emails': employee.get('team_emails', [])
                    })

                # Process coordinations
                overdue_counts, overdue_emails, overdue_coordination_ids, coordination_details = process_coordinations(
                    df, company_person_map, selected_date
                )

                # Store results in session state
                st.session_state.processing_results = {
                    'overdue_counts': overdue_counts,
                    'overdue_emails': overdue_emails,
                    'overdue_coordination_ids': overdue_coordination_ids,
                    'coordination_details': coordination_details
                }

                # Display results
                st.success("✅ Data matching completed!")

                # Summary statistics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Overdue", len(overdue_coordination_ids))
                with col2:
                    st.metric("Unique Emails", len(set(overdue_emails)))
                with col3:
                    st.metric("Companies Involved", len(overdue_counts))

                # Overdue counts by company
                st.subheader("Overdue Coordination Count by Company:")
                for company, count in sorted(overdue_counts.items(), key=lambda x: x[1], reverse=True):
                    st.write(f"- **{company}**: {count}")

                # Coordination details
                st.subheader("Coordination Details:")
                if coordination_details:
                    details_df = pd.DataFrame(coordination_details)
                    st.dataframe(details_df)

                    # Download buttons
                    col1, col2 = st.columns(2)

                    with col1:
                        # Download coordination details as Excel
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            details_df.to_excel(writer, index=False, sheet_name='Coordination Details')
                        st.download_button(
                            label="📥 Download Coordination Details (Excel)",
                            data=output.getvalue(),
                            file_name="coordination_details.xlsx",
                            mime="application/vnd.ms-excel"
                        )

                    with col2:
                        # Download overdue emails
                        emails_text = "\n".join(sorted(set(overdue_emails)))
                        st.download_button(
                            label="📧 Download Overdue Emails",
                            data=emails_text,
                            file_name="overdue_emails.txt",
                            mime="text/plain"
                        )

                if no_match_array:
                    st.warning(f"⚠️ Some people were not found in data: {set(no_match_array)}")

    elif mode == "View Database":
        st.header("📊 Employee Database")

        if db['employees']:
            st.metric("Total Employees", len(db['employees']))
            st.metric("Total Companies", len(db['companies']))

            # Show employees table
            employees_df = pd.DataFrame(db['employees'])
            st.dataframe(employees_df)

            # Show companies
            st.subheader("Companies:")
            st.write(list(db['companies']))
        else:
            st.info("No employee data available. Please load data first.")


if __name__ == "__main__":
    main()