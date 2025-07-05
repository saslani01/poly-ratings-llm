#!/usr/bin/env python3
"""
PolyRatings Data Extraction and Management System
Usage: python main.py [command] [options]

Commands:
    update --all                   Update all professors data
    update --prof <id>             Update specific professor
    update --list                  Update professors list only
    clean                          Clean and organize data
    help                           Show this help message
"""

import json
import argparse
import requests
import urllib.parse
from datetime import datetime
from pathlib import Path
import time

CONFIG = {
    "base_url": "https://api-prod.polyratings.org",
    "data_dir": "data",
    "raw_dir": "data/raw",
    "processed_dir": "data/processed",
    "logs_dir": "logs",
    "delay": 0.5,  
    "max_retries": 3
}

def setup_directories():
    """Create necessary directories"""
    dirs = [
        CONFIG["data_dir"],
        CONFIG["raw_dir"],
        CONFIG["processed_dir"],
        CONFIG["logs_dir"],
        f"{CONFIG['raw_dir']}/professors",
        f"{CONFIG['processed_dir']}/by_department",
        f"{CONFIG['processed_dir']}/by_course",
        f"{CONFIG['processed_dir']}/embeddings"
    ]
    
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    print("✓ Directory structure created")

def log_message(message, level="INFO"):
    """Log messages with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {level}: {message}"
    print(log_entry)
    
    # Also write to log file
    log_file = f"{CONFIG['logs_dir']}/extraction_{datetime.now().strftime('%Y%m%d')}.log"
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(log_entry + "\n")

def get_all_professors():
    """Get all professors from API"""
    url = f"{CONFIG['base_url']}/professors.all"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = f"{CONFIG['raw_dir']}/all_professors_{timestamp}.json"
        with open(raw_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Also save as latest
        latest_file = f"{CONFIG['raw_dir']}/all_professors_latest.json"
        with open(latest_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        log_message(f"Retrieved {len(data.get('result', {}).get('data', []))} professors")
        return data
        
    except requests.exceptions.RequestException as e:
        log_message(f"Error fetching all professors: {e}", "ERROR")
        return None

def get_professor_details(professor_id, retry_count=0):
    """Get detailed professor data by ID"""
    input_data = {"id": professor_id}
    input_encoded = urllib.parse.quote(json.dumps(input_data))
    url = f"{CONFIG['base_url']}/professors.get?input={input_encoded}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Individual professor data
        prof_file = f"{CONFIG['raw_dir']}/professors/{professor_id}.json"
        with open(prof_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        return data
        
    except requests.exceptions.RequestException as e:
        if retry_count < CONFIG['max_retries']:
            log_message(f"Retrying professor {professor_id} (attempt {retry_count + 1})", "WARN")
            time.sleep(CONFIG['delay'] * 2)
            return get_professor_details(professor_id, retry_count + 1)
        else:
            log_message(f"Failed to fetch professor {professor_id}: {e}", "ERROR")
            return None

def extract_professor_ids(all_professors_data):
    """Extract professor IDs from all professors data"""
    try:
        professors = all_professors_data.get('result', {}).get('data', [])
        ids = [prof.get('id') for prof in professors if prof.get('id')]
        
        # prof Ids
        ids_file = f"{CONFIG['processed_dir']}/professor_ids.json"
        with open(ids_file, 'w', encoding='utf-8') as f:
            json.dump(ids, f, indent=2)
        
        return ids
    except Exception as e:
        log_message(f"Error extracting professor IDs: {e}", "ERROR")
        return []

def update_all_professors():
    """Update all professors data"""
    log_message("Starting full data update...")
    
    all_profs = get_all_professors()
    if not all_profs:
        log_message("Failed to get professors list", "ERROR")
        return False
    
    professor_ids = extract_professor_ids(all_profs)
    if not professor_ids:
        log_message("No professor IDs found", "ERROR")
        return False
    
    log_message(f"Found {len(professor_ids)} professors to update")
    
    success_count = 0
    failed_ids = []
    
    for i, prof_id in enumerate(professor_ids):
        log_message(f"Processing professor {i+1}/{len(professor_ids)}: {prof_id}")
        
        prof_data = get_professor_details(prof_id)
        if prof_data:
            success_count += 1
        else:
            failed_ids.append(prof_id)
        
        if i < len(professor_ids) - 1:
            time.sleep(CONFIG['delay'])
    
    # Save update summary
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_professors": len(professor_ids),
        "successful": success_count,
        "failed": len(failed_ids),
        "failed_ids": failed_ids
    }
    
    summary_file = f"{CONFIG['logs_dir']}/update_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    
    log_message(f"Update complete: {success_count}/{len(professor_ids)} successful")
    return True

def update_single_professor(professor_id):
    """Update single professor data"""
    log_message(f"Updating professor: {professor_id}")
    
    prof_data = get_professor_details(professor_id)
    if prof_data:
        log_message(f"Successfully updated professor {professor_id}")
        return True
    else:
        log_message(f"Failed to update professor {professor_id}", "ERROR")
        return False

def clean_data():
    """Clean and organize data for processing"""
    log_message("Starting data cleaning...")
    
    # For now, just organize by department and course
    prof_files = list(Path(f"{CONFIG['raw_dir']}/professors").glob("*.json"))
    
    departments = {}
    courses = {}
    
    for prof_file in prof_files:
        try:
            with open(prof_file, 'r', encoding='utf-8') as f:
                prof_data = json.load(f)
            
            data = prof_data.get('result', {}).get('data', {})
            if not data:
                continue
            
            dept = data.get('department', 'Unknown')
            prof_courses = data.get('courses', [])
            
            # Group by department
            if dept not in departments:
                departments[dept] = []
            departments[dept].append(data)
            
            # Group by course
            for course in prof_courses:
                if course not in courses:
                    courses[course] = []
                courses[course].append(data)
                
        except Exception as e:
            log_message(f"Error processing {prof_file}: {e}", "WARN")
    
    # Save organized data
    for dept, profs in departments.items():
        dept_file = f"{CONFIG['processed_dir']}/by_department/{dept.replace('/', '_')}.json"
        with open(dept_file, 'w', encoding='utf-8') as f:
            json.dump(profs, f, indent=2, ensure_ascii=False)
    
    for course, profs in courses.items():
        course_file = f"{CONFIG['processed_dir']}/by_course/{course.replace('/', '_')}.json"
        with open(course_file, 'w', encoding='utf-8') as f:
            json.dump(profs, f, indent=2, ensure_ascii=False)
    
    log_message(f"Data organized: {len(departments)} departments, {len(courses)} courses")

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description="PolyRatings Data Extraction System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument('command', choices=['update', 'clean', 'help'])
    parser.add_argument('--all', action='store_true', help='Update all professors')
    parser.add_argument('--prof', type=str, help='Update specific professor by ID')
    parser.add_argument('--list', action='store_true', help='Update professors list only')
    
    args = parser.parse_args()
    
    if args.command == 'help':
        parser.print_help()
        return
    
    # Setup directories
    setup_directories()
    
    if args.command == 'update':
        if args.all:
            update_all_professors()
        elif args.prof:
            update_single_professor(args.prof)
        elif args.list:
            get_all_professors()
        else:
            print("Please specify --all, --prof <id>, or --list")
    
    elif args.command == 'clean':
        clean_data()

if __name__ == "__main__":
    main()