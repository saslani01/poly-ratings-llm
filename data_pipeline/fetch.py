#!/usr/bin/env python3
"""
Fetch professor data from PolyRatings API
Usage: python -m data_pipeline.fetch [--reviews | --prof <id> | --overview | --index | --update]
    --overview      # Get professors overview (fast)
    --reviews       # Get all review data (slow)
    --prof abc123   # Get specific professor
    --index         # Process raw data into searchable indexes
    --update        # Fetch all reviews then create indexes (full update)
"""

import json
import requests
import urllib.parse
from datetime import datetime
from pathlib import Path
import time
import argparse
from collections import defaultdict
import re

class PolyRatingsFetcher:
    def __init__(self):
        self.base_url = "https://api-prod.polyratings.org"
        self.data_dir = Path("data")
        self.raw_dir = Path("data/raw")
        self.processed_dir = Path("data/processed")
        self.delay = 0.5
        self.max_retries = 3
        
        # Create directories
        self.setup_directories()
    
    def setup_directories(self):
        """Create necessary directories"""
        directories = [
            self.data_dir,
            self.raw_dir,
            self.raw_dir / "professors",
            self.processed_dir,
            self.processed_dir / "stats"
        ]
        
        for dir_path in directories:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        print("✓ Directory structure created")
    
    def log_message(self, message, level="INFO"):
        """Simple logging"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {level}: {message}")
    
    def get_professors_overview(self):
        """Get overview of all professors (metadata only)"""
        url = f"{self.base_url}/professors.all"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Save with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = self.raw_dir / f"professors_overview_{timestamp}.json"
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            professors = data.get('result', {}).get('data', [])
            self.log_message(f"Retrieved overview of {len(professors)} professors")
            return data
            
        except requests.exceptions.RequestException as e:
            self.log_message(f"Error fetching professors overview: {e}", "ERROR")
            return None
    
    def get_professor_details(self, professor_id, retry_count=0):
        """Get detailed data for specific professor (includes reviews)"""
        input_data = {"id": professor_id}
        input_encoded = urllib.parse.quote(json.dumps(input_data))
        url = f"{self.base_url}/professors.get?input={input_encoded}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Save individual professor file
            prof_file = self.raw_dir / "professors" / f"{professor_id}.json"
            with open(prof_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Log review count if available
            prof_data = data.get('result', {}).get('data', {})
            review_count = prof_data.get('numEvals', 0)
            prof_name = f"{prof_data.get('firstName', '')} {prof_data.get('lastName', '')}"
            self.log_message(f"Fetched {prof_name} ({review_count} reviews)")
            
            return data
            
        except requests.exceptions.RequestException as e:
            if retry_count < self.max_retries:
                self.log_message(f"Retrying professor {professor_id} (attempt {retry_count + 1})", "WARN")
                time.sleep(self.delay * 2)
                return self.get_professor_details(professor_id, retry_count + 1)
            else:
                self.log_message(f"Failed to fetch professor {professor_id}: {e}", "ERROR")
                return None
    
    def fetch_all_reviews(self):
        """Fetch complete data for all professors (including reviews)"""
        self.log_message("Starting complete data fetch...")
        
        # First get overview of all professors
        professors_overview = self.get_professors_overview()
        if not professors_overview:
            self.log_message("Failed to get professors overview", "ERROR")
            return False
        
        # Extract professor IDs
        professors = professors_overview.get('result', {}).get('data', [])
        professor_ids = [prof.get('id') for prof in professors if prof.get('id')]
        
        if not professor_ids:
            self.log_message("No professor IDs found", "ERROR")
            return False
        
        self.log_message(f"Found {len(professor_ids)} professors to fetch")
        
        success_count = 0
        failed_ids = []
        total_reviews = 0
        
        # Fetch each professor's detailed data
        for i, prof_id in enumerate(professor_ids):
            self.log_message(f"Progress: {i+1}/{len(professor_ids)} - Fetching {prof_id}")
            
            prof_data = self.get_professor_details(prof_id)
            if prof_data:
                success_count += 1
                # Count reviews
                num_evals = prof_data.get('result', {}).get('data', {}).get('numEvals', 0)
                total_reviews += num_evals
            else:
                failed_ids.append(prof_id)
            
            # Rate limiting - be nice to the API
            if i < len(professor_ids) - 1:
                time.sleep(self.delay)
        
        # Summary
        self.log_message(f"Fetch complete!")
        self.log_message(f"Successful: {success_count}/{len(professor_ids)} professors")
        self.log_message(f"Total reviews collected: {total_reviews}")
        
        if failed_ids:
            self.log_message(f"Failed professor IDs: {failed_ids}", "WARN")
        
        return success_count > 0

    def process_raw_data(self):
        """Process raw professor data into searchable indexes"""
        self.log_message("Starting data indexing...")
        
        # Check if raw data exists
        prof_dir = self.raw_dir / "professors"
        if not prof_dir.exists():
            self.log_message("No raw professor data found. Run --reviews first.", "ERROR")
            return False
        
        # Get all professor files
        prof_files = list(prof_dir.glob("*.json"))
        if not prof_files:
            self.log_message("No professor JSON files found", "ERROR")
            return False
        
        self.log_message(f"Processing {len(prof_files)} professor files...")
        
        # Initialize indexes
        course_index = defaultdict(list)
        department_index = defaultdict(lambda: {
            'professors': [],
            'courses': set(),
            'total_reviews': 0,
            'avg_rating': 0
        })
        professor_index = {}
        all_reviews = []
        
        total_professors = 0
        total_reviews_count = 0
        
        # Process each professor file
        for i, prof_file in enumerate(prof_files):
            if i % 100 == 0:
                self.log_message(f"Processing file {i+1}/{len(prof_files)}")
            
            try:
                with open(prof_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                prof_data = data.get('result', {}).get('data', {})
                if not prof_data or not prof_data.get('id'):
                    continue
                
                prof_id = prof_data['id']
                first_name = prof_data.get('firstName', '')
                last_name = prof_data.get('lastName', '')
                prof_name = f"{first_name} {last_name}".strip()
                department = prof_data.get('department', 'Unknown')
                
                # Create key for professor index (first_last)
                prof_key = f"{first_name}_{last_name}".replace(' ', '_').replace('.', '').replace(',', '').lower()
                
                # Add to professor index
                professor_summary = {
                    'id': prof_id,
                    'name': prof_name,
                    'firstName': first_name,
                    'lastName': last_name,
                    'department': department,
                    'overallRating': prof_data.get('overallRating', 0),
                    'numEvals': prof_data.get('numEvals', 0),
                    'materialClear': prof_data.get('materialClear', 0),
                    'studentDifficulties': prof_data.get('studentDifficulties', 0),
                    'courses': prof_data.get('courses', []),
                    'reviews': prof_data.get('reviews', {})
                }
                professor_index[prof_key] = professor_summary
                
                total_professors += 1
                
                # Process reviews by course
                reviews_data = prof_data.get('reviews', {})
                for course_code, course_reviews in reviews_data.items():
                    # Clean course code
                    course_code = course_code.strip().upper()
                    
                    # Add to course index
                    course_entry = {
                        'professorId': prof_id,
                        'professorName': prof_name,
                        'department': department,
                        'professorRating': prof_data.get('overallRating', 0),
                        'reviews': course_reviews
                    }
                    course_index[course_code].append(course_entry)
                    
                    # Add to department tracking
                    dept_info = department_index[department]
                    if prof_id not in [p['id'] for p in dept_info['professors']]:
                        dept_info['professors'].append({
                            'id': prof_id,
                            'name': prof_name,
                            'rating': prof_data.get('overallRating', 0)
                        })
                    dept_info['courses'].add(course_code)
                    dept_info['total_reviews'] += len(course_reviews)
                    
                    # Flatten reviews for master list
                    for review in course_reviews:
                        flattened_review = {
                            'reviewId': review.get('id'),
                            'professorId': prof_id,
                            'professorName': prof_name,
                            'department': department,
                            'course': course_code,
                            'grade': review.get('grade'),
                            'gradeLevel': review.get('gradeLevel'),
                            'courseType': review.get('courseType'),
                            'overallRating': review.get('overallRating', 0),
                            'materialClarity': review.get('presentsMaterialClearly', 0),
                            'recognizesStudentDifficulties': review.get('recognizesStudentDifficulties', 0),
                            'reviewText': review.get('rating', ''),
                            'postDate': review.get('postDate')
                        }
                        all_reviews.append(flattened_review)
                        total_reviews_count += 1
                
            except Exception as e:
                self.log_message(f"Error processing {prof_file}: {e}", "WARN")
                continue
        
        # Convert sets to lists and calculate department averages
        for dept, info in department_index.items():
            info['courses'] = sorted(list(info['courses']))
            if info['professors']:
                ratings = [p['rating'] for p in info['professors'] if p['rating'] > 0]
                info['avg_rating'] = sum(ratings) / len(ratings) if ratings else 0
        
        # Save processed indexes
        self.log_message("Saving indexes...")
        
        # Course index - for "CPS 349" lookups
        course_file = self.processed_dir / "course_index.json"
        with open(course_file, 'w', encoding='utf-8') as f:
            json.dump(dict(course_index), f, indent=2, ensure_ascii=False)
        
        # Department index - for "CPS department" lookups
        dept_file = self.processed_dir / "department_index.json"
        with open(dept_file, 'w', encoding='utf-8') as f:
            json.dump(dict(department_index), f, indent=2, ensure_ascii=False)
        
        # Professor index - keyed by first_last
        prof_file = self.processed_dir / "professor_index.json"
        with open(prof_file, 'w', encoding='utf-8') as f:
            json.dump(professor_index, f, indent=2, ensure_ascii=False)
        
        # Create summary stats
        stats = {
            'total_professors': total_professors,
            'total_reviews': total_reviews_count,
            'total_courses': len(course_index),
            'total_departments': len(department_index),
            'indexing_date': datetime.now().isoformat(),
            'departments': list(department_index.keys())
        }
        
        # Save stats in stats folder
        stats_file = self.processed_dir / "stats" / "indexing_stats.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        # Success message
        self.log_message("✓ Indexing complete!")
        self.log_message(f"Processed {total_professors} professors")
        self.log_message(f"Processed {total_reviews_count} reviews")
        self.log_message(f"Found {len(course_index)} unique courses")
        self.log_message(f"Found {len(department_index)} departments")
        self.log_message(f"Files saved to: {self.processed_dir}")
        self.log_message(f"Stats saved to: {self.processed_dir}/stats/")
        
        return True

    def full_update(self):
        """Fetch all reviews then create indexes"""
        self.log_message("Starting full update (fetch + index)...")
        
        # Step 1: Fetch all reviews
        success = self.fetch_all_reviews()
        if not success:
            self.log_message("Failed to fetch reviews, aborting update", "ERROR")
            return False
        
        self.log_message("Reviews fetched successfully, starting indexing...")
        
        # Step 2: Process data into indexes
        success = self.process_raw_data()
        if not success:
            self.log_message("Failed to create indexes", "ERROR")
            return False
        
        self.log_message("✓ Full update complete!")
        return True

def main():
    """CLI interface for fetcher"""
    parser = argparse.ArgumentParser(
        description="Fetch PolyRatings professor data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m data_pipeline.fetch --overview      # Get professors overview (fast)
  python -m data_pipeline.fetch --reviews       # Get all review data (slow)
  python -m data_pipeline.fetch --prof abc123   # Get specific professor
  python -m data_pipeline.fetch --index         # Process raw data into indexes
  python -m data_pipeline.fetch --update        # Fetch all reviews then create indexes
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--reviews', action='store_true', 
                      help='Fetch complete data for all professors (includes reviews)')
    group.add_argument('--prof', type=str, metavar='ID',
                      help='Fetch specific professor by ID')
    group.add_argument('--overview', action='store_true',
                      help='Fetch professors overview only (metadata, no reviews)')
    group.add_argument('--index', action='store_true',
                      help='Process raw data into searchable indexes')
    group.add_argument('--update', action='store_true',
                      help='Fetch all reviews then create indexes (full update)')
    
    args = parser.parse_args()
    
    fetcher = PolyRatingsFetcher()
    
    if args.reviews:
        success = fetcher.fetch_all_reviews()
        if success:
            print("\n✓ Review data fetch completed successfully!")
            print("Next step: python -m data_pipeline.fetch --index")
        else:
            print("\n✗ Fetch failed. Check the logs above.")
    
    elif args.prof:
        result = fetcher.get_professor_details(args.prof)
        if result:
            print(f"✓ Successfully fetched professor {args.prof}")
        else:
            print(f"✗ Failed to fetch professor {args.prof}")
    
    elif args.overview:
        result = fetcher.get_professors_overview()
        if result:
            professors = result.get('result', {}).get('data', [])
            print(f"✓ Successfully fetched overview of {len(professors)} professors")
        else:
            print("✗ Failed to fetch professors overview")
    
    elif args.index:
        success = fetcher.process_raw_data()
        if success:
            print("\n✓ Data indexing completed successfully!")
            print("Files available in data/processed/:")
            print("- course_index.json: Search by course code")
            print("- department_index.json: Search by department")
            print("- professor_index.json: Search by professor (keyed by first_last)")
            print("- stats/indexing_stats.json: Processing statistics")
        else:
            print("\n✗ Indexing failed. Check the logs above.")
    
    elif args.update:
        success = fetcher.full_update()
        if success:
            print("\n✓ Full update completed successfully!")
            print("All data fetched and indexed. Files available in data/processed/:")
            print("- course_index.json: Search by course code")
            print("- department_index.json: Search by department")
            print("- professor_index.json: Search by professor (keyed by first_last)")
            print("- stats/indexing_stats.json: Processing statistics")
        else:
            print("\n✗ Update failed. Check the logs above.")

if __name__ == "__main__":
    main()