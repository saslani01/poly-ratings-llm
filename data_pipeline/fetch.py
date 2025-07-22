#!/usr/bin/env python3
"""
Fetch professor data from PolyRatings API
Usage: python -m data_pipeline.fetch [--reviews | --prof <id> | --overview]
    --overview      # Get professors overview (fast)
    --reviews       # Get all review data (slow)
    --prof abc123   # Get specific professor
"""

import json
import requests
import urllib.parse
from datetime import datetime
from pathlib import Path
import time
import argparse

class PolyRatingsFetcher:
    def __init__(self):
        self.base_url = "https://api-prod.polyratings.org"
        self.data_dir = Path("data")
        self.raw_dir = Path("data/raw")
        self.delay = 0.5
        self.max_retries = 3
        
        # Create directories
        self.setup_directories()
    
    def setup_directories(self):
        """Create necessary directories"""
        directories = [
            self.data_dir,
            self.raw_dir,
            self.raw_dir / "professors"
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
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--reviews', action='store_true', 
                      help='Fetch complete data for all professors (includes reviews)')
    group.add_argument('--prof', type=str, metavar='ID',
                      help='Fetch specific professor by ID')
    group.add_argument('--overview', action='store_true',
                      help='Fetch professors overview only (metadata, no reviews)')
    
    args = parser.parse_args()
    
    fetcher = PolyRatingsFetcher()
    
    if args.reviews:
        success = fetcher.fetch_all_reviews()
        if success:
            print("\n✓ Review data fetch completed successfully!")
            print("Next step: python -m data_pipeline.process")
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

if __name__ == "__main__":
    main()