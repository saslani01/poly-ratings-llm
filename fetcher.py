#!/usr/bin/env python3
"""
Fetch professor data from PolyRatings API and store in SQLite with fuzzy name search
Usage: python fetch.py [--all | --prof <id> | --stats | --rebuild-fuzzy]
"""

import json
import requests
import urllib.parse
import sqlite3
from datetime import datetime
from pathlib import Path
import time
import argparse
          
class PolyRatingsFetcher:
    def __init__(self):
        self.base_url = "https://api-prod.polyratings.org"
        self.data_dir = Path("data")
        self.db_path = self.data_dir / "professors.db"
        self.delay = 0.5
        self.max_retries = 3
        
        self.setup_database()
    
    def setup_database(self):
        """Create data directory and initialize SQLite database"""
        self.data_dir.mkdir(exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS professors (
                id TEXT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                department TEXT,
                overall_rating REAL,
                material_clear REAL,
                student_difficulties REAL,
                num_evals INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS courses (
                code TEXT PRIMARY KEY,
                name TEXT,
                department TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS reviews (
                id TEXT PRIMARY KEY,
                professor_id TEXT,
                course_code TEXT,
                grade TEXT,
                grade_level TEXT,
                course_type TEXT,
                rating_text TEXT,
                post_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (professor_id) REFERENCES professors (id),
                FOREIGN KEY (course_code) REFERENCES courses (code)
            );
            
            CREATE TABLE IF NOT EXISTS fetch_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT,
                status TEXT,
                message TEXT,
                professor_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
                             
             CREATE TABLE IF NOT EXISTS review_chunks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    review_id TEXT,
                    aspect TEXT,
                    content TEXT,
                    sentiment TEXT,
                    tokens_used INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (review_id) REFERENCES reviews (id)
                );
                
            CREATE VIRTUAL TABLE IF NOT EXISTS professors_fts USING fts5(
                name, 
                professor_id
            );
        """)
        
        # create index/ add others later
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_review_aspect ON review_chunks(review_id, aspect);")

        conn.commit()
        conn.close()
        print("Database initialized")
    
    def update_professor_fuzzy_search(self, prof_data):
        """Update fuzzy name search table for a professor"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            prof_id = prof_data.get('id')
            full_name = f"{prof_data.get('firstName', '')} {prof_data.get('lastName', '')}".strip()
            
            # Remove existing entry
            cursor.execute("DELETE FROM professors_fts WHERE professor_id = ?", (prof_id,))
            
            # Add new entry
            cursor.execute("""
                INSERT INTO professors_fts (name, professor_id) 
                VALUES (?, ?)
            """, (full_name, prof_id))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.log_message(f"Fuzzy search update failed for {prof_id}: {e}", "WARN")
    
    def log_message(self, message, level="INFO", professor_id=None):
        """Log message to both console and database"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {level}: {message}")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO fetch_logs (action, status, message, professor_id)
            VALUES (?, ?, ?, ?)
        """, ("fetch", level, message, professor_id))
        conn.commit()
        conn.close()
    
    def get_professors_overview(self):
        """Get overview of all professors"""
        url = f"{self.base_url}/professors.all"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            professors = data.get('result', {}).get('data', [])
            self.log_message(f"Retrieved overview of {len(professors)} professors")
            return data
            
        except requests.exceptions.RequestException as e:
            self.log_message(f"Error fetching professors overview: {e}", "ERROR")
            return None
    
    def store_professor_data(self, prof_data):
        """Store professor and review data in SQLite"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO professors 
                (id, first_name, last_name, department, overall_rating, 
                 material_clear, student_difficulties, num_evals)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                prof_data.get('id'),
                prof_data.get('firstName', ''),
                prof_data.get('lastName', ''),
                prof_data.get('department', ''),
                prof_data.get('overallRating'),
                prof_data.get('materialClear'),
                prof_data.get('studentDifficulties'),
                prof_data.get('numEvals', 0)
            ))
            
            reviews_data = prof_data.get('reviews', {})
            review_count = 0
            
            for course_code, course_reviews in reviews_data.items():
                course_code = course_code.strip().upper()
                
                cursor.execute("""
                    INSERT OR IGNORE INTO courses (code, department)
                    VALUES (?, ?)
                """, (course_code, prof_data.get('department', '')))
                
                for review in course_reviews:
                    cursor.execute("""
                    INSERT OR REPLACE INTO reviews 
                    (id, professor_id, course_code, grade, grade_level, course_type,
                    rating_text, post_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    review.get('id'),
                    prof_data.get('id'),
                    course_code,
                    review.get('grade', ''),
                    review.get('gradeLevel', ''),
                    review.get('courseType', ''),
                    review.get('rating', ''),
                    review.get('postDate', '')
                ))
                    review_count += 1
            
            conn.commit()
            
            # Update fuzzy search table
            self.update_professor_fuzzy_search(prof_data)
            
            return review_count
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_professor_details(self, professor_id, retry_count=0):
        """Get detailed data for specific professor and store in database"""
        input_data = {"id": professor_id}
        input_encoded = urllib.parse.quote(json.dumps(input_data))
        url = f"{self.base_url}/professors.get?input={input_encoded}" # that is what the PolyRating API takes
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            prof_data = data.get('result', {}).get('data', {})
            if not prof_data:
                self.log_message(f"No data returned for professor {professor_id}", "WARN", professor_id)
                return None
            
            review_count = self.store_professor_data(prof_data)
            
            prof_name = f"{prof_data.get('firstName', '')} {prof_data.get('lastName', '')}"
            self.log_message(f"Stored {prof_name} ({review_count} reviews)", "SUCCESS", professor_id)
            
            return prof_data
            
        except requests.exceptions.RequestException as e:
            if retry_count < self.max_retries:
                self.log_message(f"Retrying professor {professor_id} (attempt {retry_count + 1})", "WARN", professor_id)
                time.sleep(self.delay * 2)
                return self.get_professor_details(professor_id, retry_count + 1)
            else:
                self.log_message(f"Failed to fetch professor {professor_id}: {e}", "ERROR", professor_id)
                return None
    
    def fetch_all_professors(self):
        """Fetch and store data for all professors. Return true if there is not a single error"""
        self.log_message("Starting complete data fetch...")
        
        professors_overview = self.get_professors_overview()
        if not professors_overview:
            self.log_message("Failed to get professors overview", "ERROR")
            return False
        
        professors = professors_overview.get('result', {}).get('data', [])
        professor_ids = [prof.get('id') for prof in professors if prof.get('id')]
        
        if not professor_ids:
            self.log_message("No professor IDs found", "ERROR")
            return False
        
        self.log_message(f"Found {len(professor_ids)} professors to fetch")
        
        success_count = 0
        failed_count = 0
        failed_ids = []  
        total_reviews = 0
        
        for i, prof_id in enumerate(professor_ids):
            self.log_message(f"Progress: {i+1}/{len(professor_ids)} - Fetching {prof_id}")
            
            prof_data = self.get_professor_details(prof_id)
            if prof_data:
                success_count += 1
                total_reviews += prof_data.get('numEvals', 0)
            else:
                failed_count += 1
                failed_ids.append(prof_id)  # Store failed ID
            
       
            time.sleep(self.delay)
        
        # Summary
        self.log_message(f"Fetch complete! Success: {success_count}, Failed: {failed_count}")
        self.log_message(f"Total reviews stored: {total_reviews}")
        
        # Log failed IDs if any
        if failed_ids:
            self.log_message(f"Failed professor IDs: {', '.join(failed_ids)}", "WARN")
        
        return failed_count == 0
    
    def rebuild_fuzzy_search(self):
        """Rebuild the fuzzy name search table from existing professors"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Clear existing fuzzy search data
            cursor.execute("DELETE FROM professors_fts")
            
            # Rebuild from professors table
            cursor.execute("SELECT id, first_name, last_name FROM professors")
            professors = cursor.fetchall()
            
            for prof_id, first_name, last_name in professors:
                full_name = f"{first_name} {last_name}".strip()
                cursor.execute("""
                    INSERT INTO professors_fts (name, professor_id) 
                    VALUES (?, ?)
                """, (full_name, prof_id))
            
            conn.commit()
            self.log_message(f"Fuzzy search rebuilt with {len(professors)} professors")
            
        except Exception as e:
            conn.rollback()
            self.log_message(f"Fuzzy search rebuild failed: {e}", "ERROR")
        finally:
            conn.close()
    
    def show_stats(self):
        """Show database statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Counts
        cursor.execute("SELECT COUNT(*) FROM professors")
        prof_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM reviews")
        review_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM courses")
        course_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT department) FROM professors")
        dept_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM professors_fts")
        fts_count = cursor.fetchone()[0]
        
        # within 24 hours
        cursor.execute("""
            SELECT action, status, COUNT(*) 
            FROM fetch_logs 
            WHERE timestamp >= datetime('now', '-1 day') 
            GROUP BY action, status
        """)
        recent_logs = cursor.fetchall()
        
        conn.close()
        
        print("\n=== Database Statistics ===")
        print(f"Professors: {prof_count}")
        print(f"Reviews: {review_count}")
        print(f"Courses: {course_count}")
        print(f"Departments: {dept_count}")
        print(f"Fuzzy Search: {fts_count} entries")
        
        if recent_logs:
            print(f"\nRecent Activity (last 24h):")
            for action, status, count in recent_logs:
                print(f"  {action} {status}: {count}")
        
        print(f"\nDatabase file: {self.db_path}")

def main():
    parser = argparse.ArgumentParser(description="Fetch PolyRatings professor data to SQLite")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--all', action='store_true', help='Fetch all professors and reviews')
    group.add_argument('--prof', type=str, metavar='ID', help='Fetch specific professor by ID')
    group.add_argument('--stats', action='store_true', help='Show database statistics')
    group.add_argument('--rebuild-fuzzy', action='store_true', help='Rebuild fuzzy name search for typos')
    
    args = parser.parse_args()
    
    fetcher = PolyRatingsFetcher()
    
    if args.all:
        success = fetcher.fetch_all_professors()
        if success:
            print("\nData fetch completed!")
            fetcher.show_stats()
        else:
            print("\nFetch failed. Check the logs.") # This happens even when there is even a single error
    
    elif args.prof:
        result = fetcher.get_professor_details(args.prof)
        if result:
            print(f"Successfully fetched and stored professor {args.prof}")
        else:
            print(f"Failed to fetch professor {args.prof}")
    
    elif args.stats:
        fetcher.show_stats()
        
    elif args.rebuild_fuzzy:
        fetcher.rebuild_fuzzy_search()
        print("Fuzzy name search rebuilt!")

if __name__ == "__main__":
    main()