#!/usr/bin/env python3
"""
Process raw professor JSON files into clean review documents
Usage: python -m data_pipeline.process
"""

import json
from pathlib import Path
from datetime import datetime
import argparse

class ReviewProcessor:
    def __init__(self):
        self.raw_dir = Path("data/raw/professors")
        self.processed_dir = Path("data/processed")
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def log_message(self, message, level="INFO"):
        """Simple logging"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {level}: {message}")
    
    def process_professor_file(self, json_file):
        """Process a single professor JSON file"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            prof_data = data.get('result', {}).get('data', {})
            if not prof_data:
                self.log_message(f"No data in {json_file.name}", "WARN")
                return []
            
            # Extract professor info
            professor_id = prof_data.get('id')
            professor_name = f"{prof_data.get('firstName', '')} {prof_data.get('lastName', '')}".strip()
            department = prof_data.get('department', '')
            
            if not professor_id:
                self.log_message(f"No professor ID in {json_file.name}", "WARN")
                return []
            
            reviews = []
            course_reviews = prof_data.get('reviews', {})
            
            # Process reviews for each course
            for course_code, course_reviews_list in course_reviews.items():
                for review in course_reviews_list:
                    review_text = review.get('rating', '').strip()
                    
                    # Skip empty reviews
                    if not review_text or len(review_text) < 10:
                        continue
                    
                    # Create clean review document
                    clean_review = {
                        'document': review_text,
                        'metadata': {
                            'review_id': review.get('id'),
                            'professor_id': professor_id,
                            'professor_name': professor_name,
                            'department': department,
                            'course': course_code,
                            'grade': review.get('grade'),
                            'grade_level': review.get('gradeLevel'),
                            'course_type': review.get('courseType'),
                            'overall_rating': review.get('overallRating'),
                            'presents_material_clearly': review.get('presentsMaterialClearly'),
                            'recognizes_student_difficulties': review.get('recognizesStudentDifficulties'),
                            'post_date': review.get('postDate')
                        }
                    }
                    
                    reviews.append(clean_review)
            
            return reviews
            
        except Exception as e:
            self.log_message(f"Error processing {json_file.name}: {e}", "ERROR")
            return []
    
    def process_all_files(self):
        """Process all professor JSON files"""
        self.log_message("Starting review processing...")
        
        # Find all professor JSON files
        json_files = list(self.raw_dir.glob("*.json"))
        
        if not json_files:
            self.log_message("No professor JSON files found in data/raw/professors/", "ERROR")
            return False
        
        self.log_message(f"Found {len(json_files)} professor files to process")
        
        all_reviews = []
        processed_count = 0
        total_reviews = 0
        failed_files = []
        
        # Process each file
        for i, json_file in enumerate(json_files):
            if i % 50 == 0:  # Progress update every 50 files
                self.log_message(f"Progress: {i}/{len(json_files)} files processed")
            
            file_reviews = self.process_professor_file(json_file)
            if file_reviews:
                all_reviews.extend(file_reviews)
                processed_count += 1
                total_reviews += len(file_reviews)
            else:
                failed_files.append(json_file.name)
        
        # Report failed files
        if failed_files:
            self.log_message(f"Failed to process {len(failed_files)} files:", "WARN")
            for failed_file in failed_files:
                self.log_message(f"  - {failed_file}", "WARN")
        
        if not all_reviews:
            self.log_message("No reviews found to process", "ERROR")
            return False
        
        # Save processed reviews
        output_file = self.processed_dir / "reviews.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_reviews, f, indent=2, ensure_ascii=False)
        
        # Save processing stats
        stats = {
            'timestamp': datetime.now().isoformat(),
            'professors_processed': processed_count,
            'total_professors': len(json_files),
            'total_reviews': total_reviews,
            'failed_files': failed_files,
            'output_file': str(output_file)
        }
        
        stats_file = self.processed_dir / "processing_stats.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        
        # Summary
        self.log_message(f"Processing complete!")
        self.log_message(f"Professors processed: {processed_count}/{len(json_files)}")
        self.log_message(f"Total reviews extracted: {total_reviews}")
        if failed_files:
            self.log_message(f"Failed files: {len(failed_files)} (see details above)", "WARN")
        self.log_message(f"Output saved to: {output_file}")
        
        return True

def main():
    """CLI interface for processor"""
    parser = argparse.ArgumentParser(
        description="Process raw professor JSON files into clean review documents"
    )
    
    parser.add_argument('--stats', action='store_true',
                       help='Show processing statistics only')
    
    args = parser.parse_args()
    
    processor = ReviewProcessor()
    
    if args.stats:
        # Show existing stats if available
        stats_file = processor.processed_dir / "processing_stats.json"
        if stats_file.exists():
            with open(stats_file, 'r') as f:
                stats = json.load(f)
            print(f"Last processing: {stats['timestamp']}")
            print(f"Professors processed: {stats['professors_processed']}")
            print(f"Total reviews: {stats['total_reviews']}")
            
            # Show failed files if any
            failed_files = stats.get('failed_files', [])
            if failed_files:
                print(f"Failed files ({len(failed_files)}):")
                for failed_file in failed_files:
                    print(f"  - {failed_file}")
            else:
                print("No failed files")
        else:
            print("No processing statistics found. Run processing first.")
    else:
        success = processor.process_all_files()
        if success:
            print("\n✓ Review processing completed successfully!")
        else:
            print("\n✗ Processing failed. Check the logs above.")

if __name__ == "__main__":
    main()