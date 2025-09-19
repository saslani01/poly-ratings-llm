#!/usr/bin/env python3
"""
chunk.py - Simple review chunking with database integration
"""

import json
import re
import sqlite3
import tiktoken
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

class ReviewChunkProcessor:
    def __init__(self, db_path):
        self.db_path = db_path
        self.client = OpenAI(api_key=os.getenv("OPEN_AI_API_KEY"))
        self.encoding = tiktoken.encoding_for_model("gpt-4o-mini")
    
    def _clean_text(self, text):
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text).replace('\\"', '"').replace("\\'", "'").strip().strip('"')
    
    def _chunk_review(self, review_text):
        cleaned_text = self._clean_text(review_text)
        
        prompt = f"""Extract aspects from this professor review. Return JSON array only:
[{{"content": "text discussing aspect", "aspect": "teaching_style|grading_exams|workload|accessibility|course_structure|personality|overall", "sentiment": "positive|negative|neutral"}}]

{cleaned_text}"""

        tokens = len(self.encoding.encode(prompt))
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=800
            )
            
            tokens += response.usage.completion_tokens
            content = response.choices[0].message.content.strip()
            
            if content.startswith('```json'):
                content = content.replace('```json', '').replace('```', '').strip()
            
            return json.loads(content), tokens
            
        except Exception as e:
            print(f"Error: {e}")
            return [], 0
    
    def _store_chunks(self, review_id, chunks_data, tokens):
        try:
            with sqlite3.connect(self.db_path) as conn:
                for chunk in chunks_data:
                    conn.execute("""
                        INSERT INTO review_chunks (review_id, aspect, content, sentiment, tokens_used)
                        VALUES (?, ?, ?, ?, ?)
                    """, (review_id, chunk['aspect'], chunk['content'], chunk['sentiment'], tokens))
            return True
        except Exception as e:
            print(f"Store error: {e}")
            return False

    def _already_processed(self, review_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM review_chunks WHERE review_id = ?", (review_id,))
            return cursor.fetchone()[0] > 0

    def process_professor_reviews(self, professor_id):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT r.id, r.rating_text, p.first_name, p.last_name, r.course_code
                FROM reviews r
                JOIN professors p ON r.professor_id = p.id
                WHERE r.professor_id = ? AND r.rating_text IS NOT NULL AND r.rating_text != ''
            """, (professor_id,))
            reviews = cursor.fetchall()
        
        if not reviews:
            return {"processed": 0, "failed": 0, "total": 0, "tokens": 0}
        
        professor_name = f"{reviews[0][2]} {reviews[0][3]}"
        processed = failed = total_tokens = 0
        
        print(f"Processing {len(reviews)} reviews for {professor_name}")
        
        for i, (review_id, text, _, _, course) in enumerate(reviews):
            print(f"[{i+1}/{len(reviews)}] {course or 'N/A'}")
            
            if self._already_processed(review_id):
                print("Already processed - skipping")
                processed += 1
                continue
            
            chunks_data, tokens = self._chunk_review(text)
            
            if chunks_data and self._store_chunks(review_id, chunks_data, tokens):
                processed += 1
                total_tokens += tokens
                print(f"{len(chunks_data)} chunks, {tokens} tokens")
            else:
                failed += 1
                print("Failed")
        
        print(f"Completed {professor_name}: {processed}/{len(reviews)} processed, {total_tokens} tokens")
        return {"processed": processed, "failed": failed, "total": len(reviews), "tokens": total_tokens}

    def process_all_reviews(self, limit=None):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            limit_clause = f"LIMIT {limit}" if limit else ""
            cursor.execute(f"""
                SELECT r.id, r.rating_text, p.first_name, p.last_name, r.course_code
                FROM reviews r
                JOIN professors p ON r.professor_id = p.id
                WHERE r.rating_text IS NOT NULL AND r.rating_text != ''
                {limit_clause}
            """)
            reviews = cursor.fetchall()
        
        if not reviews:
            return {"processed": 0, "failed": 0, "total": 0, "tokens": 0}
        
        processed = failed = total_tokens = 0
        
        with open("chunking.log", "w", encoding="utf-8") as log_file:
            for i, (review_id, text, first_name, last_name, course) in enumerate(reviews):
                professor = f"{first_name} {last_name}"
                
                print(f"[{i+1}/{len(reviews)}] {professor} ({course or 'N/A'})")
                log_file.write(f"[{i+1}/{len(reviews)}] {professor} ({course or 'N/A'})\n")
                
                if self._already_processed(review_id):
                    print("Already processed - skipping")
                    log_file.write("Already processed - skipping\n")
                    processed += 1
                    continue
                
                chunks_data, tokens = self._chunk_review(text)
                
                if chunks_data and self._store_chunks(review_id, chunks_data, tokens):
                    processed += 1
                    total_tokens += tokens
                    msg = f"{len(chunks_data)} chunks, {tokens} tokens"
                    print(msg)
                    log_file.write(msg + "\n")
                else:
                    failed += 1
                    print("Failed")
                    log_file.write("Failed\n")
                
                if (i + 1) % 100 == 0:
                    print(f"Progress: {i+1}/{len(reviews)}")
                    log_file.flush()
        
        return {"processed": processed, "failed": failed, "total": len(reviews), "tokens": total_tokens}
    
def main():
    processor = ReviewChunkProcessor("data/professors.db")
    result = processor.process_all_reviews()
    print(f"Results: {result['processed']}/{result['total']} processed, {result['tokens']} tokens")

if __name__ == "__main__":
    main()