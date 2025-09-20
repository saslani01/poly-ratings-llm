#!/usr/bin/env python3
"""
chunk.py - Review chunking with database integration
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
        
        prompt = f"""Extract aspects from this professor review. Return JSON array only: extract anything giving insight about professor including slang but not physical/inappropriate
[{{"content": "text discussing aspect", "aspect": "teaching_style|grading_exams|workload|accessibility|course_structure|personality|overall", "sentiment": "positive|negative|neutral"}}]

{cleaned_text}"""

        tokens = len(self.encoding.encode(prompt))
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=1000
            )
            
            tokens += response.usage.completion_tokens
            content = response.choices[0].message.content.strip()
            
            if content.startswith('```json'):
                content = content.replace('```json', '').replace('```', '').strip()
            
            return json.loads(content), tokens, content
            
        except Exception as e:
            return [], 0, f"Error: {e}"
    
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

    def process_reviews(self, professor_id=None, limit=None):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            if professor_id:
                cursor.execute("""
                    SELECT r.id, r.rating_text, p.first_name, p.last_name, r.course_code
                    FROM reviews r
                    JOIN professors p ON r.professor_id = p.id
                    WHERE r.professor_id = ? AND r.rating_text IS NOT NULL AND r.rating_text != ''
                """, (professor_id,))
            else:
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
        
        with open("chunking.log", "w", encoding="utf-8", newline='\n') as log_file:
            log_file.write(f"Processing {len(reviews)} reviews\n\n")
            
            for i, (review_id, text, first_name, last_name, course) in enumerate(reviews):
                professor = f"{first_name} {last_name}"
                
                print(f"[{i+1}/{len(reviews)}] {professor} ({course or 'N/A'})")
                log_file.write(f"[{i+1}/{len(reviews)}] {professor} ({course or 'N/A'})\n")
                log_file.write(f"Review ID: {review_id}\n")
                log_file.write(f"Original text: {text}\n")
                
                if self._already_processed(review_id):
                    print("Already processed - skipping")
                    log_file.write("Already processed - skipping\n\n")
                    processed += 1
                    continue
                
                chunks_data, tokens, llm_response = self._chunk_review(text)
                
                log_file.write(f"Full LLM response: {llm_response}\n")
                
                if chunks_data and self._store_chunks(review_id, chunks_data, tokens):
                    processed += 1
                    total_tokens += tokens
                    msg = f"{len(chunks_data)} chunks, {tokens} tokens"
                    print(msg)
                    log_file.write(f"SUCCESS: {msg}\n")
                else:
                    failed += 1
                    print("Failed")
                    log_file.write("FAILED\n")
                    
                    # Log failures to separate file
                    with open("chunking_failures.log", "a", encoding="utf-8", newline='\n') as fail_log:
                        fail_log.write(f"[{i+1}/{len(reviews)}] {professor} ({course or 'N/A'})\n")
                        fail_log.write(f"Review ID: {review_id}\n")
                        fail_log.write(f"Original text: {text}\n")
                        fail_log.write(f"Full LLM response: {llm_response}\n")
                        fail_log.write("="*80 + "\n\n")
                
                log_file.write("\n" + "="*80 + "\n\n")
                
                if (i + 1) % 100 == 0:
                    log_file.flush()
        
        return {"processed": processed, "failed": failed, "total": len(reviews), "tokens": total_tokens}
    
def main():
    processor = ReviewChunkProcessor("data/professors.db")
    
    # Process all reviews
    result = processor.process_reviews()
    
    # Or process specific professor
    # result = processor.process_reviews(professor_id=123)
    
    print(f"Results: {result['processed']}/{result['total']} processed, {result['tokens']} tokens")

if __name__ == "__main__":
    main()