#!/usr/bin/env python3
"""
query_parser.py - Extract professor, course, department, and aspect from user queries
"""

import json
import sqlite3
from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()

class QueryParser:
    def __init__(self, db_path):
        self.db_path = db_path
        self.client = OpenAI(api_key=os.getenv("OPEN_AI_API_KEY"))
    
    def parse_query(self, query):
        prompt = f"""Extract information from this professor review query. Return JSON only:

Query: {query}

Extract:
- professor: professor name or null
- course: course code (like "CSC 202", "PHIL 126") or null  
- aspect: one ONLY from "teaching_style", "grading_exams", "workload", "accessibility", "course_structure", "personality", "overall"; Do not choose an aspect out of this list

Return JSON:
{{"professor": "...", "course": "...", "aspect": "..."}}"""
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=200
            )
            
            content = response.choices[0].message.content.strip()
            if content.startswith('```json'):
                content = content.replace('```json', '').replace('```', '').strip()
            
            result = json.loads(content)
            
            for key in result:
                if result[key] == "null" or result[key] == "":
                    result[key] = None
            
            return result
            
        except Exception as e:
            print(f"Parse error: {e}")
            return {"professor": None, "course": None, "aspect": None}
    
    def resolve_professor_course(self, parsed_query):
        """Find professor using fuzzy search"""
        result = {
            "professor_id": None,
            "professor_name": None,
            "course_code": None,
            "aspect": None
        }
        
        query_prof = parsed_query.get("professor")
        query_course = parsed_query.get("course")
        
        if not query_prof:
            return result
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Simple FTS5 search
            cursor.execute("""
                SELECT professor_id, name 
                FROM professors_fts 
                WHERE professors_fts MATCH ?
                LIMIT 1
            """, (query_prof,))
            
            match = cursor.fetchone()
            
            if match:
                prof_id, matched_name = match
                result["professor_id"] = prof_id
                result["professor_name"] = matched_name
                result["aspect"] = parsed_query.get("aspect")
                
                if query_course:
                    cursor.execute("""
                        SELECT DISTINCT course_code 
                        FROM reviews 
                        WHERE professor_id = ? AND UPPER(course_code) = UPPER(?)
                    """, (prof_id, query_course))
                    
                    if cursor.fetchone():
                        result["course_code"] = query_course.upper()
        
        return result

def main():
    parser = QueryParser("data/professors.db")
    
    test_queries = [
        "Is Theresa Migler organized?",
        "How is Theresa Miglar's exams?",
        "Should I take CSC 349 with Theresa Migler?",
        "Tell me about Hugh Smith"
    ]
    
    for query in test_queries:
        parsed = parser.parse_query(query)
        resolved = parser.resolve_professor_course(parsed)
        
        print(f"Query: {query}")
        print(f"Parsed: {parsed}")
        print(f"Resolved: {resolved}")
        print("-" * 50)

if __name__ == "__main__":
    main()