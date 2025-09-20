#!/usr/bin/env python3
"""
synthesizer.py - Generate professor summaries using chunked reviews and OpenAI API
"""

import sqlite3
from openai import OpenAI
import os
from dotenv import load_dotenv
from query_parser import QueryParser
from retriever import ChunkRetriever

load_dotenv()

class ProfessorSynthesizer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.client = OpenAI(api_key=os.getenv("OPEN_AI_API_KEY"))
        self.parser = QueryParser(db_path)
        self.retriever = ChunkRetriever(db_path)
        
    def get_numerical_professor_info(self, professor_id):
        """Get basic professor information and numerical ratings"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT first_name, last_name, department, overall_rating,
                       material_clear, student_difficulties, num_evals
                FROM professors 
                WHERE id = ?
            """, (professor_id,))
            
            row = cursor.fetchone()
            if not row:
                return None
                
            return {
                "name": f"{row[0]} {row[1]}",
                "department": row[2],
                "overall_rating": row[3],
                "material_clear": row[4],
                "student_difficulties": row[5],
                "num_evals": row[6]
            }
    
    def format_basic_stats(self, prof_info):
        """Format basic professor stats"""
        return f"""Professor {prof_info['name']} - {prof_info['department']}

Basic Stats (out of 4.0):
• Overall Rating: {prof_info['overall_rating']}/4
• Material Clarity: {prof_info['material_clear']}/4  
• Student Difficulties: {prof_info['student_difficulties']}/4
• Number of Evaluations: {prof_info['num_evals']}

"""
    
    def process_query(self, user_query):
        """Main pipeline: parse -> resolve -> get chunks -> generate answer"""
        
        parsed = self.parser.parse_query(user_query)
        print(f"Parsed query: {parsed}")
        
        resolved_prof_course = self.parser.resolve_professor_course(parsed)
        print(f"Resolved: {resolved_prof_course}")
        
        if not resolved_prof_course["professor_id"]:
            response = "Professor not found in database"
            return response, 0
        
        resolved_prof_course["original_query"] = user_query

        prof_info = self.get_numerical_professor_info(resolved_prof_course["professor_id"])
        if not prof_info:
            response = "Professor information not available"
            return response, 0
        
        chunks = self.retriever.get_chunks(
            resolved_prof_course["professor_id"], 
            resolved_prof_course["aspect"], 
            resolved_prof_course["course_code"], 
            limit=10
        )
        
        if not chunks:
            response = self.format_basic_stats(prof_info) + "No Review Excerpts found for this query."
            return response, 0
                
        response, tokens_used = self.generate_summary(prof_info, chunks, resolved_prof_course)
        
        return response, tokens_used
    
    def filter_chunks_by_aspect(self, chunks, target_aspect):
        """Filter chunks by aspect, fallback to 'overall' if no matches found"""
        if not target_aspect:
            return chunks
        
        matching_chunks = [chunk for chunk in chunks if chunk['aspect'].lower() == target_aspect.lower()]
        
        if matching_chunks:
            return matching_chunks
        
        overall_chunks = [chunk for chunk in chunks if chunk['aspect'].lower() == 'overall']
        
        if overall_chunks:
            return overall_chunks
        
        return None
    
    def generate_summary(self, prof_info, chunks, resolved):
        """Generate answer using chunks and professor info"""
        
        filtered_chunks = self.filter_chunks_by_aspect(chunks, resolved["aspect"])
        
        if not filtered_chunks:
            return self.format_basic_stats(prof_info) + "No specific review excerpts available for this query.", 0
        
        prompt = f"""Based on the following student review excerpts about Professor {prof_info['name']} from {prof_info['department']} department, answer this question: "{resolved.get('original_query', 'Tell me about this professor?')}"

Student Review Excerpts:
"""
        
        for chunk in filtered_chunks:
            prompt += f"- [{chunk['aspect']}] {chunk['content']}\n"
                
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=300,
                timeout=30
            )
            
            answer_text = response.choices[0].message.content.strip()
            
            token_usage = response.usage
            total_tokens = token_usage.total_tokens
            
            chunks_section = "\n\nReview Excerpts Used:"
            for chunk in filtered_chunks:
                chunks_section += f"\n• [{chunk['aspect']}] {chunk['content']}"
            
            final_response = self.format_basic_stats(prof_info) + answer_text + chunks_section
            
            return final_response, total_tokens
            
        except Exception as e:
            return f"Error generating answer: {e}", 0