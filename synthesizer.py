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

    def _error(self, message, code):
        """Construct a specific error and status code for the frontend"""
        return {"error": message, "code": code}, 0 # 0 is just the tokens used

    def _build_response(self, prof_info, excerpts, analysis):
        """Assemble the standard success payload"""
        return {
            "professor": prof_info,
            "stats": {
                "overall_rating": prof_info["overall_rating"],
                "material_clear": prof_info["material_clear"],
                "student_difficulties": prof_info["student_difficulties"],
                "num_evals": prof_info["num_evals"],
            },
            "excerpts": excerpts,
            "analysis": analysis,
        }

    def get_numerical_professor_info(self, professor_id):
        """Get basic professor information and numerical ratings"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT first_name, last_name, department, overall_rating,
                       material_clear, student_difficulties, num_evals
                FROM professors
                WHERE id = ?
                """,
                (professor_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "name": f"{row[0]} {row[1]}",
                "department": row[2],
                "overall_rating": row[3],
                "material_clear": row[4],
                "student_difficulties": row[5],
                "num_evals": row[6],
            }

    def process_query(self, user_query):
        """Main pipeline: parse -> resolve -> get chunks -> generate answer"""
        parsed = self.parser.parse_query(user_query)
        resolved = self.parser.resolve_professor_course(parsed)

        if not resolved["professor_id"]:
            return self._error("No professor matched your query. Check the spelling or try their full name.", 404)

        resolved["original_query"] = user_query

        prof_info = self.get_numerical_professor_info(resolved["professor_id"])
        if not prof_info:
            return self._error("We found this professor but their rating data is missing.", 500)

        chunks = self.retriever.get_chunks(
            resolved["professor_id"],
            resolved["aspect"],
            resolved["course_code"],
            limit=10,
        )
        if not chunks:
            return self._build_response(prof_info, [], "No review excerpts found for this query."), 0

        return self.generate_summary(prof_info, chunks, resolved)

    def filter_chunks_by_aspect(self, chunks, target_aspect):
        """Filter chunks by aspect, fallback to 'overall' if no matches found"""
        if not target_aspect:
            return chunks

        matching = [c for c in chunks if c["aspect"].lower() == target_aspect.lower()]
        if matching:
            return matching

        overall = [c for c in chunks if c["aspect"].lower() == "overall"]
        if overall:
            return overall

        return None

    def generate_summary(self, prof_info, chunks, resolved):
        """Generate answer using chunks and professor info"""
        filtered = self.filter_chunks_by_aspect(chunks, resolved["aspect"])
        if not filtered:
            return self._build_response(prof_info, [], "No specific review excerpts available for this query."), 0

        question = resolved.get("original_query", "Tell me about this professor?")
        prompt = (
            f'Based on the following student review excerpts about Professor {prof_info["name"]} '
            f'from {prof_info["department"]} department, answer this question: "{question}"\n\n'
            "Student Review Excerpts:\n"
        )
        for chunk in filtered:
            prompt += f"- [{chunk['aspect']}] {chunk['content']}\n"

        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=300,
                timeout=30,
            )
            answer = response.choices[0].message.content.strip()
            excerpts = [{"aspect": c["aspect"], "content": c["content"]} for c in filtered]
            return self._build_response(prof_info, excerpts, answer), response.usage.total_tokens
        except Exception as e:
            print(f"OpenAI error: {e}")
            return self._error("Couldn't generate the summary right now. Please try again shortly.", 503)