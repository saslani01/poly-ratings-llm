#!/usr/bin/env python3
"""
retriever.py - Simple chunk retrieval for professor reviews
"""

import sqlite3

class ChunkRetriever:
    def __init__(self, db_path):
        self.db_path = db_path

    def get_chunks(self, professor_id, aspect=None, course_code=None, limit=10):
        """Get chunks for a professor, optionally filtered by aspect and course"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = """
                SELECT rc.aspect, rc.content, rc.sentiment, r.course_code
                FROM review_chunks rc
                JOIN reviews r ON rc.review_id = r.id
                WHERE r.professor_id = ?
            """
            params = [professor_id]
            
            if aspect:
                query += " AND rc.aspect = ?"
                params.append(aspect)
            
            if course_code:
                query += " AND UPPER(r.course_code) = UPPER(?)"
                params.append(course_code)
            
            query += " ORDER BY rc.created_at DESC LIMIT ?"
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            return [{"aspect": row[0], "content": row[1], "sentiment": row[2], "course_code": row[3]} 
                   for row in rows]

def main():
    retriever = ChunkRetriever("data/professors.db")

    for c in retriever.get_chunks("975ae5ae-66f6-4238-b431-bf0068ff2fad", course_code="CSC 349"):
        print(c)
        print()


if __name__ == "__main__":
    main()