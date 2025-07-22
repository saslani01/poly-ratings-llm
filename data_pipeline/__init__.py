"""
Data pipeline for Cal Poly professor ratings chatbot

Modules:
- fetch: Download professor data from PolyRatings API
- process: Transform raw JSON into clean review documents  
- prep: Load processed data into ChromaDB vector store
"""

__version__ = "0.1.0"