#!/usr/bin/env python3
"""
app.py - FastAPI web API for professor review system
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field
from synthesizer import ProfessorSynthesizer
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from fastapi.staticfiles import StaticFiles
from slowapi.errors import RateLimitExceeded
import os
from datetime import datetime

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="Cal Poly Professor Review API",
    description="AI-powered professor review analysis system for Cal Poly",
    version="1.0.0"
)

app.mount("/static", StaticFiles(directory="static"), name="static")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Add validation error handler
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle Pydantic validation errors"""
    return JSONResponse(
        status_code=422,
        content={
            "detail": "Validation failed",
            "errors": exc.errors(),
            "message": "Request data does not meet validation requirements"
        }
    )

synthesizer = ProfessorSynthesizer("data/professors.db")

class QueryRequest(BaseModel):
    query: str = Field(..., max_length=100, min_length=1)

class QueryResponse(BaseModel):
    query: str
    response: str
    timestamp: str
    tokens_used: int = 0

@app.get("/")
async def home():
    """Serve the main web interface"""
    return FileResponse("templates/index.html")

@app.post("/api/query", response_model=QueryResponse)
@limiter.limit("10/minute")  # 10 queries per minute per IP address
async def query_professor(request: Request, query_request: QueryRequest):
    """
    Query professor reviews using AI analysis
    
    - **query**: Natural language question about a professor
    """
    try:
        if not query_request.query.strip():
            raise HTTPException(status_code=400, detail="Query cannot be empty")
        
        response, tokens_used = synthesizer.process_query(query_request.query)
        
        return QueryResponse(
            query=query_request.query,
            response=response,
            timestamp=datetime.now().isoformat(),
            tokens_used=tokens_used
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "database": "connected"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)