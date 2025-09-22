# ProfGPT🐎 [Try the BETA Version Now!](https://poly-ratings-llm-production.up.railway.app/)

AI-powered analysis of Cal Poly professor reviews using OpenAI GPT models. Query professors in natural language and get intelligent insights from student reviews.

## Features

- 🔍 **Natural Language Queries**: Ask questions like "How is John Smith's teaching style?"
- 🤖 **AI-Powered Analysis**: GPT-4o processes reviews into categorized insights
- 📊 **Aspect-Based Reviews**: Teaching style, grading, workload, accessibility, course structure, personlity and overal.
- 💾 **Fast Local Storage**: SQLite database for instant queries
- 🌐 **Web Interface**: Beautiful Cal Poly-themed web app with mobile support
- ⚡ **Rate Limited**: Built-in protection against API abuse

## Quick Start

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   echo "OPEN_AI_API_KEY=your_key_here" > .env
   ```

2. **Fetch professor data**
   ```bash
   python fetcher.py --all
   ```

3. **Process reviews with AI**
   ```bash
   python chunker.py
   ```

4. **Run the web app**
   ```bash
   python app.py
   ```

Visit `http://localhost:8000` to use the web interface.

## Example Queries

- "H"ow is Andrew Timothy course structure?"
- "How is Theresa Migler exams?"
- "How is Stephen Beard personlity?"
- "What is the workload like for Jefferson Clarke in ART 122?"

## File Overview

- **`app.py`** - FastAPI web application with rate limiting
- **`fetcher.py`** - Downloads data from PolyRatings API and set up the sqlite database
- **`chunker.py`** - Processes reviews with AI into structured insights
- **`synthesizer.py`** - Handles natural language queries and generates responses
- **`query_parser.py`** - Parses user queries to extract professor/course info and insight
- **`retriever.py`** - Retrieves relevant review data for analysis
- **`templates/`** - HTML templates for web interface
- **`static/`** - CSS and JavaScript files

## Database Schema

```sql
professors: id, first_name, last_name, department, overall_ratings, material_clear, student_difficulties, num_evals, created_at
reviews: id, professor_id, course_code, text, grade_level, course_type, rating_text, post_date, cteated_at
review_chunks: id, review_id, aspect, content, sentiment, tokens_used, created_at
courses: code, name, department, created_at
fetch_logs: id, action, status, message, professor_id, timestamp
professors_fts: name, professor_id
```

## Configuration

- **OpenAI API**: Uses GPT-4o-mini 
- **Rate Limits**: 10 queries per minute per IP address
- **Aspects**: teaching_style, grading_exams, workload, accessibility, course_structure, personality, overall

## Commands

```bash
# Fetch all professors (10-30 minutes)
python fetcher.py --all

# Fetch specific professor
python fetcher.py --prof "professor-id"

# Show database stats
python fetcher.py --stats

# Process reviews with AI. Takes a long time to process 50,000 reviews, so you may want to use process_professor_reviews() for testing
python chunker.py

# Run web application
python app.py
```

## Deployment

Deployed on Railway

- Rate limiting to prevent API abuse
- Mobile-responsive Cal Poly-themed interface
- Health check endpoints
- Static file serving
- Environment variable configuration

## Data Updates

Currently uses a "nuclear" approach for data updates - the entire database is rebuilt from scratch when updating. This ensures data consistency but requires reprocessing all reviews with AI (several days and ~$15 in API costs).

## API Costs (Approx.)

- **Processing**: ~$15 for 50000+ reviews
- **Queries**: ~$0.0001-0.001 per question 

## Data Source

Professor review data is sourced from [polyratings.dev](https://polyratings.dev/), which provides comprehensive Cal Poly professor ratings and reviews.

## Contributing

This project is open source under AGPL-3.0. Areas for contribution:

- **Incremental Data Updates**: Currently requires full database rebuild for updates. Could implement:
  - Fetch only new/updated professor data since last update
  - Process only new reviews rather than reprocessing everything
  - Selective re-chunking when AI prompts are improved
  - Automated update scheduling (weekly/monthly)
  - Database versioning to track data freshness
- **Performance optimizations**
- **Chatbot Memeory**
- **Comparison of professors feature**
- **Better Prompt Engineering and Models**
- **Web interface improvements**
- **Mobile app development** 
- **Deployment infrastructure**

To contribute:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

All contributions must be licensed under AGPL-3.0.

## License

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.

## Disclaimer

ProfGPT is an educational tool that uses AI to analyze professor reviews. The AI can make mistakes, and results should be considered alongside other factors when making academic decisions. This project is not officially affiliated with Cal Poly. This software is provided 'as is' without any warranties or guarantees of any kind.

## Contact

This is a student project for the Cal Poly community. For questions or suggestions, please open an issue on GitHub.