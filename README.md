# SQL Tutoring System

An intelligent SQL tutoring system that provides automated feedback and tiered hints for students learning SQL. The system compares student queries against reference solutions using AST analysis, semantic diffing, and constraint-based checking.

## Features

- **Intelligent Query Comparison**: Compares student SQL against reference solutions using multiple analysis techniques
- **Tiered Hint System**: Provides progressive hints (Level 1-4) from general guidance to conceptual explanations
- **AST Structural Analysis**: Identifies differences in columns, tables, joins, subqueries, and GROUP BY clauses
- **Semantic Verification**: Executes queries against DuckDB to compare actual results
- **Constraint-Based Feedback**: 25+ constraint detectors for common SQL mistakes (missing joins, incorrect grouping, etc.)
- **LLM Integration**: Optional Fireworks API integration for advanced conceptual hints
- **Google OAuth Authentication**: Secure authentication for UCR.edu users
- **Progress Tracking**: Tracks user progress and hint feedback
- **Web UI**: React-based interface for interactive SQL practice

## Architecture

### Backend (Python/FastAPI)

- **main.py**: Legacy single-file tool for SQL comparison and hint generation
- **new.py**: Enhanced FastAPI server with LLM integration and advanced hinting
- **auth.py**: Google OAuth authentication and JWT session management
- **db.py**: SQLite database for user progress and feedback tracking
- **canonicalizer.py**: SQL query canonicalization using sqlglot
- **normalizer.py**: AST normalization for deterministic comparison
- **ast_diff.py**: Structural AST comparison (columns, tables, joins, subqueries)
- **semantic_diff.py**: Semantic result comparison and difference detection
- **verifier.py**: DuckDB-based query execution and result verification
- **constraints.py**: 25 constraint detectors for common SQL errors

### Frontend (React/Vite)

Located in `sql-ui/` directory:
- React-based web interface for SQL practice
- Monaco Editor for SQL syntax highlighting
- Real-time query execution and feedback
- Google OAuth integration

### Data

- **questions.json**: 51 SQL practice questions (Easy, Medium, Hard difficulty)
- **sample_setup.sql**: Sample database schema (patients, doctors, admissions, province_names)
- **app.db**: SQLite database for user progress

## Installation

### Backend Dependencies

```bash
pip install -r requirements.txt
```

Required packages:
- sqlglot==28.1.0 (SQL parsing and transformation)
- duckdb==1.4.3 (In-memory SQL execution)
- fastapi==0.124.2 (Web framework)
- uvicorn==0.38.0 (ASGI server)
- pydantic==2.12.5 (Data validation)
- google-auth==2.45.0 (Google OAuth)
- python-jose==3.5.0 (JWT handling)
- requests==2.32.4 (HTTP client)

### Frontend Dependencies

```bash
cd sql-ui
npm install
```

## Configuration

Set the following environment variables:

```bash
# Google OAuth
GOOGLE_CLIENT_ID=your_google_client_id
JWT_SECRET=your_jwt_secret

# Optional: Fireworks API for LLM hints
FIREWORKS_API_KEY=your_fireworks_api_key
```

## Usage

### Running the Backend

```bash
# Start the FastAPI server
uvicorn new:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`

### Running the Frontend

```bash
cd sql-ui
npm run dev
```

The UI will be available at `http://localhost:5173`

### CLI Usage (Legacy)

The single-file tool can be used directly:

```bash
python main.py --student "SELECT * FROM patients" --reference "SELECT * FROM patients"
python main.py --student-file student.sql --reference-file ref.sql --setup sample_setup.sql --json
```

## API Endpoints

- `POST /api/compare` - Compare student SQL against reference solution
- `POST /api/auth/google` - Google OAuth authentication
- `GET /api/questions` - Get list of practice questions
- `POST /api/feedback` - Submit hint feedback
- `GET /api/progress` - Get user progress

## Hint Levels

The system provides tiered hints:

1. **Level 1**: General guidance (e.g., "Review the general area indicated below")
2. **Level 2**: Specific issue identification (e.g., "Missing columns in SELECT")
3. **Level 3**: Detailed explanation (e.g., "Ensure every attribute required by the task appears in SELECT")
4. **Level 4**: Conceptual hints (via LLM integration)

## Constraint Detectors

The system includes 25+ constraint detectors:

- **Filter Issues**: missing_where, extra_where, between_bounds, and_or, contradiction
- **Join Issues**: missing_join, join_type, join_on, cartesian, self_join_alias
- **Grouping Issues**: missing_group, extra_group, non_grouped, agg_func, count_star, having
- **Projection Issues**: distinct, projection, expr_type, alias, star
- **Other Issues**: null, operator, limit

## Database Schema

### Sample Database (DuckDB)

- `province_names`: Province ID and name mappings
- `patients`: Patient information (demographics, allergies, vitals)
- `doctors`: Doctor information and specialties
- `admissions`: Hospital admission records with diagnoses

### User Progress Database (SQLite)

- `user_progress`: Tracks user progress on questions
- `hint_feedback`: Stores feedback on hint usefulness

## Development

### Adding New Questions

Add questions to `questions.json` following the existing format:

```json
{
  "id": 52,
  "question_number": 1,
  "section": "Easy",
  "question": "Your question text here",
  "answer_ref": "SELECT * FROM table WHERE condition;"
}
```

### Adding New Constraints

Add constraint detectors to `constraints.py`:

1. Define a checker function that takes context and returns evidence or None
2. Add to CONSTRAINTS list with priority and hint messages

