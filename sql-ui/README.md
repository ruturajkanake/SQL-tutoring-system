# SQL Tutoring System - Web UI

React-based web interface for the SQL Tutoring System. This frontend provides an interactive environment for students to practice SQL queries with real-time feedback and automated hint generation.

## Features

- **Monaco Editor**: Full-featured SQL editor with syntax highlighting and autocomplete
- **Real-time Feedback**: Instant comparison of student queries against reference solutions
- **Tiered Hints**: Progressive hint system (Level 1-4) for guided learning
- **Schema Browser**: Interactive database schema viewer
- **Question Browser**: Browse and select SQL practice questions by difficulty
- **Google OAuth**: Secure authentication for UCR.edu users
- **Progress Tracking**: Visual progress tracking across questions

## Installation

```bash
npm install
```

## Development

```bash
npm run dev
```

The application will be available at `http://localhost:5173`

## Build

```bash
npm run build
```

## Preview Production Build

```bash
npm run preview
```

## Components

- **App.jsx**: Main application component with question selection and query editor
- **Schema.jsx**: Database schema visualization component
- **main.jsx**: Application entry point

## Environment Variables

Create a `.env` file in the root directory:

```
VITE_API_URL=http://localhost:8000
VITE_GOOGLE_CLIENT_ID=your_google_client_id
```

## Dependencies

- react: UI framework
- @monaco-editor/react: Monaco React wrapper for SQL editing
- @react-oauth/google: Google OAuth integration
- axios: HTTP client for API communication
- tailwindcss: Utility-first CSS framework

## API Integration

The frontend communicates with the FastAPI backend running on port 8000. Ensure the backend is running before starting the frontend.

## Usage

1. Sign in with Google OAuth (UCR.edu email required)
2. Select a question from the question browser
3. Write your SQL query in the editor
4. Click "Run Query" to execute and compare against reference
5. View hints and feedback to improve your solution
