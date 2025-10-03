# Reflex1.9
Stable version after CoPilot

A basic Reflex application that connects to OpenAI's GPT API (successor to Codex) for code generation.

## Features

- Interactive web interface built with Reflex
- Connect to OpenAI's GPT API for code generation
- Real-time code generation based on natural language prompts
- Clean, responsive UI

## Prerequisites

- Python 3.8 or higher
- OpenAI API key ([Get one here](https://platform.openai.com/api-keys))

## Setup

1. Clone the repository:
```bash
git clone https://github.com/mkmdenver/Reflex1.9.git
cd Reflex1.9
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up your OpenAI API key:
   - Copy `.env.example` to `.env`:
     ```bash
     cp .env.example .env
     ```
   - Edit `.env` and add your OpenAI API key:
     ```
     OPENAI_API_KEY=your_actual_api_key_here
     ```

## Usage

1. Initialize the Reflex app (first time only):
```bash
reflex init
```

2. Run the application:
```bash
reflex run
```

3. Open your browser and navigate to `http://localhost:3000`

4. Enter a coding prompt (e.g., "Write a Python function to calculate fibonacci numbers") and click "Generate Code"

## Project Structure

```
Reflex1.9/
├── reflex1_9/           # Main application directory
│   ├── __init__.py      # Package initialization
│   └── reflex1_9.py     # Main application code
├── rxconfig.py          # Reflex configuration
├── requirements.txt     # Python dependencies
├── .env.example         # Example environment variables
├── .gitignore           # Git ignore patterns
├── LICENSE              # Project license
└── README.md            # This file
```

## Technologies Used

- [Reflex](https://reflex.dev/) - Full-stack Python web framework
- [OpenAI API](https://openai.com/) - AI-powered code generation
- Python 3.8+

## Note

This project uses GPT-3.5-turbo as the model. OpenAI's Codex has been deprecated, but GPT models provide similar and improved code generation capabilities.

## License

This project is licensed under the terms in the LICENSE file.
