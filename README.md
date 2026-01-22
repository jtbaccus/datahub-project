# Personal Data Hub

A Python-based personal data aggregator that pulls fitness and finance data into a unified local system.

## Overview

DataHub aggregates scattered personal data (fitness metrics, financial transactions) into one queryable, visualizable hub. Built with a local-first, privacy-respecting philosophy.

### Supported Devices & Services

**Fitness:**
- iPhone + Apple Watch (via Apple Health export)
- Oura Ring (API)
- Peloton (API)
- Tonal (unofficial API)

**Finance:**
- Bank CSV imports (Chase, Bank of America, Apple Card, Amex)
- SimpleFIN Bridge (automated bank syncing)

## Installation

```bash
# Clone the repository
git clone <repo-url>
cd datahub-project

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install the package
pip install -e ".[web]"

# Initialize database
datahub init

# Verify setup
datahub status
```

## Quick Start

```bash
# Import Apple Health data
datahub import apple-health export.xml

# Import bank transactions
datahub import bank-csv transactions.csv

# Sync from APIs
datahub sync peloton
datahub sync oura

# View summary
datahub summary --days 7

# Start web dashboard
datahub web
# Opens at http://localhost:8000
```

## Configuration

Config is stored in `~/.datahub/config.json`. Set values with:

```bash
# Peloton credentials
datahub config peloton.username "your-email"
datahub config peloton.password "your-password"

# Oura API token (get from https://cloud.ouraring.com/personal-access-tokens)
datahub config oura.token "your-token"

# Tonal credentials
datahub config tonal.email "your-email"
datahub config tonal.password "your-password"

# SimpleFIN Bridge ($15/year at simplefin.org)
datahub sync simplefin --setup "YOUR_SETUP_TOKEN"
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `datahub init` | Set up database |
| `datahub config KEY VALUE` | Set config values |
| `datahub import apple-health FILE` | Import Apple Health XML |
| `datahub import bank-csv FILE` | Import bank CSV |
| `datahub sync peloton` | Sync Peloton data |
| `datahub sync oura` | Sync Oura Ring data |
| `datahub sync tonal` | Sync Tonal workouts |
| `datahub sync simplefin` | Sync bank transactions |
| `datahub status` | Show data counts and sync status |
| `datahub summary [--days N]` | Fitness summary |
| `datahub transactions [--days N]` | List transactions |
| `datahub spending [--days N]` | Spending by category |
| `datahub insights` | Fitness/spending correlations |
| `datahub export FORMAT` | Export to JSON/CSV |
| `datahub web` | Start web dashboard |

## Tech Stack

- **Language:** Python 3.11+
- **CLI:** Click + Rich
- **Database:** SQLAlchemy 2.0 + SQLite
- **HTTP Client:** httpx
- **Validation:** Pydantic
- **Web:** FastAPI + Jinja2 + HTMX + Tailwind CSS + Chart.js

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=datahub --cov-report=term-missing
```

## Project Structure

```
datahub-project/
├── pyproject.toml
├── datahub/
│   ├── cli.py                  # CLI commands
│   ├── db.py                   # SQLAlchemy models
│   ├── config.py               # Configuration management
│   ├── dedup.py                # Data deduplication logic
│   └── connectors/
│       ├── fitness/            # Fitness data connectors
│       └── finance/            # Finance data connectors
├── web/
│   ├── app.py                  # FastAPI application
│   └── templates/              # Jinja2 templates
└── tests/                      # Test suite
```

## License

MIT
