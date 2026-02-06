# Personal Data Hub

A Python-based personal data aggregator that pulls fitness and finance data into a unified local system.

**Repository:** https://github.com/jtbaccus/datahub-project

## Project Overview

**Core Purpose:** Aggregate scattered personal data (fitness metrics, financial transactions) into one queryable, visualizable hub.

**Philosophy:** Local-first, privacy-respecting, built to grow.

**Target Hardware:**
- iPhone + Apple Watch
- Oura Ring
- Tonal
- Peloton

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.11+ |
| CLI | Click + Rich |
| Database | SQLAlchemy 2.0 + SQLite |
| HTTP Client | httpx |
| Validation | Pydantic |
| Web Framework | FastAPI |
| Templating | Jinja2 |
| Styling | Tailwind CSS (CDN) |
| Charts | Chart.js |
| Interactivity | HTMX |
| Testing | pytest + pytest-cov |

## Project Structure

```
datahub-project/
├── pyproject.toml              # Project config and dependencies
├── datahub/
│   ├── __init__.py             # Package init
│   ├── cli.py                  # CLI with 12+ commands
│   ├── db.py                   # SQLAlchemy models (DataPoint, Transaction, SyncLog)
│   ├── config.py               # JSON-based config management
│   ├── dedup.py                # Multi-source deduplication logic
│   └── connectors/
│       ├── base.py             # Abstract connector interface
│       ├── fitness/
│       │   ├── apple_health.py # XML import (steps, HR, HRV, sleep, workouts)
│       │   ├── peloton.py      # API connector (workouts, metrics)
│       │   ├── oura.py         # API v2 connector (sleep, readiness, activity)
│       │   └── tonal.py        # Unofficial API (strength workouts, sets, reps, volume)
│       └── finance/
│           ├── csv_import.py   # Bank CSV import (Chase, BofA, Apple Card, Amex)
│           └── simplefin.py    # SimpleFIN Bridge API (automated bank syncing)
├── web/
│   ├── app.py                  # FastAPI app with routes
│   └── templates/
│       ├── base.html           # Dark mode layout
│       ├── dashboard.html      # Stats, charts, recent activity
│       ├── fitness.html        # Workout history
│       └── finance.html        # Spending breakdown
└── tests/
    ├── conftest.py             # Shared fixtures (test_session, temp_config)
    ├── test_config.py          # Config management tests (22 tests)
    ├── test_db.py              # Database model tests (24 tests)
    └── test_dedup.py           # Deduplication logic tests (22 tests)
```

## CLI Commands

```bash
# Setup
datahub init                        # Set up database
datahub config KEY VALUE            # Set config values

# Data Import
datahub import apple-health FILE    # Import Apple Health XML
datahub import bank-csv FILE        # Import bank CSV

# API Sync
datahub sync peloton                # Sync Peloton data
datahub sync oura                   # Sync Oura Ring data
datahub sync tonal                  # Sync Tonal strength workouts
datahub sync simplefin              # Sync bank transactions via SimpleFIN

# Queries
datahub status                      # Show data counts and sync status
datahub query TYPE                  # Query data by type
datahub summary [--days N]          # Fitness summary
datahub transactions [--days N]     # List transactions
datahub spending [--days N]         # Spending by category
datahub insights                    # Fitness/spending correlations

# Export
datahub export FORMAT               # Export to JSON/CSV

# Web
datahub web                         # Start web dashboard
```

## Configuration

Config is stored in `~/.datahub/config.json`. Set values with:

```bash
# Peloton credentials
datahub config peloton.username "your-email"
datahub config peloton.password "your-password"

# Oura API token (get from https://cloud.ouraring.com/personal-access-tokens)
datahub config oura.token "your-token"

# Tonal credentials (uses unofficial API - may break if Tonal changes their API)
datahub config tonal.email "your-email"
datahub config tonal.password "your-password"

# SimpleFIN Bridge (automated bank syncing - $15/year at simplefin.org)
datahub sync simplefin --setup "YOUR_SETUP_TOKEN"  # First-time setup
```

## Getting Started

```bash
# Install the package
cd /home/jontb/Vibes/datahub-project
pip install -e ".[web]"

# Initialize database
datahub init

# Verify setup
datahub status

# Start web dashboard
datahub web
# Opens at http://localhost:8000
```

## Database

SQLite database stored at `~/.datahub/datahub.db`. Three main models:

- **DataPoint** - Generic fitness data (steps, heart rate, HRV, sleep, workouts)
- **Transaction** - Financial transactions with categories
- **SyncLog** - Tracks last sync times for each connector

## Adding a New Connector

1. Create a new file in `datahub/connectors/fitness/` or `datahub/connectors/finance/`
2. Inherit from `BaseConnector` in `datahub/connectors/base.py`
3. Implement the `sync()` method
4. Register the connector in `cli.py`

## Development

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=datahub --cov-report=term-missing
```

### Test Coverage (as of 2026-02-06)

| Module | Coverage | Tests |
|--------|----------|-------|
| `datahub/config.py` | 100% | 22 |
| `datahub/db.py` | 100% | 24 |
| `datahub/dedup.py` | 97% | 22 |
| `datahub/cli.py` | 69% | 57 |
| `web/app.py` | 84% | 24 |
| `connectors/fitness/apple_health.py` | 92% | 52 |
| `connectors/finance/csv_import.py` | 95% | 10 |
| **Total** | **211 tests passing, 57% overall** | |

## Next Steps

See [TODO.md](TODO.md) for detailed roadmap.

### Near-term
- [x] Add tests for CSV import connector
- [x] Add tests for Apple Health connector
- [x] Add tests for web routes
- [x] Add tests for CLI commands
- [ ] Add tests for API connectors (Peloton, Oura, Tonal, SimpleFIN)

### Future Features
- **Calendar integration** - Google Calendar, Apple Calendar
- **Interactive charts** - Date range selection, trend comparisons
- **Natural language queries** - Ask questions about your data
- **Automation triggers** - Alerts for spending thresholds
