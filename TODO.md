# DataHub Development Roadmap

## Completed

### Phase 1: Core Infrastructure
- [x] Database models (DataPoint, Transaction, SyncLog)
- [x] Configuration management with dot notation
- [x] Multi-source deduplication logic
- [x] CLI framework with Click + Rich

### Phase 2: Connectors
- [x] Apple Health XML import
- [x] Peloton API connector
- [x] Oura Ring API v2 connector
- [x] Tonal unofficial API connector
- [x] Bank CSV import (Chase, BofA, Apple Card, Amex)
- [x] SimpleFIN Bridge integration

### Phase 3: Web Dashboard
- [x] FastAPI application
- [x] Dashboard with stats and charts
- [x] Fitness history page
- [x] Finance/spending page
- [x] Dark mode Tailwind styling

### Phase 4: Test Suite (Phase 1 - Core)
- [x] Test infrastructure (conftest.py, pytest config)
- [x] `test_dedup.py` - 22 tests, 97% coverage
- [x] `test_config.py` - 22 tests, 100% coverage
- [x] `test_db.py` - 24 tests, 100% coverage

## In Progress

### Phase 4: Test Suite (Phase 2 - Connectors)
- [ ] `tests/connectors/test_csv_import.py`
  - [ ] Chase CSV format parsing
  - [ ] Bank of America format
  - [ ] Apple Card format
  - [ ] Generic format with column mapping
  - [ ] Duplicate detection (MD5 hash)
  - [ ] Date parsing formats
  - [ ] Amount parsing (currency symbols, negatives)

- [ ] `tests/connectors/test_apple_health.py`
  - [ ] XML record parsing
  - [ ] Workout extraction
  - [ ] Source detection (Apple Watch vs iPhone)
  - [ ] Duplicate prevention

## Backlog

### Phase 4: Test Suite (Phase 3 - Web & CLI)
- [ ] `tests/web/test_app.py`
  - [ ] Dashboard route (GET /)
  - [ ] Fitness route (GET /fitness)
  - [ ] Finance route (GET /finance)
  - [ ] Stats API (GET /api/stats)

- [ ] `tests/test_cli.py`
  - [ ] init command
  - [ ] status command
  - [ ] config get/set commands

### Phase 5: Enhanced Features
- [ ] Calendar integration (Google Calendar, Apple Calendar)
- [ ] Interactive charts with date range selection
- [ ] Trend comparisons across metrics
- [ ] Natural language queries
- [ ] Spending threshold alerts
- [ ] Data export improvements (more formats)

### Phase 6: Quality of Life
- [ ] CI/CD pipeline (GitHub Actions)
- [ ] Pre-commit hooks (ruff, mypy)
- [ ] API documentation (OpenAPI/Swagger)
- [ ] User documentation site

## Notes

### Deduplication Strategy
The deduplication module (`datahub/dedup.py`) handles overlapping data from multiple sources:
- Groups records by time bucket (hourly for activity, daily for sleep)
- Picks highest-priority source for each bucket
- Source priorities are data-type specific (e.g., Oura best for sleep, Apple Watch best for steps)

### Known Issues
- SQLAlchemy `datetime.utcnow()` deprecation warnings (cosmetic, from SQLAlchemy internals)
- Tonal API is unofficial and may break if they change their API

### Testing Commands
```bash
# Run all tests
.venv/bin/pytest tests/ -v

# Run specific test file
.venv/bin/pytest tests/test_dedup.py -v

# Run with coverage
.venv/bin/pytest tests/ --cov=datahub --cov-report=term-missing

# Run single test
.venv/bin/pytest tests/test_dedup.py::TestGetSourcePriority::test_known_source_steps_apple_watch -v
```
