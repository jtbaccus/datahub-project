# AGENTS.md - DataHub

Inherit the workspace rules from `/home/jtbaccus/turing/AGENTS.md`.

## Project Profile
- Personal data hub for local-first aggregation of fitness and finance data.
- Stack: Python 3.11+, Click, Rich, SQLAlchemy, SQLite, httpx, Pydantic, FastAPI, Jinja2, HTMX, Tailwind, Chart.js, pytest.

## Commands
- `pip install -e ".[dev]"`
- `datahub init`
- `datahub web`
- `pytest tests/ -v`

## Working Rules
- Preserve the local-first, privacy-respecting architecture.
- Respect connector abstraction boundaries when adding data sources.
- Prefer targeted tests for touched modules, then broader verification if the change warrants it.
