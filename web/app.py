"""DataHub Web Dashboard."""

from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select

from datahub.config import Config
from datahub.db import get_session, DataPoint, Transaction, SyncLog
from datahub.dedup import deduplicate_daily_totals, get_deduplicated_total

app = FastAPI(title="DataHub Dashboard")

# Templates
templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=templates_dir)


def get_db():
    """Get database session."""
    config = Config()
    return get_session(config.get_db_path())


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard view."""
    session = get_db()

    # Get summary stats
    now = datetime.now()
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)
    two_months_ago = now - timedelta(days=60)

    # Steps this week (deduplicated)
    steps_week = get_deduplicated_total(session, "steps", week_ago, now)

    # Workouts this week (including Tonal strength workouts)
    workouts_week = session.execute(
        select(func.count(DataPoint.id))
        .where(DataPoint.data_type.in_(["workout", "strength_workout"]))
        .where(DataPoint.timestamp >= week_ago)
    ).scalar() or 0

    # Spending this month
    spending_month = session.execute(
        select(func.sum(Transaction.amount))
        .where(Transaction.date >= month_ago)
        .where(Transaction.amount < 0)
    ).scalar() or 0

    # Spending last month (for comparison)
    spending_last_month = session.execute(
        select(func.sum(Transaction.amount))
        .where(Transaction.date >= two_months_ago)
        .where(Transaction.date < month_ago)
        .where(Transaction.amount < 0)
    ).scalar() or 0

    # Calculate spending change percentage
    if spending_last_month and spending_last_month != 0:
        spending_change = ((spending_month - spending_last_month) / abs(spending_last_month)) * 100
    else:
        spending_change = None

    # Calories this week (deduplicated)
    active_calories_week = get_deduplicated_total(session, "active_calories", week_ago, now)
    resting_calories_week = get_deduplicated_total(session, "resting_calories", week_ago, now)
    total_calories_week = active_calories_week + resting_calories_week

    # Average sleep this week (deduplicated)
    sleep_data = deduplicate_daily_totals(session, "sleep_minutes", week_ago, now)
    avg_sleep_minutes = sum(d["total"] for d in sleep_data) / len(sleep_data) if sleep_data else 0
    avg_sleep_hours = avg_sleep_minutes / 60

    # Average HRV this week (deduplicated)
    hrv_data = deduplicate_daily_totals(session, "hrv", week_ago, now)
    avg_hrv = sum(d["total"] for d in hrv_data) / len(hrv_data) if hrv_data else 0

    # Recent data points by type
    data_by_type_rows = session.execute(
        select(
            DataPoint.data_type,
            func.count(DataPoint.id).label("count"),
        )
        .group_by(DataPoint.data_type)
        .order_by(func.count(DataPoint.id).desc())
        .limit(10)
    ).all()
    # Convert to JSON-serializable format
    data_by_type = [{"data_type": row.data_type, "count": row.count} for row in data_by_type_rows]

    # Daily steps for chart (last 14 days) - deduplicated
    daily_steps = deduplicate_daily_totals(
        session, "steps", now - timedelta(days=14), now
    )

    # Daily calories for chart (last 14 days) - deduplicated
    daily_active_cal = deduplicate_daily_totals(
        session, "active_calories", now - timedelta(days=14), now
    )
    daily_resting_cal = deduplicate_daily_totals(
        session, "resting_calories", now - timedelta(days=14), now
    )

    # Merge active and resting calories by date
    resting_by_date = {d["date"]: d["total"] for d in daily_resting_cal}
    daily_calories = []
    for d in daily_active_cal:
        daily_calories.append({
            "date": d["date"],
            "active": d["total"],
            "resting": resting_by_date.get(d["date"], 0),
            "total": d["total"] + resting_by_date.get(d["date"], 0),
        })

    # Daily sleep for trend (last 14 days)
    daily_sleep = deduplicate_daily_totals(
        session, "sleep_minutes", now - timedelta(days=14), now
    )
    # Convert to hours
    daily_sleep = [{"date": d["date"], "hours": d["total"] / 60} for d in daily_sleep]

    # Recent transactions
    recent_txns = session.execute(
        select(Transaction)
        .order_by(Transaction.date.desc())
        .limit(10)
    ).scalars().all()

    session.close()

    return templates.TemplateResponse(request, "dashboard.html", {
        "steps_week": int(steps_week),
        "workouts_week": workouts_week,
        "spending_month": abs(spending_month),
        "spending_last_month": abs(spending_last_month) if spending_last_month else 0,
        "spending_change": spending_change,
        "active_calories_week": int(active_calories_week),
        "total_calories_week": int(total_calories_week),
        "avg_sleep_hours": avg_sleep_hours,
        "avg_hrv": avg_hrv,
        "data_by_type": data_by_type,
        "daily_steps": daily_steps,
        "daily_calories": daily_calories,
        "daily_sleep": daily_sleep,
        "recent_txns": recent_txns,
    })


@app.get("/fitness", response_class=HTMLResponse)
async def fitness(request: Request):
    """Fitness data view."""
    import json
    session = get_db()

    now = datetime.now()

    # Get workout history (including strength workouts from Tonal)
    workouts = session.execute(
        select(DataPoint)
        .where(DataPoint.data_type.in_(["workout", "strength_workout"]))
        .order_by(DataPoint.timestamp.desc())
        .limit(50)
    ).scalars().all()

    # Get Tonal strength workouts specifically for detailed view
    strength_workouts = session.execute(
        select(DataPoint)
        .where(DataPoint.data_type == "strength_workout")
        .order_by(DataPoint.timestamp.desc())
        .limit(20)
    ).scalars().all()

    # Parse metadata for strength workouts
    strength_details = []
    for sw in strength_workouts:
        detail = {
            "timestamp": sw.timestamp,
            "duration": sw.value,
            "source": sw.source,
        }
        if sw.metadata_json:
            try:
                meta = json.loads(sw.metadata_json)
                detail.update(meta)
            except json.JSONDecodeError:
                pass
        strength_details.append(detail)

    # Daily summaries for last 30 days (deduplicated)
    daily_data = []
    for data_type in ["steps", "active_calories", "heart_rate"]:
        deduped = deduplicate_daily_totals(
            session, data_type, now - timedelta(days=30), now
        )
        for row in deduped:
            daily_data.append({
                "date": row["date"],
                "data_type": data_type,
                "total": row["total"],
            })
    # Sort by date descending
    daily_data.sort(key=lambda x: x["date"], reverse=True)

    # Total volume lifted (Tonal)
    total_volume = session.execute(
        select(func.sum(DataPoint.value))
        .where(DataPoint.data_type == "volume")
        .where(DataPoint.timestamp >= now - timedelta(days=30))
    ).scalar() or 0

    session.close()

    return templates.TemplateResponse(request, "fitness.html", {
        "workouts": workouts,
        "daily_data": daily_data,
        "strength_workouts": strength_details,
        "total_volume": int(total_volume),
    })


@app.get("/finance", response_class=HTMLResponse)
async def finance(request: Request):
    """Finance data view."""
    session = get_db()

    now = datetime.now()
    month_ago = now - timedelta(days=30)

    # Spending by category
    spending_by_cat_rows = session.execute(
        select(
            Transaction.category,
            func.sum(Transaction.amount).label("total"),
            func.count(Transaction.id).label("count"),
        )
        .where(Transaction.date >= month_ago)
        .where(Transaction.amount < 0)
        .group_by(Transaction.category)
        .order_by(func.sum(Transaction.amount))
    ).all()
    # Convert to JSON-serializable format
    spending_by_cat = [
        {"category": row.category or "Uncategorized", "total": row.total, "count": row.count}
        for row in spending_by_cat_rows
    ]

    # Recent transactions
    recent_txns = session.execute(
        select(Transaction)
        .order_by(Transaction.date.desc())
        .limit(50)
    ).scalars().all()

    # Daily spending
    daily_spending_rows = session.execute(
        select(
            func.date(Transaction.date).label("date"),
            func.sum(Transaction.amount).label("total"),
        )
        .where(Transaction.date >= month_ago)
        .where(Transaction.amount < 0)
        .group_by(func.date(Transaction.date))
        .order_by(func.date(Transaction.date))
    ).all()
    # Convert to JSON-serializable format
    daily_spending = [{"date": str(row.date), "total": row.total} for row in daily_spending_rows]

    session.close()

    return templates.TemplateResponse(request, "finance.html", {
        "spending_by_cat": spending_by_cat,
        "recent_txns": recent_txns,
        "daily_spending": daily_spending,
    })


@app.get("/api/stats")
async def api_stats():
    """API endpoint for dashboard stats."""
    session = get_db()

    now = datetime.now()
    week_ago = now - timedelta(days=7)

    stats = {
        "steps_week": get_deduplicated_total(session, "steps", week_ago, now),
        "data_points_total": session.execute(
            select(func.count(DataPoint.id))
        ).scalar() or 0,
        "transactions_total": session.execute(
            select(func.count(Transaction.id))
        ).scalar() or 0,
    }

    session.close()
    return stats


def run_server(host: str = "127.0.0.1", port: int = 8000):
    """Run the web server."""
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    run_server()
