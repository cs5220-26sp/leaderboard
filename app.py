import json
import sqlite3
import re
from datetime import datetime, timezone

from flask import Flask, request, jsonify, render_template, g

app = Flask(__name__)
DATABASE = "leaderboard.db"

HEADER = "===== CS5220 HW3 LEADERBOARD SUBMISSION ====="
FOOTER = "===== END CS5220 HW3 LEADERBOARD SUBMISSION ====="


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = sqlite3.connect(DATABASE)
    db.execute("""
        CREATE TABLE IF NOT EXISTS submissions (
            name TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            raw_output TEXT NOT NULL,
            metrics TEXT NOT NULL DEFAULT '{}'
        )
    """)
    db.commit()
    db.close()


def extract_times(section_text):
    """Extract all 'Simulation Time = X seconds' values from a section."""
    return [float(m) for m in re.findall(r"Simulation Time\s*=\s*([\d.eE+-]+)\s*seconds", section_text)]


def parse_output(raw_output):
    """Parse raw job-leaderboard output into structured metrics.

    Extracts:
      RS1e5  - serial runtime (seconds) from SERIAL section
      PE1    - parallel efficiency from 64 to 128 tasks (2M particles)
      PE2    - parallel efficiency from 128 to 256 tasks (2M particles)
      overall - PE1 * 0.4 + PE2 * 0.4 + (1/RS1e5) * 0.2
    """
    metrics = {}

    # Parse SERIAL section → RS1e5
    serial_match = re.search(r"--- SERIAL ---\n(.*?)--- END SERIAL ---", raw_output, re.DOTALL)
    if serial_match:
        times = extract_times(serial_match.group(1))
        if times:
            metrics["RS1e5"] = times[0]

    # Parse SCALE_2M section → PE1, PE2
    # Order of runs: N1×64 (64 tasks), N2×64 (128 tasks), N2×128 (256 tasks)
    scale_2m_match = re.search(r"--- SCALE_2M ---\n(.*?)--- END SCALE_2M ---", raw_output, re.DOTALL)
    if scale_2m_match:
        times = extract_times(scale_2m_match.group(1))
        if len(times) >= 3:
            t64, t128, t256 = times[0], times[1], times[2]
            metrics["T_2M_64"] = t64
            metrics["T_2M_128"] = t128
            metrics["T_2M_256"] = t256
            if t128 > 0:
                metrics["PE1"] = t64 / (2 * t128)
            if t256 > 0:
                metrics["PE2"] = t128 / (2 * t256)

    # Compute overall score
    if "PE1" in metrics and "PE2" in metrics and "RS1e5" in metrics and metrics["RS1e5"] > 0:
        rs = 1.0 / metrics["RS1e5"]
        metrics["overall"] = metrics["PE1"] * 0.4 + metrics["PE2"] * 0.4 + rs * 0.2

    return metrics


def validate_output(raw_output):
    """Validate that the output has the expected structure."""
    if HEADER not in raw_output:
        return False, "Missing header marker"
    if FOOTER not in raw_output:
        return False, "Missing footer marker"

    name_match = re.search(r"LEADERBOARD_NAME:\s*(\S+)", raw_output)
    if not name_match:
        return False, "Missing LEADERBOARD_NAME"

    return True, name_match.group(1)


@app.route("/")
def index():
    return render_template("leaderboard.html")


@app.route("/api/submit", methods=["POST"])
def submit():
    raw_output = request.get_data(as_text=True)

    valid, result = validate_output(raw_output)
    if not valid:
        return jsonify({"error": result}), 400

    name = result
    timestamp_match = re.search(r"TIMESTAMP:\s*(\S+)", raw_output)
    timestamp = timestamp_match.group(1) if timestamp_match else datetime.now(timezone.utc).isoformat()

    metrics = parse_output(raw_output)

    db = get_db()
    db.execute(
        """INSERT INTO submissions (name, timestamp, raw_output, metrics)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(name) DO UPDATE SET
               timestamp = excluded.timestamp,
               raw_output = excluded.raw_output,
               metrics = excluded.metrics""",
        (name, timestamp, raw_output, json.dumps(metrics)),
    )
    db.commit()

    return jsonify({"status": "ok", "name": name, "timestamp": timestamp})


@app.route("/api/leaderboard")
def leaderboard_data():
    db = get_db()
    rows = db.execute(
        "SELECT name, timestamp, metrics FROM submissions ORDER BY timestamp DESC"
    ).fetchall()

    entries = []
    for row in rows:
        entry = {
            "name": row["name"],
            "timestamp": row["timestamp"],
            "metrics": json.loads(row["metrics"]),
        }
        entries.append(entry)

    entries.sort(key=lambda e: e["metrics"].get("overall", 0), reverse=True)
    return jsonify(entries)


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5220, debug=True)
