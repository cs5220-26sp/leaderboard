import json
import os
import sqlite3
import re
from datetime import datetime, timezone

from flask import Flask, request, jsonify, render_template, g

app = Flask(__name__)
DATABASE = os.environ.get("DATABASE_PATH", "leaderboard.db")

HW3_HEADER = "===== CS5220 HW3 LEADERBOARD SUBMISSION ====="
HW3_FOOTER = "===== END CS5220 HW3 LEADERBOARD SUBMISSION ====="
HW4_HEADER = "===== CS5220 HW4 LEADERBOARD SUBMISSION ====="
HW4_FOOTER = "===== END CS5220 HW4 LEADERBOARD SUBMISSION ====="
ADMIN_KEY = os.environ.get("ADMIN_KEY", "changeme")


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
    db.execute("""
        CREATE TABLE IF NOT EXISTS hw4_submissions (
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
      parallel_performance - PE1 * 0.5 + PE2 * 0.5
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

    # Compute parallel performance
    if "PE1" in metrics and "PE2" in metrics:
        metrics["parallel_performance"] = metrics["PE1"] * 0.5 + metrics["PE2"] * 0.5

    return metrics


def validate_output(raw_output, header, footer):
    """Validate that the output has the expected structure."""
    if header not in raw_output:
        return False, "Missing header marker"
    if footer not in raw_output:
        return False, "Missing footer marker"

    name_match = re.search(r"LEADERBOARD_NAME:\s*(\S+)", raw_output)
    if not name_match:
        return False, "Missing LEADERBOARD_NAME"

    return True, name_match.group(1)


def parse_hw4_output(raw_output):
    """Parse raw job-leaderboard output for HW4 into structured metrics.

    Extracts TSC timer statistics (cycles per PE) from the PERF section:
      tsc_min  - minimum cycles across all PEs
      tsc_max  - maximum cycles across all PEs (used as 'runtime')
      tsc_mean - mean cycles across all PEs
    """
    metrics = {}

    perf_match = re.search(r"--- PERF ---\n(.*?)--- END PERF ---", raw_output, re.DOTALL)
    if perf_match:
        section = perf_match.group(1)
        min_match = re.search(r"Min:\s+([\d]+)", section)
        max_match = re.search(r"Max:\s+([\d]+)", section)
        mean_match = re.search(r"Mean:\s+([\d.]+)", section)
        if min_match:
            metrics["tsc_min"] = int(min_match.group(1))
        if max_match:
            metrics["tsc_max"] = int(max_match.group(1))
            metrics["runtime"] = metrics["tsc_max"]
        if mean_match:
            metrics["tsc_mean"] = float(mean_match.group(1))

    return metrics


@app.route("/")
def index():
    return render_template("leaderboard.html")


@app.route("/api/submit", methods=["POST"])
def submit():
    raw_output = request.get_data(as_text=True)

    valid, result = validate_output(raw_output, HW3_HEADER, HW3_FOOTER)
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

    entries.sort(key=lambda e: e["metrics"].get("parallel_performance", 0), reverse=True)
    return jsonify(entries)


def require_admin():
    key = request.headers.get("X-Admin-Key", "")
    if key != ADMIN_KEY:
        return False
    return True


@app.route("/api/admin/clear", methods=["POST"])
def clear_all():
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 401
    db = get_db()
    db.execute("DELETE FROM submissions")
    db.commit()
    return jsonify({"status": "ok", "message": "All submissions cleared"})


@app.route("/api/admin/delete/<name>", methods=["POST"])
def delete_entry(name):
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 401
    db = get_db()
    db.execute("DELETE FROM submissions WHERE name = ?", (name,))
    db.commit()
    return jsonify({"status": "ok", "message": f"Deleted {name}"})


@app.route("/hw4")
def hw4_index():
    return render_template("leaderboard_hw4.html")


@app.route("/api/hw4/submit", methods=["POST"])
def hw4_submit():
    raw_output = request.get_data(as_text=True)

    valid, result = validate_output(raw_output, HW4_HEADER, HW4_FOOTER)
    if not valid:
        return jsonify({"error": result}), 400

    name = result
    timestamp_match = re.search(r"TIMESTAMP:\s*(\S+)", raw_output)
    timestamp = timestamp_match.group(1) if timestamp_match else datetime.now(timezone.utc).isoformat()

    metrics = parse_hw4_output(raw_output)

    db = get_db()
    db.execute(
        """INSERT INTO hw4_submissions (name, timestamp, raw_output, metrics)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(name) DO UPDATE SET
               timestamp = excluded.timestamp,
               raw_output = excluded.raw_output,
               metrics = excluded.metrics""",
        (name, timestamp, raw_output, json.dumps(metrics)),
    )
    db.commit()

    return jsonify({"status": "ok", "name": name, "timestamp": timestamp})


@app.route("/api/hw4/leaderboard")
def hw4_leaderboard_data():
    db = get_db()
    rows = db.execute(
        "SELECT name, timestamp, metrics FROM hw4_submissions ORDER BY timestamp DESC"
    ).fetchall()

    entries = []
    for row in rows:
        entry = {
            "name": row["name"],
            "timestamp": row["timestamp"],
            "metrics": json.loads(row["metrics"]),
        }
        entries.append(entry)

    entries.sort(key=lambda e: e["metrics"].get("runtime", float("inf")))
    return jsonify(entries)


@app.route("/api/hw4/admin/clear", methods=["POST"])
def hw4_clear_all():
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 401
    db = get_db()
    db.execute("DELETE FROM hw4_submissions")
    db.commit()
    return jsonify({"status": "ok", "message": "All HW4 submissions cleared"})


@app.route("/api/hw4/admin/delete/<name>", methods=["POST"])
def hw4_delete_entry(name):
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 401
    db = get_db()
    db.execute("DELETE FROM hw4_submissions WHERE name = ?", (name,))
    db.commit()
    return jsonify({"status": "ok", "message": f"Deleted {name}"})


init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5220)), debug=True)
