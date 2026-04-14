import os
import time
import threading
import uuid
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

from decimal import Decimal

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for

from dremio_client import TABLE_PATH, VIEW_PATH, run_dml, run_query

app = Flask(__name__)
app.secret_key = "insurance-dremio-app-2025"

# Cache static assets (CSS, JS, images) for 1 hour
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 3600

# Release tag — computed once at startup (i.e. when Flask reloads after code change)
CET = timezone(timedelta(hours=1))
RELEASE_TAG = f"Release-{datetime.now(CET).strftime('%y%m%d-%H%M')}"


@app.context_processor
def _inject_release():
    return {"release_tag": RELEASE_TAG}

PUBLIC_HOST  = "ec2-52-47-189-120.eu-west-3.compute.amazonaws.com"
S3_AI_PATH   = "@S3-agoujet/ai_functions/JPEG_Cars"

# ---------------------------------------------------------------------------
# Active session tracker (in-memory, 15-minute window)
# ---------------------------------------------------------------------------

_active_sessions: dict = {}  # {session_id: last_seen_timestamp}
_SESSION_TTL = 15 * 60       # 15 minutes


@app.after_request
def _set_cache_headers(response):
    """Set cache headers based on content type."""
    if request.path.startswith("/static/"):
        # Static assets: cache 1 hour
        response.headers["Cache-Control"] = "public, max-age=3600"
    elif request.path.startswith("/api/"):
        # API data: no store — list depends on DB state (e.g. used pictures are excluded)
        response.headers["Cache-Control"] = "no-store"
    else:
        # HTML pages: no-cache but allow revalidation (browser stores it, checks freshness)
        response.headers["Cache-Control"] = "no-cache"
    return response


@app.before_request
def _track_session():
    if "sid" not in session:
        session["sid"] = str(uuid.uuid4())
    now = time.time()
    _active_sessions[session["sid"]] = now
    # Purge expired
    for k in [k for k, v in _active_sessions.items() if now - v > _SESSION_TTL]:
        del _active_sessions[k]


def _active_user_count() -> int:
    now = time.time()
    return sum(1 for v in _active_sessions.values() if now - v <= _SESSION_TTL)

# ---------------------------------------------------------------------------
# Picture server (runs in a background thread)
# ---------------------------------------------------------------------------

PICS_DIR  = Path(__file__).parent / "car_pics"
PICS_PORT = 8080


class _PicsHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        # Car pictures are static — cache for 1 hour, allow browser to revalidate
        self.send_header("Cache-Control", "public, max-age=3600, immutable")
        super().end_headers()

    def log_message(self, *args):
        pass  # silence request logs


def _start_picture_server():
    PICS_DIR.mkdir(parents=True, exist_ok=True)
    handler = lambda *a, **kw: _PicsHandler(*a, directory=str(PICS_DIR), **kw)
    try:
        server = HTTPServer(("0.0.0.0", PICS_PORT), handler)
    except OSError:
        return  # port already bound by the other reloader process — skip silently
    print(f"  [pics] serving {PICS_DIR} → http://{PUBLIC_HOST}:{PICS_PORT}/")
    server.serve_forever()


threading.Thread(target=_start_picture_server, daemon=True).start()


# ---------------------------------------------------------------------------
# S3-compatible object server (moto) — same car_pics/ folder
# ---------------------------------------------------------------------------

S3_PORT   = 9000
S3_BUCKET = "insurance-car-pics"


def _start_s3_server():
    try:
        import sys, io
        from moto.server import ThreadedMotoServer
        import boto3

        _old_stderr = sys.stderr
        sys.stderr = io.StringIO()
        try:
            server = ThreadedMotoServer(ip_address="0.0.0.0", port=S3_PORT)
            server.start()
        finally:
            sys.stderr = _old_stderr

        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://localhost:{S3_PORT}",  # boto3 bind local, accessible via public DNS
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        s3.create_bucket(Bucket=S3_BUCKET)

        PICS_DIR.mkdir(parents=True, exist_ok=True)
        for f in PICS_DIR.iterdir():
            if f.is_file():
                s3.upload_file(str(f), S3_BUCKET, f.name)
                print(f"  [s3]   uploaded {f.name}")

        print(f"  [s3]   endpoint  http://{PUBLIC_HOST}:{S3_PORT}/")
        print(f"  [s3]   bucket    s3://{S3_BUCKET}/")
        print(f"  [s3]   key/secret: test / test")

    except OSError:
        pass
    except Exception as exc:
        print(f"  [s3]   failed to start: {exc}")


threading.Thread(target=_start_s3_server, daemon=True).start()


# ---------------------------------------------------------------------------
# JSON serialisation helper (dates, Decimal, etc.)
# ---------------------------------------------------------------------------

def _serialize_row(row: dict) -> dict:
    """Convert a DB row dict to JSON-safe types."""
    out = {}
    for k, v in row.items():
        if v is None:
            out[k] = None
        elif isinstance(v, Decimal):
            out[k] = float(v)
        elif hasattr(v, 'isoformat'):
            out[k] = str(v)
        elif isinstance(v, bool):
            out[k] = v
        else:
            out[k] = v
    return out


# ---------------------------------------------------------------------------
# View query (used by home + follow case)
# ---------------------------------------------------------------------------

VIEW_SQL = f'SELECT * FROM {VIEW_PATH}."insu_open_all_case_fullinfo"'


# ---------------------------------------------------------------------------
# Home
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    recent_cases = []
    query_error = None
    try:
        recent_cases = run_query(f"{VIEW_SQL} limit 3")
    except Exception as exc:
        query_error = str(exc)
        print(f"  [dremio] home query failed: {exc}")
    return render_template("index.html", recent_cases=recent_cases, query_error=query_error,
                           active_users=_active_user_count())


# ---------------------------------------------------------------------------
# Sequential ID generator (no auto-increment in Lakehouse / Iceberg)
# ---------------------------------------------------------------------------

def _next_id(table: str, column: str, prefix: str, width: int = 3) -> str:
    """Return the next sequential ID like CUST-004, CONT-0004, CASE-004."""
    try:
        rows = run_query(f"""
            SELECT {column} FROM {TABLE_PATH}.{table}
            WHERE {column} LIKE '{prefix}%'
        """)
        max_num = 0
        for r in rows:
            val = r[column]
            # Extract trailing digits from the value
            suffix = val[len(prefix):]
            # For simple format PREFIX-NNN
            try:
                num = int(suffix)
                if num > max_num:
                    max_num = num
            except ValueError:
                # For format like CONT-0011, try stripping leading zeros
                try:
                    num = int(suffix.lstrip("0") or "0")
                    if num > max_num:
                        max_num = num
                except ValueError:
                    pass
    except Exception:
        max_num = 0
    return f"{prefix}{max_num + 1:0{width}d}"


def _next_case_number() -> str:
    """Return the next case number in CLM-YYYY-NNNN format."""
    year = datetime.utcnow().strftime("%Y")
    prefix = f"CLM-{year}-"
    try:
        rows = run_query(f"""
            SELECT case_number FROM {TABLE_PATH}.CASES
            WHERE case_number LIKE '{prefix}%'
        """)
        max_num = 0
        for r in rows:
            suffix = r["case_number"][len(prefix):]
            try:
                num = int(suffix)
                if num > max_num:
                    max_num = num
            except ValueError:
                pass
    except Exception:
        max_num = 0
    return f"{prefix}{max_num + 1:04d}"


# ---------------------------------------------------------------------------
# New Case
# ---------------------------------------------------------------------------

@app.route("/new-case", methods=["GET", "POST"])
def new_case():
    # Load customers for the dropdown
    customers = []
    try:
        customers = run_query(f"""
            SELECT customer_id, first_name, last_name, email
            FROM {TABLE_PATH}.CUSTOMERS
            ORDER BY last_name, first_name
        """)
    except Exception:
        pass

    # Load contracts for the dropdown
    contracts = []
    try:
        contracts = run_query(f"""
            SELECT contract_id, customer_id, product_name
            FROM {TABLE_PATH}.CONTRACTS
            ORDER BY contract_id
        """)
    except Exception:
        pass

    if request.method == "POST":
        def esc(v: str) -> str:
            return v.replace("'", "''")

        now         = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        today       = datetime.utcnow().strftime("%Y-%m-%d")
        case_id     = _next_id("CASES", "case_id", "CASE-")
        case_number = _next_case_number()

        customer_id   = esc(request.form["customer_id"])
        contract_id   = esc(request.form["contract_id"])
        case_type     = esc(request.form["case_type"])
        incident_date = request.form["incident_date"]
        incident_loc  = esc(request.form.get("incident_location", ""))
        description   = esc(request.form["description"])
        priority      = esc(request.form.get("priority", "MEDIUM"))
        est_amount    = request.form.get("estimated_amount", "0") or "0"
        notes         = esc(request.form.get("notes", ""))

        photo_file_name = esc(request.form.get("photo_file_name", "").strip())
        photo_file_url  = esc(request.form.get("photo_file_url", "").strip())
        photo_category  = esc(request.form.get("photo_category", "").strip())

        try:
            run_dml(f"""
                INSERT INTO {TABLE_PATH}.CASES
                    (case_id, contract_id, customer_id, case_number, case_type,
                     incident_date, report_date, incident_location, description,
                     status, priority, estimated_amount, notes, created_at, updated_at)
                VALUES
                    ('{case_id}', '{contract_id}', '{customer_id}', '{case_number}', '{case_type}',
                     DATE '{incident_date}', DATE '{today}', '{incident_loc}', '{description}',
                     'OPEN', '{priority}', {est_amount}, '{notes}', TIMESTAMP '{now}', TIMESTAMP '{now}')
            """)

            # Insert initial photo document if one was selected
            if photo_file_name:
                doc_id   = _next_id("CASE_DOCUMENTS", "document_id", "DOC-")
                size_raw = request.form.get("photo_file_size_kb", "0") or "0"
                run_dml(f"""
                    INSERT INTO {TABLE_PATH}.CASE_DOCUMENTS
                        (document_id, case_id, document_type, file_name, file_url,
                         file_size_kb, description, photo_category, uploaded_by,
                         uploaded_at, is_validated)
                    VALUES
                        ('{doc_id}', '{case_id}', 'PHOTO', '{photo_file_name}', '{photo_file_url}',
                         0, '', '{photo_category}', '',
                         TIMESTAMP '{now}', false)
                """)

            flash(f"Case {case_number} created successfully!", "success")
            return redirect(url_for("case_detail", case_number=case_number))

        except Exception as exc:
            flash(f"Error creating case: {exc}", "danger")

    return render_template("new_case.html", customers=customers, contracts=contracts)


# ---------------------------------------------------------------------------
# Follow a Case (search using the VIEW)
# ---------------------------------------------------------------------------

@app.route("/follow-case", methods=["GET", "POST"])
def follow_case():
    cases       = []
    search_term = ""

    if request.method == "POST":
        search_term = request.form.get("search", "").strip()
        if search_term:
            safe = search_term.replace("'", "''")
            try:
                upper_safe = safe.upper()
                cases = run_query(f"""
                    {VIEW_SQL}
                    WHERE UPPER(case_number) LIKE '%{upper_safe}%'
                       OR UPPER(customer)    LIKE '%{upper_safe}%'
                """)
            except Exception as exc:
                flash(f"Search error: {exc}", "danger")

    return render_template("follow_case.html", cases=cases, search_term=search_term)


# ---------------------------------------------------------------------------
# Case Detail (editable — all CASES fields)
# ---------------------------------------------------------------------------

@app.route("/case/<case_number>")
def case_detail(case_number):
    """Render the case page instantly (skeleton) — data loaded via JS + /api/case/."""
    return render_template("case_detail.html", case_number=case_number)


@app.route("/api/case/<case_number>")
def api_case_detail(case_number):
    """Return full case data as JSON (case + customer + documents + contract)."""
    safe = case_number.replace("'", "''")
    try:
        rows = run_query(f"""
            SELECT c.*,
                   cu.first_name, cu.last_name, cu.email, cu.phone,
                   cu.address_line1, cu.city, cu.zip_code, cu.country,
                   cu.date_of_birth, cu.gender, cu.risk_score
            FROM   {TABLE_PATH}.CASES     c
            JOIN   {TABLE_PATH}.CUSTOMERS cu ON c.customer_id = cu.customer_id
            WHERE  c.case_number = '{safe}'
        """)
        if not rows:
            return jsonify({"error": "Case not found"}), 404
        case = _serialize_row(rows[0])
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    documents = []
    try:
        docs = run_query(f"""
            SELECT * FROM {TABLE_PATH}.CASE_DOCUMENTS
            WHERE case_id = '{case["case_id"].replace("'", "''")}'
            ORDER BY uploaded_at DESC
        """)
        documents = [_serialize_row(d) for d in docs]
    except Exception:
        pass

    contract = None
    try:
        contracts = run_query(f"""
            SELECT * FROM {TABLE_PATH}.CONTRACTS
            WHERE contract_id = '{case["contract_id"].replace("'", "''")}'
        """)
        if contracts:
            contract = _serialize_row(contracts[0])
    except Exception:
        pass

    return jsonify({"case": case, "documents": documents, "contract": contract})


# ---------------------------------------------------------------------------
# Update Case (all editable fields)
# ---------------------------------------------------------------------------

@app.route("/case/<case_number>/update", methods=["POST"])
def update_case(case_number):
    def esc(v: str) -> str:
        return v.replace("'", "''")

    now  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    safe = case_number.replace("'", "''")

    status        = esc(request.form.get("status", ""))
    priority      = esc(request.form.get("priority", ""))
    case_type     = esc(request.form.get("case_type", ""))
    incident_date = request.form.get("incident_date", "")
    incident_loc  = esc(request.form.get("incident_location", ""))
    description   = esc(request.form.get("description", ""))
    est_amount    = request.form.get("estimated_amount", "0") or "0"
    app_amount    = request.form.get("approved_amount", "0") or "0"
    agent         = esc(request.form.get("assigned_agent", ""))
    notes         = esc(request.form.get("notes", ""))

    try:
        run_dml(f"""
            UPDATE {TABLE_PATH}.CASES
            SET    status             = '{status}',
                   priority           = '{priority}',
                   case_type          = '{case_type}',
                   incident_date      = DATE '{incident_date}',
                   incident_location  = '{incident_loc}',
                   description        = '{description}',
                   estimated_amount   = {est_amount},
                   approved_amount    = {app_amount},
                   assigned_agent     = '{agent}',
                   notes              = '{notes}',
                   updated_at         = TIMESTAMP '{now}'
            WHERE  case_number = '{safe}'
        """)
        flash("Case updated successfully.", "success")
    except Exception as exc:
        flash(f"Error updating case: {exc}", "danger")

    return redirect(url_for("case_detail", case_number=case_number))


# ---------------------------------------------------------------------------
# Add Document to a Case
# ---------------------------------------------------------------------------

@app.route("/case/<case_number>/add-document", methods=["POST"])
def add_document(case_number):
    def esc(v: str) -> str:
        return v.replace("'", "''")

    safe = case_number.replace("'", "''")
    now  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    doc_id = _next_id("CASE_DOCUMENTS", "document_id", "DOC-")

    # Get case_id from case_number
    try:
        rows = run_query(f"SELECT case_id FROM {TABLE_PATH}.CASES WHERE case_number = '{safe}'")
        if not rows:
            flash("Case not found.", "warning")
            return redirect(url_for("follow_case"))
        case_id = rows[0]["case_id"]
    except Exception as exc:
        flash(f"Error: {exc}", "danger")
        return redirect(url_for("case_detail", case_number=case_number))

    doc_type       = esc(request.form.get("document_type", "PHOTO"))
    file_name      = esc(request.form.get("file_name", ""))
    file_url       = esc(request.form.get("file_url", ""))
    file_size_kb   = request.form.get("file_size_kb", "0") or "0"
    description    = esc(request.form.get("doc_description", ""))
    photo_category = esc(request.form.get("photo_category", ""))
    uploaded_by    = esc(request.form.get("uploaded_by", ""))

    try:
        run_dml(f"""
            INSERT INTO {TABLE_PATH}.CASE_DOCUMENTS
                (document_id, case_id, document_type, file_name, file_url,
                 file_size_kb, description, photo_category, uploaded_by,
                 uploaded_at, is_validated)
            VALUES
                ('{doc_id}', '{case_id}', '{doc_type}', '{file_name}', '{file_url}',
                 {file_size_kb}, '{description}', '{photo_category}', '{uploaded_by}',
                 TIMESTAMP '{now}', false)
        """)
        flash(f"Document '{file_name}' added.", "success")
    except Exception as exc:
        flash(f"Error adding document: {exc}", "danger")

    return redirect(url_for("case_detail", case_number=case_number))


# ---------------------------------------------------------------------------
# Remove Document from a Case
# ---------------------------------------------------------------------------

@app.route("/case/<case_number>/remove-document/<document_id>", methods=["POST"])
def remove_document(case_number, document_id):
    safe_doc_id = document_id.replace("'", "''")
    try:
        run_dml(f"""
            DELETE FROM {TABLE_PATH}.CASE_DOCUMENTS
            WHERE document_id = '{safe_doc_id}'
        """)
        flash("Document removed.", "success")
    except Exception as exc:
        flash(f"Error removing document: {exc}", "danger")
    return redirect(url_for("case_detail", case_number=case_number))


# ---------------------------------------------------------------------------
# API: list pictures from the local HTTP server (car_pics/)
# ---------------------------------------------------------------------------

@app.route("/api/pictures")
def api_pictures():
    PICS_DIR.mkdir(parents=True, exist_ok=True)

    # Exclude pictures already linked to any case document
    used_names = set()
    try:
        rows = run_query(f"SELECT file_name FROM {TABLE_PATH}.CASE_DOCUMENTS")
        used_names = {r["file_name"] for r in rows}
    except Exception:
        pass  # table may not exist yet

    pics = []
    for f in sorted(PICS_DIR.iterdir()):
        if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
            if f.name in used_names:
                continue  # already linked to a case
            size_kb = f.stat().st_size // 1024
            pics.append({
                "name": f.name,
                "url": f"http://{PUBLIC_HOST}:{PICS_PORT}/{f.name}",
                "size_kb": size_kb,
            })
    return jsonify(pics)


# ---------------------------------------------------------------------------
# AI Analysis of a document image (Dremio AI_GENERATE)
# ---------------------------------------------------------------------------

@app.route("/case/<case_number>/ai-analyze/<document_id>", methods=["POST"])
def ai_analyze_document(case_number, document_id):
    safe_doc_id = document_id.replace("'", "''")

    # Get file_name for this document
    try:
        rows = run_query(f"SELECT file_name FROM {TABLE_PATH}.CASE_DOCUMENTS WHERE document_id = '{safe_doc_id}'")
        if not rows:
            return jsonify({"error": "Document not found"}), 404
        file_name = rows[0]["file_name"]
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    # Run Dremio AI_GENERATE on the S3 file
    safe_file = file_name.replace("'", "''")
    try:
        ai_rows = run_query(f"""
            SELECT
                AI_GENERATE (
                    ROW('Analyse this JPEG car crash image. Extract: (1) the car vendor/brand name if visible, (2) a short description of the nature of the crash, (3) choose the most appropriate photo_category from this list only: DAMAGE, OVERVIEW, DETAIL, SCENE, DOCUMENT.', file)
                    WITH SCHEMA ROW (car_vendor VARCHAR, crash_nature VARCHAR, photo_category VARCHAR)
                ) AS JPEG_CAR_CRASH_OUTPUT
            FROM TABLE (list_files('{S3_AI_PATH}/{safe_file}')) as file
        """)
        if not ai_rows:
            return jsonify({"error": "No result returned by AI_GENERATE"}), 500

        output = ai_rows[0].get("JPEG_CAR_CRASH_OUTPUT", {})
        if isinstance(output, dict):
            car_vendor    = output.get("car_vendor", "Unknown") or "Unknown"
            crash_nature  = output.get("crash_nature", "Unknown") or "Unknown"
            photo_cat     = output.get("photo_category", "") or ""
        else:
            car_vendor, crash_nature, photo_cat = str(output), "—", ""

        description = f"Car: {car_vendor} | Crash: {crash_nature}"

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    # Update description (and photo_category if returned) in CASE_DOCUMENTS
    safe_desc = description.replace("'", "''")
    set_clause = f"description = '{safe_desc}'"
    valid_cats = {"DAMAGE", "OVERVIEW", "DETAIL", "SCENE", "DOCUMENT"}
    if photo_cat.upper() in valid_cats:
        set_clause += f", photo_category = '{photo_cat.upper()}'"
    try:
        run_dml(f"""
            UPDATE {TABLE_PATH}.CASE_DOCUMENTS
            SET {set_clause}
            WHERE document_id = '{safe_doc_id}'
        """)
        return jsonify({"description": description, "photo_category": photo_cat.upper() if photo_cat.upper() in valid_cats else None})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=5000)
