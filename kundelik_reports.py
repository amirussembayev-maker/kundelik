import json
import os
import re
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import gspread
from gspread.exceptions import APIError, WorksheetNotFound


APP_TIMEZONE = ZoneInfo(os.getenv("REPORT_TIMEZONE", "Asia/Almaty"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "parent_site"))

REGISTRY_SPREADSHEET_ID = os.getenv("REGISTRY_SPREADSHEET_ID", "1yRrq9yIO4RdJmAKWFxq7L5yR2haitdkorzs_yU9l5_I")
REGISTRY_SHEET_NAMES = [
    item.strip()
    for item in (os.getenv("REGISTRY_SHEET_NAMES") or "IELTS,Pre-IELTS,Pre-IELTS VIP,NUET,SAT").split(",")
    if item.strip()
]
ERRORS_SHEET_NAME = os.getenv("ERRORS_SHEET_NAME", "Errors")
BASE_URL = (os.getenv("BASE_URL") or "").rstrip("/")

GRADE_SOURCE_SPREADSHEET_IDS = [
    item.strip()
    for item in (os.getenv("GRADE_SOURCE_SPREADSHEET_IDS") or "").split(",")
    if item.strip()
]
ATTENDANCE_SOURCE_SPREADSHEET_IDS = [
    item.strip()
    for item in (os.getenv("ATTENDANCE_SOURCE_SPREADSHEET_IDS") or "").split(",")
    if item.strip()
]

GRADE_SHEET_EXCLUDE = {
    item.strip().lower()
    for item in (os.getenv("GRADE_SHEET_EXCLUDE") or "errors").split(",")
    if item.strip()
}
ATTENDANCE_SHEET_EXCLUDE = {
    item.strip().lower()
    for item in (os.getenv("ATTENDANCE_SHEET_EXCLUDE") or "").split(",")
    if item.strip()
}


@dataclass
class RegistryRow:
    sheet_name: str
    worksheet: Any
    row_number: int
    header_map: dict[str, int]


@dataclass
class StudentRecord:
    hash: str
    name: str
    group: str = ""
    product: str = ""
    subject: str = ""
    status: str = ""
    link: str = ""
    updated_at: str = ""
    registry: RegistryRow | None = None
    courses: list[dict[str, Any]] = field(default_factory=list)
    attendance_lessons: list[dict[str, Any]] = field(default_factory=list)
    attendance_summary: dict[str, Any] = field(default_factory=dict)


def gs_retry(func, *args, retries=6, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except APIError as exc:
            if "429" in str(exc) and attempt < retries - 1:
                wait = 20 + attempt * 15
                print(f"Rate limit hit, waiting {wait}s...")
                time.sleep(wait)
                continue
            raise


def normalize_lookup(value: Any) -> str:
    return (
        str(value or "")
        .lower()
        .replace("\n", " ")
        .replace("ё", "е")
        .replace("–", "-")
        .replace("—", "-")
        .strip()
    )


def normalize_name(value: Any) -> str:
    return re.sub(r"\s+", " ", normalize_lookup(value))


def canonical_header(value: Any) -> str:
    normalized = normalize_lookup(value)
    aliases = {
        "hash": ["hash", "хеш ученика", "хеш ученика (не менять!)", "student hash"],
        "name": ["фио ученика", "full name", "name", "student", "student name"],
        "group": ["группа", "group", "flow"],
        "product": ["product", "продукт"],
        "subject": ["предмет", "subject"],
        "status": ["статус", "status"],
        "link": ["ссылка", "link", "url"],
        "updated_at": ["последнее обновление", "updated at", "last updated"],
        "role": ["role", "роль"],
        "meeting_name": ["meeting name", "lesson", "урок", "meeting"],
        "meeting_date": ["meeting date", "date", "дата", "lesson date"],
        "duration": ["duration", "длительность"],
        "email": ["email", "почта"],
    }
    for key, variants in aliases.items():
        if normalized in variants:
            return key
    return ""


def score_header_row(row: list[str]) -> int:
    return sum(1 for cell in row if canonical_header(cell))


def read_table(worksheet) -> tuple[list[str], dict[str, int], list[dict[str, Any]]]:
    values = gs_retry(worksheet.get_all_values)
    if not values:
        return [], {}, []

    header_index = 0
    best_score = -1
    for idx, row in enumerate(values[:10]):
        score = score_header_row(row)
        if score > best_score:
            best_score = score
            header_index = idx

    headers = [str(value or "").strip() for value in values[header_index]]
    header_map: dict[str, int] = {}
    for idx, header in enumerate(headers):
        key = canonical_header(header)
        if key and key not in header_map:
            header_map[key] = idx

    rows = []
    for raw_index, raw in enumerate(values[header_index + 1 :], start=header_index + 2):
        if not "".join(raw).strip():
            continue
        item = {}
        for idx, header in enumerate(headers):
            key = canonical_header(header) or header
            item[key] = raw[idx] if idx < len(raw) else ""
        rows.append({"row_number": raw_index, "raw": raw, "values": item})

    return headers, header_map, rows


def parse_numeric(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("%", "").replace(",", ".")
    if not text or text == "-":
        return None
    try:
        return round(float(text), 1)
    except ValueError:
        return None


def generate_hash() -> str:
    seed = f"{datetime.now().timestamp()}-{os.getpid()}-{os.urandom(4).hex()}"
    return seed.encode().hex()[:8]


def format_datetime(dt: datetime) -> str:
    return dt.astimezone(APP_TIMEZONE).strftime("%d.%m.%Y %H:%M")


def escape_html(value: Any) -> str:
    return (
        str(value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def init_google_client() -> gspread.Client:
    google_json = os.getenv("GOOGLE_JSON")
    if not google_json:
        raise RuntimeError("Missing GOOGLE_JSON")
    return gspread.service_account_from_dict(json.loads(google_json))


def build_registry(gc: gspread.Client) -> tuple[gspread.Spreadsheet, dict[str, StudentRecord], list[list[str]]]:
    spreadsheet = gs_retry(gc.open_by_key, REGISTRY_SPREADSHEET_ID)
    students: dict[str, StudentRecord] = {}
    errors: list[list[str]] = []

    for sheet_name in REGISTRY_SHEET_NAMES:
        try:
            worksheet = gs_retry(spreadsheet.worksheet, sheet_name)
        except WorksheetNotFound:
            errors.append([sheet_name, "", "Registry sheet not found"])
            continue

        _, header_map, rows = read_table(worksheet)
        print(f"Registry sheet {sheet_name}: {len(rows)} rows")
        print(f"Header map {sheet_name}: {header_map}")

        if "name" not in header_map:
            errors.append([sheet_name, "", 'Missing column "ФИО ученика"'])
            continue

        pending_hash_updates = []
        for row in rows:
            values = row["values"]
            name = str(values.get("name", "")).strip()
            if not name:
                continue

            key = normalize_name(name)
            student_hash = str(values.get("hash", "")).strip() or generate_hash()

            record = StudentRecord(
                hash=student_hash,
                name=name,
                group=str(values.get("group", "")).strip(),
                product=str(values.get("product", "")).strip(),
                subject=str(values.get("subject", "")).strip(),
                status=str(values.get("status", "")).strip(),
                link=str(values.get("link", "")).strip(),
                updated_at=str(values.get("updated_at", "")).strip(),
                registry=RegistryRow(
                    sheet_name=sheet_name,
                    worksheet=worksheet,
                    row_number=row["row_number"],
                    header_map=header_map,
                ),
            )

            if key in students:
                errors.append([sheet_name, name, f"Duplicate student in registry. Already exists in {students[key].registry.sheet_name}"])
                continue

            students[key] = record

            if not str(values.get("hash", "")).strip() and "hash" in header_map:
                pending_hash_updates.append((row["row_number"], header_map["hash"] + 1, student_hash))

        for row_idx, col_idx, value in pending_hash_updates:
            gs_retry(worksheet.update_cell, row_idx, col_idx, value)

    return spreadsheet, students, errors


def detect_course_label(spreadsheet: gspread.Spreadsheet, sheet_name: str) -> str:
    return spreadsheet.title.strip() or sheet_name


def collect_grades(gc: gspread.Client, students: dict[str, StudentRecord], errors: list[list[str]]) -> None:
    for spreadsheet_id in GRADE_SOURCE_SPREADSHEET_IDS:
        spreadsheet = gs_retry(gc.open_by_key, spreadsheet_id)
        worksheets = gs_retry(spreadsheet.worksheets)

        for worksheet in worksheets:
            if worksheet.title.lower() in GRADE_SHEET_EXCLUDE:
                continue

            try:
                headers, header_map, rows = read_table(worksheet)
            except APIError as exc:
                errors.append([spreadsheet.title, worksheet.title, f"Read error: {exc}"])
                continue

            if "name" not in header_map:
                continue

            ignored_indexes = {
                idx
                for key, idx in header_map.items()
                if key in {"hash", "name", "group", "product", "subject", "status", "link", "updated_at", "email"}
            }
            topic_columns = [
                (idx, header)
                for idx, header in enumerate(headers)
                if header and idx not in ignored_indexes and normalize_lookup(header) != "email"
            ]
            if not topic_columns:
                continue

            course_label = detect_course_label(spreadsheet, worksheet.title)
            for row in rows:
                student_name = str(row["values"].get("name", "")).strip()
                if not student_name:
                    continue

                key = normalize_name(student_name)
                if key not in students:
                    continue

                topics = []
                for index, topic_header in topic_columns:
                    raw_value = row["raw"][index] if index < len(row["raw"]) else ""
                    score = parse_numeric(raw_value)
                    if score is None:
                        continue
                    topics.append(
                        {
                            "topic": re.sub(r"\s+", " ", topic_header).strip(),
                            "score": score,
                        }
                    )

                if not topics:
                    continue

                average = round(sum(item["score"] for item in topics) / len(topics), 1)
                students[key].courses.append(
                    {
                        "course": course_label,
                        "sheet": worksheet.title,
                        "average": average,
                        "topics": topics,
                    }
                )


def normalize_attendance_status(status: str) -> str:
    normalized = normalize_lookup(status)
    if "late" in normalized and "left early" in normalized:
        return "Late + Left early"
    if "late" in normalized:
        return "Late"
    if "left early" in normalized or "partial" in normalized:
        return "Partial"
    if "absent" in normalized or "missed" in normalized:
        return "Absent"
    return "Present"


def collect_attendance(gc: gspread.Client, students: dict[str, StudentRecord]) -> None:
    for spreadsheet_id in ATTENDANCE_SOURCE_SPREADSHEET_IDS:
        spreadsheet = gs_retry(gc.open_by_key, spreadsheet_id)
        worksheets = gs_retry(spreadsheet.worksheets)

        for worksheet in worksheets:
            if worksheet.title.lower() in ATTENDANCE_SHEET_EXCLUDE:
                continue

            _, header_map, rows = read_table(worksheet)
            if "name" not in header_map or "status" not in header_map:
                continue

            for row in rows:
                values = row["values"]
                student_name = str(values.get("name", "")).strip()
                if not student_name:
                    continue

                key = normalize_name(student_name)
                if key not in students:
                    continue

                students[key].attendance_lessons.append(
                    {
                        "date": str(values.get("meeting_date") or values.get("date") or ""),
                        "lesson": str(values.get("meeting_name") or worksheet.title or ""),
                        "status": normalize_attendance_status(str(values.get("status", "")).strip()),
                        "duration": str(values.get("duration") or ""),
                        "source": spreadsheet.title,
                    }
                )


def finalize_students(students: dict[str, StudentRecord]) -> list[StudentRecord]:
    generated_at = format_datetime(datetime.now(APP_TIMEZONE))
    items = []
    for student in students.values():
        student.courses.sort(key=lambda item: (item["course"], item["sheet"]))
        student.attendance_lessons.sort(key=lambda item: str(item.get("date", "")), reverse=True)

        total_lessons = len(student.attendance_lessons)
        present = sum(1 for item in student.attendance_lessons if item["status"] == "Present")
        late = sum(1 for item in student.attendance_lessons if item["status"] == "Late")
        partial = sum(1 for item in student.attendance_lessons if item["status"] == "Partial")
        absent = sum(1 for item in student.attendance_lessons if item["status"] == "Absent")
        participated = present + late + partial
        attendance_rate = round((participated / total_lessons) * 100) if total_lessons else None

        averages = [course["average"] for course in student.courses]
        overall_average = round(sum(averages) / len(averages), 1) if averages else None
        topic_count = sum(len(course["topics"]) for course in student.courses)

        student.link = f"{BASE_URL}/{student.hash}/" if BASE_URL else student.link
        student.updated_at = generated_at
        student.subject = student.subject or student.product
        student.status = student.status or "Active"
        student.attendance_summary = {
            "total_lessons": total_lessons,
            "present": present,
            "late": late,
            "partial": partial,
            "absent": absent,
            "attendance_rate": attendance_rate,
            "lessons": student.attendance_lessons[:15],
            "summary": {
                "overall_average": overall_average,
                "topic_count": topic_count,
                "course_count": len(student.courses),
                "attendance_rate": attendance_rate,
                "lessons_tracked": total_lessons,
            },
        }
        items.append(student)

    return sorted(items, key=lambda item: item.name.lower())


def render_student_html(student: StudentRecord) -> str:
    summary = student.attendance_summary.get("summary", {})
    course_sections = "".join(
        f"""
        <section class="course-block">
          <div class="course-meta">
            <div>
              <h3>{escape_html(course['course'])}</h3>
              <p>{escape_html(course['sheet'])}</p>
            </div>
            <div class="score-chip">{course['average']}/100</div>
          </div>
          <div class="topic-grid">
            {''.join(f'<div class="topic-row"><span>{escape_html(topic["topic"])}</span><strong>{topic["score"]}</strong></div>' for topic in course["topics"])}
          </div>
        </section>
        """
        for course in student.courses
    ) or '<section class="course-block empty-block">По этому ученику пока не найдено grades.</section>'

    lesson_rows = "".join(
        f"""
        <tr>
          <td>{escape_html(lesson['date']) or '—'}</td>
          <td>{escape_html(lesson['lesson'])}</td>
          <td>{escape_html(lesson['status'])}</td>
          <td>{escape_html(lesson['duration']) or '—'}</td>
        </tr>
        """
        for lesson in student.attendance_summary.get("lessons", [])
    ) or '<tr><td colspan="4">Attendance по ученику пока не найден.</td></tr>'

    hero_subject = student.subject or student.product or "Student Report"
    hero_product = student.product or student.subject or "Academic Progress"

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{escape_html(student.name)} • WeGlobal Reports</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg: #f5f5f7;
      --panel: rgba(255, 255, 255, 0.82);
      --panel-strong: #ffffff;
      --text: #1d1d1f;
      --muted: #6e6e73;
      --line: rgba(0, 0, 0, 0.08);
      --accent: #0071e3;
      --accent-soft: rgba(0, 113, 227, 0.10);
      --success: #34c759;
      --warning: #ff9f0a;
      --danger: #ff453a;
      --shadow: 0 16px 40px rgba(0, 0, 0, 0.07);
      --radius: 28px;
      --radius-sm: 18px;
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      font-family: 'Inter', sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(0, 113, 227, 0.10), transparent 22%),
        radial-gradient(circle at top right, rgba(255, 255, 255, 0.60), transparent 18%),
        linear-gradient(180deg, #fbfbfd 0%, var(--bg) 100%);
    }}
    .topbar {{
      position: sticky;
      top: 0;
      z-index: 10;
      backdrop-filter: blur(20px);
      background: rgba(251, 251, 253, 0.72);
      border-bottom: 1px solid var(--line);
    }}
    .topbar-inner, .hero, .section, .footer {{
      width: min(1120px, calc(100% - 32px));
      margin: 0 auto;
    }}
    .topbar-inner {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 14px 0;
      gap: 16px;
    }}
    .brand {{
      font-weight: 700;
      font-size: 20px;
      letter-spacing: -0.02em;
    }}
    .brand span {{ color: var(--muted); font-weight: 500; }}
    .top-meta {{
      display: flex;
      gap: 20px;
      align-items: center;
      color: var(--muted);
      font-size: 13px;
    }}
    .hero {{
      padding: 56px 0 26px;
      text-align: center;
    }}
    .eyebrow {{
      color: var(--accent);
      font-size: 14px;
      font-weight: 600;
      letter-spacing: -0.01em;
      margin-bottom: 12px;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(42px, 6vw, 72px);
      line-height: 0.98;
      letter-spacing: -0.05em;
      font-weight: 800;
    }}
    .hero-copy {{
      margin: 18px auto 0;
      max-width: 760px;
      font-size: clamp(20px, 2.4vw, 30px);
      line-height: 1.2;
      letter-spacing: -0.03em;
      color: var(--muted);
    }}
    .hero-actions {{
      display: flex;
      gap: 12px;
      justify-content: center;
      flex-wrap: wrap;
      margin-top: 24px;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 132px;
      padding: 12px 20px;
      border-radius: 999px;
      text-decoration: none;
      font-weight: 600;
      transition: transform .18s ease, box-shadow .18s ease, background .18s ease;
    }}
    .btn.primary {{
      background: var(--accent);
      color: #fff;
      box-shadow: 0 10px 24px rgba(0, 113, 227, 0.24);
    }}
    .btn.secondary {{
      color: var(--accent);
      border: 1px solid rgba(0, 113, 227, 0.30);
      background: rgba(255,255,255,0.75);
    }}
    .btn:hover {{
      transform: translateY(-1px);
    }}
    .hero-panel {{
      margin-top: 34px;
      background: linear-gradient(180deg, rgba(255,255,255,0.86) 0%, rgba(255,255,255,0.74) 100%);
      border: 1px solid rgba(255,255,255,0.9);
      border-radius: 36px;
      box-shadow: var(--shadow);
      padding: 28px;
    }}
    .identity-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 18px;
      text-align: left;
    }}
    .identity-card {{
      padding: 18px 18px 20px;
      border-radius: 22px;
      background: rgba(255,255,255,0.86);
      border: 1px solid rgba(0,0,0,0.05);
    }}
    .label {{
      display: block;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--muted);
      margin-bottom: 10px;
      font-weight: 600;
    }}
    .value {{
      font-size: 24px;
      line-height: 1.18;
      letter-spacing: -0.03em;
      font-weight: 700;
    }}
    .section {{
      padding: 18px 0 64px;
    }}
    .stat-strip {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 18px;
      margin-bottom: 18px;
    }}
    .stat-card, .panel {{
      background: var(--panel);
      backdrop-filter: blur(20px);
      border: 1px solid rgba(255,255,255,0.88);
      border-radius: 28px;
      box-shadow: var(--shadow);
    }}
    .stat-card {{
      padding: 22px 18px;
      text-align: center;
    }}
    .stat-card .value {{
      font-size: clamp(30px, 4vw, 44px);
    }}
    .content-grid {{
      display: grid;
      grid-template-columns: 1.16fr 0.84fr;
      gap: 18px;
      align-items: start;
    }}
    .panel {{
      padding: 24px;
    }}
    .panel-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
    }}
    .panel h2 {{
      margin: 0;
      font-size: clamp(26px, 2.4vw, 38px);
      letter-spacing: -0.04em;
      font-weight: 700;
    }}
    .panel-subtle {{
      color: var(--muted);
      font-size: 14px;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 68px;
      padding: 8px 12px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-weight: 700;
      font-size: 13px;
      white-space: nowrap;
    }}
    .course-block {{
      padding: 22px;
      border-radius: 24px;
      background: var(--panel-strong);
      border: 1px solid var(--line);
      margin-bottom: 14px;
    }}
    .course-meta {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 16px;
    }}
    .course-meta h3 {{
      margin: 0;
      font-size: 24px;
      line-height: 1.05;
      letter-spacing: -0.03em;
    }}
    .course-meta p {{
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 14px;
    }}
    .score-chip {{
      background: #f0f6ff;
      color: var(--accent);
      padding: 10px 14px;
      border-radius: 999px;
      font-weight: 700;
      white-space: nowrap;
    }}
    .topic-grid {{
      display: grid;
      gap: 10px;
    }}
    .topic-row {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      border-radius: 18px;
      background: #fafafc;
      border: 1px solid var(--line);
      padding: 14px 16px;
      font-size: 15px;
    }}
    .topic-row span {{
      color: var(--text);
      max-width: 82%;
    }}
    .topic-row strong {{
      font-size: 15px;
      font-weight: 700;
    }}
    .metric-list {{
      display: grid;
      gap: 12px;
      margin-bottom: 18px;
    }}
    .metric-box {{
      padding: 18px;
      border-radius: 22px;
      background: var(--panel-strong);
      border: 1px solid var(--line);
    }}
    .metric-box .value {{
      font-size: 36px;
      margin-top: 6px;
    }}
    .table-wrap {{
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 22px;
      background: var(--panel-strong);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 520px;
    }}
    th, td {{
      padding: 14px 16px;
      text-align: left;
      border-top: 1px solid var(--line);
      font-size: 14px;
    }}
    th {{
      border-top: 0;
      background: #fafafc;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.04em;
      font-size: 12px;
      font-weight: 600;
    }}
    .empty-block {{
      color: var(--muted);
    }}
    .footer {{
      padding-bottom: 40px;
      text-align: center;
      color: var(--muted);
      font-size: 13px;
    }}
    @media (max-width: 980px) {{
      .identity-grid, .stat-strip, .content-grid {{
        grid-template-columns: 1fr;
      }}
      .course-meta, .panel-head, .topbar-inner {{
        flex-direction: column;
        align-items: flex-start;
      }}
      .top-meta {{
        gap: 8px;
        flex-direction: column;
        align-items: flex-start;
      }}
    }}
  </style>
</head>
<body>
  <div class="topbar">
    <div class="topbar-inner">
      <div class="brand">WeGlobal <span>Reports</span></div>
      <div class="top-meta">
        <span>{escape_html(hero_product)}</span>
        <span>Обновлено: {escape_html(student.updated_at)}</span>
      </div>
    </div>
  </div>

  <section class="hero">
    <div class="eyebrow">Student Performance Report</div>
    <h1>{escape_html(student.name)}</h1>
    <div class="hero-copy">{escape_html(hero_subject)}. Чистая сводка по успеваемости, посещаемости и прогрессу по курсам.</div>
    <div class="hero-actions">
      <a class="btn primary" href="#grades">Открыть grades</a>
      <a class="btn secondary" href="#attendance">Открыть attendance</a>
    </div>
    <div class="hero-panel">
      <div class="identity-grid">
        <div class="identity-card"><span class="label">ФИО</span><div class="value">{escape_html(student.name)}</div></div>
        <div class="identity-card"><span class="label">Группа</span><div class="value">{escape_html(student.group or '—')}</div></div>
        <div class="identity-card"><span class="label">Product</span><div class="value">{escape_html(student.product or '—')}</div></div>
        <div class="identity-card"><span class="label">Статус</span><div class="value">{escape_html(student.status or '—')}</div></div>
      </div>
    </div>
  </section>

  <section class="section">
    <div class="stat-strip">
      <div class="stat-card"><span class="label">Средний grade</span><div class="value">{summary.get('overall_average') if summary.get('overall_average') is not None else '—'}</div></div>
      <div class="stat-card"><span class="label">Темы</span><div class="value">{summary.get('topic_count', 0)}</div></div>
      <div class="stat-card"><span class="label">Attendance</span><div class="value">{str(summary.get('attendance_rate')) + '%' if summary.get('attendance_rate') is not None else '—'}</div></div>
      <div class="stat-card"><span class="label">Уроки</span><div class="value">{summary.get('lessons_tracked', 0)}</div></div>
    </div>

    <div class="content-grid">
      <div class="panel" id="grades">
        <div class="panel-head">
          <div>
            <h2>Courses & Grades</h2>
            <div class="panel-subtle">Индивидуальные результаты по всем найденным курсам ученика.</div>
          </div>
          <div class="pill">{summary.get('course_count', 0)} курс(ов)</div>
        </div>
        {course_sections}
      </div>

      <div class="panel" id="attendance">
        <div class="panel-head">
          <div>
            <h2>Attendance</h2>
            <div class="panel-subtle">Сводка по посещаемости и последние найденные уроки.</div>
          </div>
          <div class="pill">{student.attendance_summary.get('total_lessons', 0)} уроков</div>
        </div>

        <div class="metric-list">
          <div class="metric-box"><span class="label">Посещено</span><div class="value">{student.attendance_summary.get('present', 0)}</div></div>
          <div class="metric-box"><span class="label">Late</span><div class="value">{student.attendance_summary.get('late', 0)}</div></div>
          <div class="metric-box"><span class="label">Partial</span><div class="value">{student.attendance_summary.get('partial', 0)}</div></div>
          <div class="metric-box"><span class="label">Absent</span><div class="value">{student.attendance_summary.get('absent', 0)}</div></div>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Дата</th>
                <th>Урок</th>
                <th>Статус</th>
                <th>Duration</th>
              </tr>
            </thead>
            <tbody>{lesson_rows}</tbody>
          </table>
        </div>
      </div>
    </div>
  </section>

  <div class="footer">© 2026 WeGlobal • Individual student report</div>
</body>
</html>
"""


def render_index_html(students: list[StudentRecord]) -> str:
    cards = "".join(
        f"""
        <a class="student-card" href="./{student.hash}/">
          <div>
            <strong>{escape_html(student.name)}</strong>
            <span>{escape_html(student.group or 'Без группы')}</span>
          </div>
          <div class="card-meta">
            <b>{student.attendance_summary.get('summary', {}).get('overall_average', '—')}</b>
            <small>grade</small>
          </div>
        </a>
        """
        for student in students
    )
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>WeGlobal Reports</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    body {{
      margin: 0;
      font-family: 'Inter', sans-serif;
      color: #1d1d1f;
      background: linear-gradient(180deg, #fbfbfd 0%, #f5f5f7 100%);
    }}
    .topbar {{
      position: sticky;
      top: 0;
      backdrop-filter: blur(20px);
      background: rgba(251, 251, 253, 0.72);
      border-bottom: 1px solid rgba(0,0,0,0.08);
    }}
    .topbar-inner, .hero, .grid-wrap {{
      width: min(1120px, calc(100% - 32px));
      margin: 0 auto;
    }}
    .topbar-inner {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 14px 0;
    }}
    .brand {{ font-size: 20px; font-weight: 700; letter-spacing: -0.02em; }}
    .brand span {{ color: #6e6e73; font-weight: 500; }}
    .hero {{
      text-align: center;
      padding: 64px 0 24px;
    }}
    .eyebrow {{
      color: #0071e3;
      font-size: 14px;
      font-weight: 600;
      margin-bottom: 12px;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(42px, 6vw, 72px);
      line-height: 0.98;
      letter-spacing: -0.05em;
    }}
    .hero p {{
      margin: 18px auto 0;
      max-width: 760px;
      color: #6e6e73;
      font-size: clamp(20px, 2.4vw, 30px);
      line-height: 1.2;
      letter-spacing: -0.03em;
    }}
    .grid-wrap {{
      padding-bottom: 54px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 16px;
    }}
    .student-card {{
      display: flex;
      justify-content: space-between;
      gap: 14px;
      text-decoration: none;
      color: inherit;
      background: rgba(255,255,255,0.82);
      backdrop-filter: blur(20px);
      border: 1px solid rgba(255,255,255,0.88);
      border-radius: 24px;
      box-shadow: 0 16px 40px rgba(0,0,0,0.07);
      padding: 20px;
      transition: transform .18s ease, box-shadow .18s ease;
    }}
    .student-card:hover {{
      transform: translateY(-2px);
      box-shadow: 0 20px 48px rgba(0,0,0,0.10);
    }}
    .student-card strong {{
      display: block;
      font-size: 20px;
      line-height: 1.08;
      letter-spacing: -0.03em;
    }}
    .student-card span, .student-card small {{
      color: #6e6e73;
    }}
    .card-meta {{
      text-align: right;
    }}
    .card-meta b {{
      display: block;
      font-size: 28px;
      color: #0071e3;
    }}
  </style>
</head>
<body>
  <div class="topbar">
    <div class="topbar-inner">
      <div class="brand">WeGlobal <span>Reports</span></div>
      <div>Updated: {escape_html(format_datetime(datetime.now(APP_TIMEZONE)))}</div>
    </div>
  </div>
  <section class="hero">
    <div class="eyebrow">Student Performance Reports</div>
    <h1>WeGlobal Reports</h1>
    <p>Accurate individual report pages for each student, with grades and attendance separated by personal link.</p>
  </section>
  <div class="grid-wrap">
    <div class="grid">{cards}</div>
  </div>
</body>
</html>
"""


def export_site(students: list[StudentRecord]) -> None:
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "data").mkdir(parents=True, exist_ok=True)

    for student in students:
        student_dir = OUTPUT_DIR / student.hash
        student_dir.mkdir(parents=True, exist_ok=True)
        (student_dir / "index.html").write_text(render_student_html(student), encoding="utf-8")
        payload = {
            "hash": student.hash,
            "name": student.name,
            "group": student.group,
            "product": student.product,
            "subject": student.subject,
            "status": student.status,
            "url": student.link,
            "updated_at": student.updated_at,
            "courses": student.courses,
            "attendance": student.attendance_summary,
        }
        (OUTPUT_DIR / "data" / f"{student.hash}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    index_payload = {
        "generated_at": format_datetime(datetime.now(APP_TIMEZONE)),
        "students": [
            {
                "hash": student.hash,
                "name": student.name,
                "group": student.group,
                "product": student.product,
                "subject": student.subject,
                "status": student.status,
                "url": student.link,
                "updated_at": student.updated_at,
            }
            for student in students
        ],
    }
    (OUTPUT_DIR / "data" / "students.json").write_text(
        json.dumps(index_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (OUTPUT_DIR / "index.html").write_text(render_index_html(students), encoding="utf-8")


def update_registry_rows(students: list[StudentRecord]) -> None:
    for student in students:
        registry = student.registry
        if not registry:
            continue
        link_col = registry.header_map.get("link")
        updated_col = registry.header_map.get("updated_at")
        if link_col is not None:
            gs_retry(registry.worksheet.update_cell, registry.row_number, link_col + 1, student.link)
        if updated_col is not None:
            gs_retry(registry.worksheet.update_cell, registry.row_number, updated_col + 1, student.updated_at)


def write_errors_sheet(spreadsheet, errors: list[list[str]]) -> None:
    try:
        worksheet = gs_retry(spreadsheet.worksheet, ERRORS_SHEET_NAME)
    except WorksheetNotFound:
        worksheet = gs_retry(spreadsheet.add_worksheet, title=ERRORS_SHEET_NAME, rows=1000, cols=3)

    values = [["Sheet", "Student", "Error"]]
    values.extend(errors if errors else [["", "", "No errors"]])
    gs_retry(worksheet.clear)
    gs_retry(worksheet.update, f"A1:C{len(values)}", values)


def main() -> None:
    print("=== WeGlobal Reports: start ===")
    gc = init_google_client()
    print("Google auth: OK")

    registry_spreadsheet, registry_students, errors = build_registry(gc)
    print(f"Registry loaded: {len(registry_students)} students")

    collect_grades(gc, registry_students, errors)
    print("Grades collected")

    collect_attendance(gc, registry_students)
    print("Attendance collected")

    students = finalize_students(registry_students)
    export_site(students)
    print(f"Site exported to {OUTPUT_DIR}")

    update_registry_rows(students)
    write_errors_sheet(registry_spreadsheet, errors)
    print("Registry updated")
    print("=== WeGlobal Reports: done ===")


if __name__ == "__main__":
    main()
