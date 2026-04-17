import json
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import gspread


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
    for item in (os.getenv("GRADE_SHEET_EXCLUDE") or "").split(",")
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
        "activity_score": ["activity score", "активность"],
    }
    for key, variants in aliases.items():
        if normalized in variants:
            return key
    return ""


def score_header_row(row: list[str]) -> int:
    return sum(1 for cell in row if canonical_header(cell))


def read_table(worksheet) -> tuple[list[str], dict[str, int], list[dict[str, Any]]]:
    values = worksheet.get_all_display_values()
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
    spreadsheet = gc.open_by_key(REGISTRY_SPREADSHEET_ID)
    students: dict[str, StudentRecord] = {}
    errors: list[list[str]] = []

    for sheet_name in REGISTRY_SHEET_NAMES:
        worksheet = spreadsheet.worksheet(sheet_name)
        _, header_map, rows = read_table(worksheet)

        if "name" not in header_map:
            errors.append([sheet_name, "", 'Missing column "ФИО ученика"'])
            continue

        hash_updates = []
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
                hash_updates.append((row["row_number"], header_map["hash"] + 1, student_hash))

        for row_idx, col_idx, value in hash_updates:
            worksheet.update_cell(row_idx, col_idx, value)

    return spreadsheet, students, errors


def detect_course_label(spreadsheet: gspread.Spreadsheet, sheet_name: str) -> str:
    return spreadsheet.title.strip() or sheet_name


def collect_grades(gc: gspread.Client, students: dict[str, StudentRecord], errors: list[list[str]]) -> None:
    for spreadsheet_id in GRADE_SOURCE_SPREADSHEET_IDS:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        for worksheet in spreadsheet.worksheets():
            if worksheet.title.lower() in GRADE_SHEET_EXCLUDE:
                continue

            headers, header_map, rows = read_table(worksheet)
            if "name" not in header_map:
                continue

            ignored_indexes = {
                idx for key, idx in header_map.items()
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
                    topics.append({
                        "topic": re.sub(r"\s+", " ", topic_header).strip(),
                        "score": score,
                    })

                if not topics:
                    continue

                average = round(sum(item["score"] for item in topics) / len(topics), 1)
                students[key].courses.append({
                    "course": course_label,
                    "sheet": worksheet.title,
                    "average": average,
                    "topics": topics,
                })


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
        spreadsheet = gc.open_by_key(spreadsheet_id)
        for worksheet in spreadsheet.worksheets():
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

                students[key].attendance_lessons.append({
                    "date": str(values.get("meeting_date") or ""),
                    "lesson": str(values.get("meeting_name") or worksheet.title),
                    "status": normalize_attendance_status(str(values.get("status", ""))),
                    "duration": str(values.get("duration") or ""),
                    "source": spreadsheet.title,
                })


def finalize_students(students: dict[str, StudentRecord]) -> list[StudentRecord]:
    generated_at = format_datetime(datetime.now(APP_TIMEZONE))
    result = []

    for student in students.values():
        student.courses.sort(key=lambda item: (item["course"], item["sheet"]))
        student.attendance_lessons.sort(key=lambda item: str(item.get("date", "")), reverse=True)

        total = len(student.attendance_lessons)
        present = sum(1 for item in student.attendance_lessons if item["status"] == "Present")
        late = sum(1 for item in student.attendance_lessons if item["status"] == "Late")
        partial = sum(1 for item in student.attendance_lessons if item["status"] == "Partial")
        absent = sum(1 for item in student.attendance_lessons if item["status"] == "Absent")
        attended = present + late + partial
        attendance_rate = round((attended / total) * 100) if total else None

        averages = [course["average"] for course in student.courses]
        overall_average = round(sum(averages) / len(averages), 1) if averages else None
        topic_count = sum(len(course["topics"]) for course in student.courses)

        student.link = f"{BASE_URL}/{student.hash}/" if BASE_URL else student.link
        student.updated_at = generated_at
        student.subject = student.subject or student.product
        student.status = student.status or "Active"
        student.attendance_summary = {
            "total_lessons": total,
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
                "lessons_tracked": total,
            },
        }
        result.append(student)

    return sorted(result, key=lambda item: item.name.lower())


def render_student_html(student: StudentRecord) -> str:
    summary = student.attendance_summary.get("summary", {})
    courses_html = "".join(
        f"""
        <section class="course-card">
          <div class="course-head">
            <div>
              <h3>{escape_html(course['course'])}</h3>
              <p>{escape_html(course['sheet'])}</p>
            </div>
            <div class="badge">{course['average']}/100</div>
          </div>
          <div class="topic-list">
            {''.join(f'<div class="topic-row"><span>{escape_html(topic["topic"])}</span><strong>{topic["score"]}</strong></div>' for topic in course["topics"])}
          </div>
        </section>
        """
        for course in student.courses
    ) or '<section class="course-card empty">Оценки пока не найдены.</section>'

    lessons_html = "".join(
        f"""
        <tr>
          <td>{escape_html(lesson['date']) or '—'}</td>
          <td>{escape_html(lesson['lesson'])}</td>
          <td>{escape_html(lesson['status'])}</td>
          <td>{escape_html(lesson['duration']) or '—'}</td>
        </tr>
        """
        for lesson in student.attendance_summary.get("lessons", [])
    ) or '<tr><td colspan="4">Attendance пока не найден.</td></tr>'

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{escape_html(student.name)} — iqra kids</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800;900&display=swap" rel="stylesheet">
  <style>
    :root {{
      --blue: #59ade7;
      --blue-dark: #3b97d5;
      --pink: #ff6794;
      --bg: #f4f8fd;
      --card: #ffffff;
      --text: #34465f;
      --muted: #7d8ba1;
      --line: #e3edf7;
      --green: #69cd77;
      --orange: #f3b54c;
      --shadow: 0 16px 36px rgba(71, 120, 171, 0.12);
      --radius: 22px;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: 'Nunito', sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(89, 173, 231, 0.12), transparent 22%),
        radial-gradient(circle at top right, rgba(255, 103, 148, 0.10), transparent 16%),
        linear-gradient(180deg, #f8fbff 0%, var(--bg) 100%);
    }}
    .header {{
      background: linear-gradient(135deg, var(--blue) 0%, var(--blue-dark) 100%);
      color: white;
      padding: 22px 16px 32px;
    }}
    .header-inner, .main, .footer {{ max-width: 820px; margin: 0 auto; }}
    .header-inner {{ display: flex; justify-content: space-between; gap: 16px; align-items: center; }}
    .brand {{ font-size: 34px; font-weight: 900; letter-spacing: -0.03em; }}
    .brand span {{ font-size: 17px; background: var(--pink); border-radius: 10px; padding: 4px 9px; margin-left: 4px; }}
    .updated {{ text-align: right; font-size: 13px; font-weight: 700; }}
    .updated strong {{ display: block; font-size: 22px; }}
    .main {{ padding: 18px 16px 40px; }}
    h1 {{ text-align: center; color: var(--blue); margin: 8px 0 20px; font-size: 38px; }}
    .card, .course-card, .panel {{ background: var(--card); border-radius: var(--radius); box-shadow: var(--shadow); }}
    .card {{ padding: 18px; margin-bottom: 16px; }}
    .grid-2, .stats {{ display: grid; gap: 14px; }}
    .grid-2 {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .info span {{ display: block; font-size: 12px; color: var(--muted); text-transform: uppercase; font-weight: 800; margin-bottom: 4px; }}
    .info strong {{ color: var(--blue); font-size: 24px; }}
    .stats {{ grid-template-columns: repeat(4, minmax(0, 1fr)); margin-bottom: 18px; }}
    .stat {{ background: var(--card); border-radius: 18px; padding: 16px 12px; box-shadow: var(--shadow); text-align: center; border-bottom: 4px solid var(--blue); }}
    .stat.orange {{ border-bottom-color: var(--orange); }}
    .stat.green {{ border-bottom-color: var(--green); }}
    .stat span {{ display: block; color: var(--muted); font-size: 12px; font-weight: 800; text-transform: uppercase; }}
    .stat strong {{ display: block; margin-top: 6px; font-size: 34px; color: var(--blue-dark); }}
    .layout {{ display: grid; gap: 16px; grid-template-columns: 1.2fr 0.8fr; }}
    .panel {{ padding: 18px; }}
    .panel-head, .course-head {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 14px; }}
    .panel h2, .course-card h3 {{ margin: 0; }}
    .course-card {{ padding: 18px; margin-bottom: 14px; }}
    .course-head p {{ margin: 4px 0 0; color: var(--muted); }}
    .badge {{ border-radius: 999px; background: #edf7ff; color: var(--blue-dark); padding: 8px 12px; font-weight: 900; white-space: nowrap; }}
    .topic-list {{ display: grid; gap: 10px; }}
    .topic-row {{ display: flex; justify-content: space-between; gap: 12px; padding: 12px 14px; background: #f9fbfe; border: 1px solid var(--line); border-radius: 16px; }}
    .mini-list {{ display: grid; gap: 10px; }}
    .mini {{ background: #f9fbfe; border: 1px solid var(--line); border-radius: 16px; padding: 14px; }}
    .mini strong {{ display: block; font-size: 24px; margin-top: 4px; }}
    .mini span {{ color: var(--muted); font-size: 12px; text-transform: uppercase; font-weight: 800; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 520px; }}
    .table-wrap {{ overflow: auto; border: 1px solid var(--line); border-radius: 16px; }}
    th, td {{ padding: 12px 14px; text-align: left; border-top: 1px solid var(--line); }}
    th {{ border-top: 0; background: #f1f8fe; color: var(--blue-dark); font-size: 12px; text-transform: uppercase; }}
    .empty {{ color: var(--muted); }}
    .footer {{ padding: 0 16px 28px; text-align: center; color: var(--muted); font-size: 13px; font-weight: 700; }}
    @media (max-width: 860px) {{
      .layout, .grid-2, .stats {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 30px; }}
    }}
  </style>
</head>
<body>
  <header class="header">
    <div class="header-inner">
      <div class="brand">iqra <span>kids</span></div>
      <div class="updated">Дата обновления<strong>{escape_html(student.updated_at)}</strong></div>
    </div>
  </header>
  <main class="main">
    <h1>Отчёт об успеваемости</h1>
    <section class="card">
      <div class="grid-2">
        <div class="info"><span>ФИО</span><strong>{escape_html(student.name)}</strong></div>
        <div class="info"><span>Предмет</span><strong>{escape_html(student.subject or student.product or '—')}</strong></div>
        <div class="info"><span>Группа</span><strong>{escape_html(student.group or '—')}</strong></div>
        <div class="info"><span>Product</span><strong>{escape_html(student.product or '—')}</strong></div>
      </div>
    </section>
    <section class="stats">
      <div class="stat"><span>Средний grade</span><strong>{summary.get('overall_average') if summary.get('overall_average') is not None else '—'}</strong></div>
      <div class="stat orange"><span>Темы</span><strong>{summary.get('topic_count', 0)}</strong></div>
      <div class="stat green"><span>Attendance</span><strong>{str(summary.get('attendance_rate')) + '%' if summary.get('attendance_rate') is not None else '—'}</strong></div>
      <div class="stat"><span>Уроки</span><strong>{summary.get('lessons_tracked', 0)}</strong></div>
    </section>
    <div class="layout">
      <section class="panel">
        <div class="panel-head"><h2>Курсы и оценки</h2><div class="badge">{summary.get('course_count', 0)} курс(ов)</div></div>
        {courses_html}
      </section>
      <div>
        <section class="panel">
          <div class="panel-head"><h2>Attendance</h2><div class="badge">{student.attendance_summary.get('total_lessons', 0)} уроков</div></div>
          <div class="mini-list">
            <div class="mini"><span>Посещено</span><strong>{student.attendance_summary.get('present', 0)}</strong></div>
            <div class="mini"><span>Late</span><strong>{student.attendance_summary.get('late', 0)}</strong></div>
            <div class="mini"><span>Partial</span><strong>{student.attendance_summary.get('partial', 0)}</strong></div>
            <div class="mini"><span>Absent</span><strong>{student.attendance_summary.get('absent', 0)}</strong></div>
          </div>
        </section>
        <section class="panel">
          <div class="panel-head"><h2>Последние уроки</h2></div>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Дата</th><th>Урок</th><th>Статус</th><th>Duration</th></tr></thead>
              <tbody>{lessons_html}</tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
  </main>
  <footer class="footer">© 2026 iqra kids • отчёт обновляется автоматически из Google Sheets</footer>
</body>
</html>
"""


def render_index_html(students: list[StudentRecord]) -> str:
    cards = "".join(
        f"""
        <a class="card" href="./{student.hash}/">
          <div>
            <strong>{escape_html(student.name)}</strong>
            <span>{escape_html(student.group or 'Без группы')}</span>
          </div>
          <div class="meta">
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
  <title>iqra kids reports</title>
  <style>
    body {{ margin: 0; font-family: Arial, sans-serif; background: linear-gradient(180deg, #f7fbff 0%, #eef6fd 100%); color: #24405e; }}
    .wrap {{ max-width: 980px; margin: 0 auto; padding: 40px 16px; }}
    h1 {{ font-size: 36px; margin: 0 0 8px; color: #4babe7; }}
    p {{ color: #70839a; }}
    .grid {{ display: grid; gap: 14px; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); margin-top: 24px; }}
    .card {{ text-decoration: none; color: inherit; background: white; border-radius: 18px; padding: 18px; box-shadow: 0 16px 36px rgba(76, 132, 184, 0.10); display: flex; justify-content: space-between; gap: 12px; }}
    .card strong {{ display: block; font-size: 18px; }}
    .card span, .card small {{ color: #7a8ca2; }}
    .meta {{ text-align: right; }}
    .meta b {{ display: block; font-size: 24px; color: #4babe7; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>iqra kids reports</h1>
    <p>Обновлено: {escape_html(format_datetime(datetime.now(APP_TIMEZONE)))}</p>
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
            registry.worksheet.update_cell(registry.row_number, link_col + 1, student.link)
        if updated_col is not None:
            registry.worksheet.update_cell(registry.row_number, updated_col + 1, student.updated_at)


def write_errors_sheet(spreadsheet, errors: list[list[str]]) -> None:
    try:
        worksheet = spreadsheet.worksheet(ERRORS_SHEET_NAME)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=ERRORS_SHEET_NAME, rows=1000, cols=3)

    values = [["Sheet", "Student", "Error"]]
    values.extend(errors if errors else [["", "", "No errors"]])
    worksheet.clear()
    worksheet.update(f"A1:C{len(values)}", values)


def main() -> None:
    print("=== Kundelik Reports: start ===")
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
    print("=== Kundelik Reports: done ===")


if __name__ == "__main__":
    main()
