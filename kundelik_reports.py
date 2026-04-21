import hashlib
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

MONTH_NAMES_RU = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
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
            message = str(exc)
            transient_codes = ("429", "500", "502", "503", "504")
            if any(code in message for code in transient_codes) and attempt < retries - 1:
                wait = min(90, 10 * (attempt + 1))
                print(f"Transient Google API error ({message}). Waiting {wait}s before retry...")
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


def parse_datetime_value(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text or text == "-":
        return None
    patterns = [
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%m/%d/%Y, %I:%M:%S %p",
        "%m/%d/%Y, %I:%M %p",
    ]
    for pattern in patterns:
        try:
            return datetime.strptime(text, pattern).replace(tzinfo=APP_TIMEZONE)
        except ValueError:
            continue
    return None


def extract_lesson_datetime_from_title(title: str) -> datetime | None:
    match = re.search(r"(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})", str(title))
    if match:
        return parse_datetime_value(match.group(1))
    return None


def round_ielts_band(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value * 2) / 2


def format_number(value: float | int | None) -> str:
    if value is None:
        return "—"
    if isinstance(value, int):
        return str(value)
    if abs(value - round(value)) < 1e-9:
        return str(int(round(value)))
    return f"{value:.1f}"


def month_key_to_label(month_key: str) -> str:
    parts = month_key.split("-")
    if len(parts) != 2:
        return month_key
    year = int(parts[0])
    month = int(parts[1])
    return f"{MONTH_NAMES_RU.get(month, month_key)} {year}"


def month_key_from_datetime(dt: datetime | None) -> str:
    if not dt:
        return "Без даты"
    return dt.strftime("%Y-%m")


def status_to_ru(status: str) -> str:
    normalized = normalize_lookup(status)
    if "late" in normalized and "left early" in normalized:
        return "Опоздал и вышел раньше"
    if "late" in normalized:
        return "Опоздал"
    if "left early" in normalized or "partial" in normalized:
        return "Частично"
    if "absent" in normalized or "missed" in normalized:
        return "Отсутствовал"
    if "teacher" in normalized:
        return "Преподаватель"
    return "Посетил"


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


def build_unique_hash(name: str, used_hashes: set[str]) -> str:
    while True:
        seed = f"{name}|{datetime.now(APP_TIMEZONE).isoformat()}|{os.urandom(8).hex()}|{time.time_ns()}"
        candidate = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:8]
        if candidate not in used_hashes:
            used_hashes.add(candidate)
            return candidate


def init_google_client() -> gspread.Client:
    google_json = os.getenv("GOOGLE_JSON")
    if not google_json:
        raise RuntimeError("Missing GOOGLE_JSON")
    return gspread.service_account_from_dict(json.loads(google_json))


def build_registry(gc: gspread.Client) -> tuple[gspread.Spreadsheet, dict[str, StudentRecord], list[list[str]]]:
    spreadsheet = gs_retry(gc.open_by_key, REGISTRY_SPREADSHEET_ID)
    students: dict[str, StudentRecord] = {}
    errors: list[list[str]] = []
    used_hashes: set[str] = set()

    for sheet_name in REGISTRY_SHEET_NAMES:
        try:
            worksheet = gs_retry(spreadsheet.worksheet, sheet_name)
        except WorksheetNotFound:
            errors.append([sheet_name, "", "Не найден registry-лист"])
            continue

        _, header_map, rows = read_table(worksheet)
        print(f"Registry sheet {sheet_name}: {len(rows)} rows")
        print(f"Header map {sheet_name}: {header_map}")

        if "name" not in header_map:
            errors.append([sheet_name, "", 'Не найдена колонка "ФИО ученика"'])
            continue

        pending_updates = []
        for row in rows:
            values = row["values"]
            name = str(values.get("name", "")).strip()
            if not name:
                continue

            key = normalize_name(name)
            raw_hash = str(values.get("hash", "")).strip()
            if not raw_hash or raw_hash in used_hashes:
                student_hash = build_unique_hash(name, used_hashes)
                if "hash" in header_map:
                    pending_updates.append((row["row_number"], header_map["hash"] + 1, student_hash))
            else:
                student_hash = raw_hash
                used_hashes.add(student_hash)

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
                errors.append([sheet_name, name, f"Дубликат ученика в registry. Уже есть в {students[key].registry.sheet_name}"])
                continue

            students[key] = record

        for row_idx, col_idx, value in pending_updates:
            gs_retry(worksheet.update_cell, row_idx, col_idx, value)

    return spreadsheet, students, errors


def course_display_name(spreadsheet_title: str) -> str:
    normalized = normalize_lookup(spreadsheet_title)
    if "ielts mock new" in normalized or "ielts mock" in normalized:
        return "IELTS MOCK TESTS"
    if "_mock" in normalized or "progress report mock" in normalized:
        return "NUET MOCK TESTS"
    return spreadsheet_title.strip()


def extract_sort_tuple(title: str) -> tuple[int, int, str]:
    normalized = normalize_lookup(title)
    hw_match = re.search(r"\bhw\s*(\d+)", normalized)
    mock_match = re.search(r"\bmock\s*(\d+)", normalized)
    if hw_match:
        return (0, int(hw_match.group(1)), normalized)
    if mock_match:
        return (1, int(mock_match.group(1)), normalized)
    return (2, 9999, normalized)


def parse_mock_header(header: str) -> tuple[int | None, str]:
    normalized = re.sub(r"\s+", " ", str(header).strip())
    match = re.search(r"MOCK\s*(\d+)\s*\|\s*(.+)", normalized, re.IGNORECASE)
    if not match:
        return None, normalized
    mock_no = int(match.group(1))
    part = match.group(2).strip()
    return mock_no, part


def normalize_mock_part(part: str) -> str:
    normalized = normalize_lookup(part)
    if "read" in normalized:
        return "reading"
    if "liste" in normalized:
        return "listening"
    if "writi" in normalized:
        return "writing"
    if "spea" in normalized:
        return "speaking"
    if "over" in normalized:
        return "overall"
    if "critic" in normalized:
        return "critic"
    if "math" in normalized:
        return "math"
    return normalized


def build_general_course(spreadsheet, worksheet, row, topic_columns):
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
                "sort_key": extract_sort_tuple(topic_header),
                "is_hw": normalize_lookup(topic_header).startswith("hw"),
                "is_mock": normalize_lookup(topic_header).startswith("mock"),
            }
        )

    if not topics:
        return None

    topics.sort(key=lambda item: item["sort_key"])
    homework_topics = [item for item in topics if item["is_hw"]]
    average = round(sum(item["score"] for item in homework_topics) / len(homework_topics), 1) if homework_topics else None

    return {
        "kind": "homework",
        "course": course_display_name(spreadsheet.title),
        "sheet": worksheet.title,
        "average": average,
        "topics": topics,
        "homework_count": len(homework_topics),
        "mock_count": 0,
    }


def build_ielts_mock_course(spreadsheet, worksheet, row, topic_columns):
    mocks: dict[int, dict[str, Any]] = {}
    for index, topic_header in topic_columns:
        raw_value = row["raw"][index] if index < len(row["raw"]) else ""
        score = parse_numeric(raw_value)
        if score is None:
            continue
        mock_no, part = parse_mock_header(topic_header)
        if mock_no is None:
            continue
        bucket = mocks.setdefault(
            mock_no,
            {
                "number": mock_no,
                "reading": None,
                "listening": None,
                "writing": None,
                "speaking": None,
                "overall": None,
            },
        )
        bucket[normalize_mock_part(part)] = score

    if not mocks:
        return None

    mock_items = []
    overall_values = []
    for mock_no in sorted(mocks):
        item = mocks[mock_no]
        if item.get("overall") is not None:
            overall_values.append(item["overall"])
        else:
            parts = [item.get(name) for name in ("reading", "listening", "writing", "speaking") if item.get(name) is not None]
            if parts:
                item["overall"] = round_ielts_band(sum(parts) / len(parts))
                overall_values.append(item["overall"])
        mock_items.append(item)

    average_band = round_ielts_band(sum(overall_values) / len(overall_values)) if overall_values else None

    return {
        "kind": "ielts_mock",
        "course": "IELTS MOCK TESTS",
        "sheet": worksheet.title,
        "average_band": average_band,
        "mock_count": len(mock_items),
        "mocks": mock_items,
        "average": None,
        "homework_count": 0,
    }


def build_nuet_mock_course(spreadsheet, worksheet, row, topic_columns):
    mocks: dict[int, dict[str, Any]] = {}
    for index, topic_header in topic_columns:
        raw_value = row["raw"][index] if index < len(row["raw"]) else ""
        score = parse_numeric(raw_value)
        if score is None:
            continue
        mock_no, part = parse_mock_header(topic_header)
        if mock_no is None:
            continue
        bucket = mocks.setdefault(mock_no, {"number": mock_no, "critic": None, "math": None})
        bucket[normalize_mock_part(part)] = score

    if not mocks:
        return None

    mock_items = [mocks[number] for number in sorted(mocks)]
    return {
        "kind": "nuet_mock",
        "course": "NUET MOCK TESTS",
        "sheet": worksheet.title,
        "mock_count": len(mock_items),
        "mocks": mock_items,
        "average": None,
        "homework_count": 0,
    }


def collect_grades(gc: gspread.Client, students: dict[str, StudentRecord], errors: list[list[str]]) -> None:
    for spreadsheet_id in GRADE_SOURCE_SPREADSHEET_IDS:
        try:
            spreadsheet = gs_retry(gc.open_by_key, spreadsheet_id)
            worksheets = gs_retry(spreadsheet.worksheets)
        except APIError as exc:
            errors.append([spreadsheet_id, "", f"Не удалось открыть grade spreadsheet: {exc}"])
            continue

        title_normalized = normalize_lookup(spreadsheet.title)
        is_ielts_mock_book = "ielts mock" in title_normalized
        is_nuet_mock_book = "_mock" in title_normalized or "progress report mock" in title_normalized

        for worksheet in worksheets:
            if worksheet.title.lower() in GRADE_SHEET_EXCLUDE:
                continue

            try:
                headers, header_map, rows = read_table(worksheet)
            except APIError as exc:
                errors.append([spreadsheet.title, worksheet.title, f"Ошибка чтения: {exc}"])
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

            for row in rows:
                student_name = str(row["values"].get("name", "")).strip()
                if not student_name:
                    continue

                key = normalize_name(student_name)
                if key not in students:
                    continue

                if is_ielts_mock_book:
                    course = build_ielts_mock_course(spreadsheet, worksheet, row, topic_columns)
                elif is_nuet_mock_book:
                    course = build_nuet_mock_course(spreadsheet, worksheet, row, topic_columns)
                else:
                    course = build_general_course(spreadsheet, worksheet, row, topic_columns)

                if course:
                    students[key].courses.append(course)


def collect_attendance(gc: gspread.Client, students: dict[str, StudentRecord]) -> None:
    for spreadsheet_id in ATTENDANCE_SOURCE_SPREADSHEET_IDS:
        try:
            spreadsheet = gs_retry(gc.open_by_key, spreadsheet_id)
            worksheets = gs_retry(spreadsheet.worksheets)
        except APIError:
            continue

        for worksheet in worksheets:
            if worksheet.title.lower() in ATTENDANCE_SHEET_EXCLUDE:
                continue

            try:
                _, header_map, rows = read_table(worksheet)
            except APIError:
                continue

            if "name" not in header_map:
                continue

            current_lesson_name = worksheet.title
            current_lesson_dt = extract_lesson_datetime_from_title(worksheet.title)

            for row in rows:
                values = row["values"]
                name_value = str(values.get("name", "")).strip()
                role_value = str(values.get("role", "")).strip()

                if name_value and "|" in name_value and not role_value:
                    current_lesson_name = name_value
                    current_lesson_dt = extract_lesson_datetime_from_title(name_value) or current_lesson_dt
                    continue

                if not name_value:
                    continue

                key = normalize_name(name_value)
                if key not in students:
                    continue

                lesson_dt = parse_datetime_value(str(values.get("meeting_date") or values.get("date") or "")) or current_lesson_dt
                lesson_name = str(values.get("meeting_name") or current_lesson_name or worksheet.title)
                status = status_to_ru(str(values.get("status", "")).strip())
                duration = str(values.get("duration") or "")

                students[key].attendance_lessons.append(
                    {
                        "date": lesson_dt.strftime("%d.%m.%Y %H:%M") if lesson_dt else "—",
                        "lesson": lesson_name,
                        "status": status,
                        "duration": duration,
                        "source": spreadsheet.title,
                        "month_key": month_key_from_datetime(lesson_dt),
                    }
                )


def finalize_students(students: dict[str, StudentRecord]) -> list[StudentRecord]:
    generated_at = format_datetime(datetime.now(APP_TIMEZONE))
    items = []

    for student in students.values():
        student.courses.sort(key=lambda item: (item["kind"], item["course"], item["sheet"]))
        student.attendance_lessons.sort(
            key=lambda item: (item.get("month_key", ""), item.get("date", "")),
            reverse=True,
        )

        total_lessons = len(student.attendance_lessons)
        present = sum(1 for item in student.attendance_lessons if item["status"] == "Посетил")
        late = sum(1 for item in student.attendance_lessons if item["status"] == "Опоздал")
        partial = sum(1 for item in student.attendance_lessons if item["status"] == "Частично")
        absent = sum(1 for item in student.attendance_lessons if item["status"] == "Отсутствовал")
        participated = present + late + partial
        attendance_rate = round((participated / total_lessons) * 100) if total_lessons else None

        homework_courses = [course for course in student.courses if course["kind"] == "homework" and course.get("average") is not None]
        homework_averages = [course["average"] for course in homework_courses if course.get("average") is not None]
        overall_average = round(sum(homework_averages) / len(homework_averages), 1) if homework_averages else None
        topic_count = sum(course.get("homework_count", 0) for course in homework_courses)
        mock_count = sum(course.get("mock_count", 0) for course in student.courses if course["kind"] in {"ielts_mock", "nuet_mock"})

        monthly_summary: dict[str, list[dict[str, Any]]] = {}
        for lesson in student.attendance_lessons:
            monthly_summary.setdefault(lesson["month_key"], []).append(lesson)

        student.link = f"{BASE_URL}/{student.hash}/" if BASE_URL else student.link
        student.updated_at = generated_at
        student.subject = student.subject or student.product
        student.status = student.status or "Активный"
        student.attendance_summary = {
            "total_lessons": total_lessons,
            "present": present,
            "late": late,
            "partial": partial,
            "absent": absent,
            "attendance_rate": attendance_rate,
            "lessons": student.attendance_lessons[:20],
            "monthly": monthly_summary,
            "summary": {
                "overall_average": overall_average,
                "topic_count": topic_count,
                "mock_count": mock_count,
                "course_count": len(student.courses),
                "attendance_rate": attendance_rate,
                "lessons_tracked": total_lessons,
            },
        }
        items.append(student)

    return sorted(items, key=lambda item: item.name.lower())


def render_homework_course(course: dict[str, Any]) -> str:
    topic_rows = "".join(
        f'<div class="topic-row"><span>{escape_html(topic["topic"])}</span><strong>{format_number(topic["score"])}</strong></div>'
        for topic in course["topics"]
    )
    return f"""
    <section class="course-block">
      <div class="course-meta">
        <div>
          <h3>{escape_html(course['course'])}</h3>
          <p>{escape_html(course['sheet'])}</p>
        </div>
        <div class="score-chip">{format_number(course['average'])}/100</div>
      </div>
      <div class="topic-grid">{topic_rows}</div>
    </section>
    """


def render_ielts_mock_course(course: dict[str, Any]) -> str:
    mock_rows = []
    for mock in course["mocks"]:
        for part_key, label in (
            ("reading", "Reading"),
            ("listening", "Listening"),
            ("writing", "Writing"),
            ("speaking", "Speaking"),
        ):
            value = mock.get(part_key)
            mock_rows.append(
                f'<div class="mock-part"><span>Mock {mock["number"]} • {label}</span><strong>{format_number(value) if value is not None else "Не сдавал"}</strong></div>'
            )
        mock_rows.append(
            f'<div class="mock-part total"><span>Mock {mock["number"]} • Итоговый band</span><strong>{format_number(mock.get("overall")) if mock.get("overall") is not None else "Не сдавал"}</strong></div>'
        )

    return f"""
    <section class="course-block mock-block">
      <div class="course-meta">
        <div>
          <h3>{escape_html(course['course'])}</h3>
          <p>{escape_html(course['sheet'])}</p>
        </div>
        <div class="score-chip">{format_number(course.get('average_band')) if course.get('average_band') is not None else '—'} band</div>
      </div>
      <div class="mock-grid">{''.join(mock_rows)}</div>
    </section>
    """


def render_nuet_mock_course(course: dict[str, Any]) -> str:
    mock_rows = []
    for mock in course["mocks"]:
        mock_rows.append(
            f'<div class="mock-part"><span>Mock {mock["number"]} • Critic</span><strong>{format_number(mock.get("critic")) if mock.get("critic") is not None else "Не сдавал"}</strong></div>'
        )
        mock_rows.append(
            f'<div class="mock-part"><span>Mock {mock["number"]} • Math</span><strong>{format_number(mock.get("math")) if mock.get("math") is not None else "Не сдавал"}</strong></div>'
        )

    return f"""
    <section class="course-block mock-block">
      <div class="course-meta">
        <div>
          <h3>{escape_html(course['course'])}</h3>
          <p>{escape_html(course['sheet'])}</p>
        </div>
        <div class="score-chip">{course.get('mock_count', 0)} mock</div>
      </div>
      <div class="mock-grid">{''.join(mock_rows)}</div>
    </section>
    """


def render_student_html(student: StudentRecord) -> str:
    summary = student.attendance_summary.get("summary", {})

    homework_html = "".join(render_homework_course(course) for course in student.courses if course["kind"] == "homework")
    homework_html = homework_html or '<section class="course-block empty-block">По этому ученику пока не найдены домашние задания.</section>'

    mock_html = "".join(
        render_ielts_mock_course(course) if course["kind"] == "ielts_mock" else render_nuet_mock_course(course)
        for course in student.courses
        if course["kind"] in {"ielts_mock", "nuet_mock"}
    )
    mock_html = mock_html or '<section class="course-block empty-block">По этому ученику пока не найдены mock tests.</section>'

    lesson_rows = "".join(
        f"""
        <tr>
          <td>{escape_html(lesson['date'])}</td>
          <td>{escape_html(lesson['lesson'])}</td>
          <td>{escape_html(lesson['status'])}</td>
          <td>{escape_html(lesson['duration']) or '—'}</td>
        </tr>
        """
        for lesson in student.attendance_summary.get("lessons", [])
    ) or '<tr><td colspan="4">Посещаемость по ученику пока не найдена.</td></tr>'

    month_cards = []
    for month_key in sorted(student.attendance_summary.get("monthly", {}).keys(), reverse=True):
        lessons = student.attendance_summary["monthly"][month_key]
        month_cards.append(
            f"""
            <div class="month-card">
              <div class="month-head">
                <strong>{escape_html(month_key_to_label(month_key))}</strong>
                <span>{len(lessons)} уроков</span>
              </div>
              <div class="month-list">
                {''.join(f'<div class="month-row"><span>{escape_html(item["date"])}</span><small>{escape_html(item["lesson"])}</small></div>' for item in lessons[:6])}
              </div>
            </div>
            """
        )

    months_html = "".join(month_cards) or '<div class="month-card empty-block">Пока нет данных по месяцам.</div>'
    hero_subject = student.subject or student.product or "Отчёт по ученику"
    hero_product = student.product or student.subject or "Академический прогресс"

    translations = {
        "ru": {
            "eyebrow": "Индивидуальный отчёт по ученику",
            "copy": f"{hero_subject}. Аккуратная сводка по успеваемости, выполненным заданиям и посещаемости.",
            "open_homework": "Открыть задания",
            "open_mock": "Открыть mock tests",
            "open_attendance": "Открыть посещаемость",
            "name": "ФИО",
            "group": "Группа",
            "product": "Продукт",
            "status": "Статус",
            "avg": "Средний grade",
            "topics": "Темы",
            "mock_count": "Mock tests",
            "attendance": "Посещаемость",
            "lessons": "Уроки",
            "homework_title": "Домашние задания",
            "homework_copy": "Результаты по homework и темам, без влияния mock tests на средний grade.",
            "mock_title": "Mock tests",
            "mock_copy": "Результаты пробных тестов по IELTS и NUET с отдельной логикой отображения.",
            "attendance_title": "Посещаемость",
            "attendance_copy": "Сводка по посещаемости, последние уроки и разбивка по месяцам.",
            "months_title": "По месяцам",
            "present": "Посещено",
            "late": "Опоздал",
            "partial": "Частично",
            "absent": "Отсутствовал",
            "date": "Дата",
            "lesson": "Урок",
            "duration": "Длительность",
            "updated": "Обновлено",
            "footer": "© 2026 WeGlobal • Индивидуальный отчёт ученика",
        },
        "en": {
            "eyebrow": "Individual student report",
            "copy": f"{hero_subject}. A clear summary of homework, mock tests and attendance.",
            "open_homework": "Open homework",
            "open_mock": "Open mock tests",
            "open_attendance": "Open attendance",
            "name": "Name",
            "group": "Group",
            "product": "Product",
            "status": "Status",
            "avg": "Average grade",
            "topics": "Topics",
            "mock_count": "Mock tests",
            "attendance": "Attendance",
            "lessons": "Lessons",
            "homework_title": "Homework",
            "homework_copy": "Results for homework and topics, excluding mock tests from the average grade.",
            "mock_title": "Mock tests",
            "mock_copy": "IELTS and NUET mock results with separate display logic.",
            "attendance_title": "Attendance",
            "attendance_copy": "Attendance summary, recent lessons and monthly breakdown.",
            "months_title": "By month",
            "present": "Present",
            "late": "Late",
            "partial": "Partial",
            "absent": "Absent",
            "date": "Date",
            "lesson": "Lesson",
            "duration": "Duration",
            "updated": "Updated",
            "footer": "© 2026 WeGlobal • Individual student report",
        },
        "kz": {
            "eyebrow": "Оқушының жеке есебі",
            "copy": f"{hero_subject}. Үй тапсырмасы, mock tests және қатысу бойынша жинақы есеп.",
            "open_homework": "Тапсырмаларды ашу",
            "open_mock": "Mock tests ашу",
            "open_attendance": "Қатысуды ашу",
            "name": "Аты-жөні",
            "group": "Топ",
            "product": "Өнім",
            "status": "Мәртебе",
            "avg": "Орташа grade",
            "topics": "Тақырыптар",
            "mock_count": "Mock tests",
            "attendance": "Қатысу",
            "lessons": "Сабақтар",
            "homework_title": "Үй тапсырмасы",
            "homework_copy": "Homework және тақырыптар нәтижесі, mock tests орташа бағалауға әсер етпейді.",
            "mock_title": "Mock tests",
            "mock_copy": "IELTS және NUET mock нәтижелері бөлек көрсетіледі.",
            "attendance_title": "Қатысу",
            "attendance_copy": "Қатысу жиынтығы, соңғы сабақтар және ай бойынша бөліну.",
            "months_title": "Айлар бойынша",
            "present": "Қатысты",
            "late": "Кешікті",
            "partial": "Ішінара",
            "absent": "Қатыспады",
            "date": "Күні",
            "lesson": "Сабақ",
            "duration": "Ұзақтығы",
            "updated": "Жаңартылды",
            "footer": "© 2026 WeGlobal • Оқушының жеке есебі",
        },
    }

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
      --panel: rgba(255, 255, 255, 0.86);
      --panel-strong: #ffffff;
      --text: #1d1d1f;
      --muted: #6e6e73;
      --line: rgba(0, 0, 0, 0.08);
      --accent: #0071e3;
      --accent-soft: rgba(0, 113, 227, 0.10);
      --shadow: 0 16px 40px rgba(0, 0, 0, 0.07);
      --radius: 28px;
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      font-family: 'Inter', sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(0, 113, 227, 0.10), transparent 22%),
        linear-gradient(180deg, #fbfbfd 0%, var(--bg) 100%);
    }}
    .topbar {{
      position: sticky;
      top: 0;
      z-index: 10;
      backdrop-filter: blur(20px);
      background: rgba(251, 251, 253, 0.78);
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
      gap: 16px;
      padding: 14px 0;
    }}
    .brand {{
      font-weight: 700;
      font-size: 20px;
      letter-spacing: -0.02em;
    }}
    .brand span {{
      color: var(--muted);
      font-weight: 500;
    }}
    .top-meta {{
      display: flex;
      align-items: center;
      gap: 16px;
      color: var(--muted);
      font-size: 13px;
    }}
    .lang-switch {{
      display: flex;
      gap: 6px;
    }}
    .lang-btn {{
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.7);
      color: var(--text);
      padding: 6px 10px;
      border-radius: 999px;
      cursor: pointer;
      font-weight: 600;
      font-size: 12px;
    }}
    .lang-btn.active {{
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
    }}
    .hero {{
      padding: 56px 0 26px;
      text-align: center;
    }}
    .eyebrow {{
      color: var(--accent);
      font-size: 14px;
      font-weight: 600;
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
      min-width: 164px;
      padding: 12px 20px;
      border-radius: 999px;
      text-decoration: none;
      font-weight: 600;
      transition: transform .18s ease, box-shadow .18s ease;
    }}
    .btn.primary {{
      background: var(--accent);
      color: #fff;
      box-shadow: 0 10px 24px rgba(0, 113, 227, 0.24);
    }}
    .btn.secondary {{
      color: var(--accent);
      border: 1px solid rgba(0, 113, 227, 0.30);
      background: rgba(255,255,255,0.8);
    }}
    .btn:hover {{
      transform: translateY(-1px);
    }}
    .hero-panel {{
      margin-top: 34px;
      background: linear-gradient(180deg, rgba(255,255,255,0.88) 0%, rgba(255,255,255,0.78) 100%);
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
      grid-template-columns: repeat(5, minmax(0, 1fr));
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
    .stack {{
      display: grid;
      gap: 18px;
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
    .topic-grid, .mock-grid {{
      display: grid;
      gap: 10px;
    }}
    .topic-row, .mock-part {{
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
    .mock-part.total {{
      background: #f0f6ff;
    }}
    .metrics-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .metric-box, .month-card {{
      padding: 18px;
      border-radius: 22px;
      background: var(--panel-strong);
      border: 1px solid var(--line);
    }}
    .metric-box .value {{
      font-size: 36px;
      margin-top: 6px;
    }}
    .months-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .month-head {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: center;
      margin-bottom: 10px;
    }}
    .month-list {{
      display: grid;
      gap: 8px;
    }}
    .month-row {{
      display: grid;
      gap: 2px;
      padding: 10px 12px;
      border-radius: 14px;
      background: #fafafc;
      border: 1px solid var(--line);
    }}
    .month-row span {{
      font-weight: 600;
      font-size: 13px;
    }}
    .month-row small {{
      color: var(--muted);
      font-size: 12px;
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
      .identity-grid, .stat-strip, .metrics-grid {{
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
        <span><span data-i18n="updated">{escape_html(translations["ru"]["updated"])}</span>: {escape_html(student.updated_at)}</span>
        <div class="lang-switch">
          <button class="lang-btn active" data-lang="ru">RU</button>
          <button class="lang-btn" data-lang="kz">KZ</button>
          <button class="lang-btn" data-lang="en">EN</button>
        </div>
      </div>
    </div>
  </div>

  <section class="hero">
    <div class="eyebrow" data-i18n="eyebrow">{escape_html(translations["ru"]["eyebrow"])}</div>
    <h1>{escape_html(student.name)}</h1>
    <div class="hero-copy" data-i18n="copy">{escape_html(translations["ru"]["copy"])}</div>
    <div class="hero-actions">
      <a class="btn primary" href="#homework" data-i18n="open_homework">{escape_html(translations["ru"]["open_homework"])}</a>
      <a class="btn secondary" href="#mock-tests" data-i18n="open_mock">{escape_html(translations["ru"]["open_mock"])}</a>
      <a class="btn secondary" href="#attendance" data-i18n="open_attendance">{escape_html(translations["ru"]["open_attendance"])}</a>
    </div>
    <div class="hero-panel">
      <div class="identity-grid">
        <div class="identity-card"><span class="label" data-i18n="name">{escape_html(translations["ru"]["name"])}</span><div class="value">{escape_html(student.name)}</div></div>
        <div class="identity-card"><span class="label" data-i18n="group">{escape_html(translations["ru"]["group"])}</span><div class="value">{escape_html(student.group or '—')}</div></div>
        <div class="identity-card"><span class="label" data-i18n="product">{escape_html(translations["ru"]["product"])}</span><div class="value">{escape_html(student.product or '—')}</div></div>
        <div class="identity-card"><span class="label" data-i18n="status">{escape_html(translations["ru"]["status"])}</span><div class="value">{escape_html(student.status or '—')}</div></div>
      </div>
    </div>
  </section>

  <section class="section">
    <div class="stat-strip">
      <div class="stat-card"><span class="label" data-i18n="avg">{escape_html(translations["ru"]["avg"])}</span><div class="value">{format_number(summary.get('overall_average'))}</div></div>
      <div class="stat-card"><span class="label" data-i18n="topics">{escape_html(translations["ru"]["topics"])}</span><div class="value">{summary.get('topic_count', 0)}</div></div>
      <div class="stat-card"><span class="label" data-i18n="mock_count">{escape_html(translations["ru"]["mock_count"])}</span><div class="value">{summary.get('mock_count', 0)}</div></div>
      <div class="stat-card"><span class="label" data-i18n="attendance">{escape_html(translations["ru"]["attendance"])}</span><div class="value">{str(summary.get('attendance_rate')) + '%' if summary.get('attendance_rate') is not None else '—'}</div></div>
      <div class="stat-card"><span class="label" data-i18n="lessons">{escape_html(translations["ru"]["lessons"])}</span><div class="value">{summary.get('lessons_tracked', 0)}</div></div>
    </div>

    <div class="stack">
      <div class="panel" id="homework">
        <div class="panel-head">
          <div>
            <h2 data-i18n="homework_title">{escape_html(translations["ru"]["homework_title"])}</h2>
            <div class="panel-subtle" data-i18n="homework_copy">{escape_html(translations["ru"]["homework_copy"])}</div>
          </div>
          <div class="pill">{summary.get('topic_count', 0)} HW</div>
        </div>
        {homework_html}
      </div>

      <div class="panel" id="mock-tests">
        <div class="panel-head">
          <div>
            <h2 data-i18n="mock_title">{escape_html(translations["ru"]["mock_title"])}</h2>
            <div class="panel-subtle" data-i18n="mock_copy">{escape_html(translations["ru"]["mock_copy"])}</div>
          </div>
          <div class="pill">{summary.get('mock_count', 0)} mock</div>
        </div>
        {mock_html}
      </div>

      <div class="panel" id="attendance">
        <div class="panel-head">
          <div>
            <h2 data-i18n="attendance_title">{escape_html(translations["ru"]["attendance_title"])}</h2>
            <div class="panel-subtle" data-i18n="attendance_copy">{escape_html(translations["ru"]["attendance_copy"])}</div>
          </div>
          <div class="pill">{student.attendance_summary.get('total_lessons', 0)} уроков</div>
        </div>

        <div class="metrics-grid">
          <div class="metric-box"><span class="label" data-i18n="present">{escape_html(translations["ru"]["present"])}</span><div class="value">{student.attendance_summary.get('present', 0)}</div></div>
          <div class="metric-box"><span class="label" data-i18n="late">{escape_html(translations["ru"]["late"])}</span><div class="value">{student.attendance_summary.get('late', 0)}</div></div>
          <div class="metric-box"><span class="label" data-i18n="partial">{escape_html(translations["ru"]["partial"])}</span><div class="value">{student.attendance_summary.get('partial', 0)}</div></div>
          <div class="metric-box"><span class="label" data-i18n="absent">{escape_html(translations["ru"]["absent"])}</span><div class="value">{student.attendance_summary.get('absent', 0)}</div></div>
        </div>

        <div class="panel-head">
          <div>
            <h2 data-i18n="months_title">{escape_html(translations["ru"]["months_title"])}</h2>
          </div>
        </div>
        <div class="months-grid">{months_html}</div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th data-i18n="date">{escape_html(translations["ru"]["date"])}</th>
                <th data-i18n="lesson">{escape_html(translations["ru"]["lesson"])}</th>
                <th data-i18n="status">{escape_html(translations["ru"]["status"])}</th>
                <th data-i18n="duration">{escape_html(translations["ru"]["duration"])}</th>
              </tr>
            </thead>
            <tbody>{lesson_rows}</tbody>
          </table>
        </div>
      </div>
    </div>
  </section>

  <div class="footer" data-i18n="footer">{escape_html(translations["ru"]["footer"])}</div>

  <script>
    const translations = {json.dumps(translations, ensure_ascii=False)};
    const buttons = document.querySelectorAll('.lang-btn');
    function applyLanguage(lang) {{
      document.documentElement.lang = lang;
      document.querySelectorAll('[data-i18n]').forEach(node => {{
        const key = node.getAttribute('data-i18n');
        if (translations[lang] && translations[lang][key]) {{
          node.textContent = translations[lang][key];
        }}
      }});
      buttons.forEach(btn => btn.classList.toggle('active', btn.dataset.lang === lang));
    }}
    buttons.forEach(btn => btn.addEventListener('click', () => applyLanguage(btn.dataset.lang)));
  </script>
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
            <b>{format_number(student.attendance_summary.get('summary', {}).get('overall_average'))}</b>
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
      <div>Обновлено: {escape_html(format_datetime(datetime.now(APP_TIMEZONE)))}</div>
    </div>
  </div>
  <section class="hero">
    <div class="eyebrow">Индивидуальные отчёты учеников</div>
    <h1>WeGlobal Reports</h1>
    <p>Аккуратные персональные страницы по каждому ученику с отдельной ссылкой, homework, mock tests и посещаемостью.</p>
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
