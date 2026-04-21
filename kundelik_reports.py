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
