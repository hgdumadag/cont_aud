import hashlib
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from django.utils import timezone
from openpyxl import load_workbook

from auditpilot.models import SourceFileArchive
from auditpilot.services.utils import normalize_text


@dataclass
class SheetPayload:
    sheet_name: str
    original_headers: list
    internal_headers: list
    dataframe: pd.DataFrame


@dataclass
class WorkbookPayload:
    sheets: dict


def uniquify_headers(headers):
    counts = Counter()
    internal = []
    for index, header in enumerate(headers, start=1):
        text = normalize_text(header) or f'Unnamed Column {index}'
        counts[text] += 1
        internal.append(text if counts[text] == 1 else f'{text}__{counts[text]}')
    return internal


def store_uploaded_workbook(uploaded_file, as_of_label='', uploaded_by=''):
    digest = hashlib.sha256()
    for chunk in uploaded_file.chunks():
        digest.update(chunk)
    uploaded_file.seek(0)
    archive = SourceFileArchive.objects.create(
        original_name=Path(uploaded_file.name).name,
        sha256=digest.hexdigest(),
        size_bytes=getattr(uploaded_file, 'size', 0),
        uploaded_by=uploaded_by,
        as_of_label=as_of_label,
    )
    timestamp = timezone.now().strftime('%Y/%m/%d')
    stored_name = f"raw_snapshots/{timestamp}/{archive.sha256}_{Path(uploaded_file.name).name}"
    archive.stored_file.save(stored_name, uploaded_file, save=True)
    return archive


def load_workbook_payload(path):
    workbook = load_workbook(path, read_only=True, data_only=True)
    payload = {}
    for worksheet in workbook.worksheets:
        rows = worksheet.iter_rows(values_only=True)
        try:
            raw_headers = next(rows)
        except StopIteration:
            continue
        original_headers = [normalize_text(header) or f'Unnamed Column {index}' for index, header in enumerate(raw_headers, start=1)]
        internal_headers = uniquify_headers(original_headers)
        records = []
        for row_number, row in enumerate(rows, start=2):
            values = list(row)
            if len(values) < len(internal_headers):
                values.extend([None] * (len(internal_headers) - len(values)))
            record = {internal_headers[index]: values[index] for index in range(len(internal_headers))}
            record['__source_row_number'] = row_number
            records.append(record)
        dataframe = pd.DataFrame(records)
        payload[worksheet.title] = SheetPayload(
            sheet_name=worksheet.title,
            original_headers=original_headers,
            internal_headers=internal_headers,
            dataframe=dataframe,
        )
    return WorkbookPayload(sheets=payload)
