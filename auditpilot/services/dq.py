from collections import Counter
from dataclasses import dataclass, field

import pandas as pd
from django.db.models import Max

from auditpilot.models import FindingSeverity, RunStatusChoices, SchemaSnapshot, SheetRun, SheetStatusChoices
from auditpilot.services.constants import SOURCE_SPECS
from auditpilot.services.utils import is_placeholder, parse_date_value, stable_hash


@dataclass
class SheetEvaluation:
    sheet_name: str
    dataframe: pd.DataFrame
    schema_version: str = ''
    status: str = SheetStatusChoices.PASSED
    hard_fail: bool = False
    row_count: int = 0
    metrics: dict = field(default_factory=dict)
    missing_required: list = field(default_factory=list)
    new_columns: list = field(default_factory=list)
    findings: list = field(default_factory=list)


def _count_difference(expected, actual):
    missing = []
    for key, count in expected.items():
        diff = count - actual.get(key, 0)
        if diff > 0:
            missing.extend([key] * diff)
    return missing


def _placeholder_ratios(dataframe):
    ratios = {}
    if dataframe.empty:
        return ratios
    for column in dataframe.columns:
        if column == '__source_row_number':
            continue
        populated = [value for value in dataframe[column].tolist() if value not in (None, '')]
        if not populated:
            continue
        placeholders = sum(1 for value in populated if is_placeholder(value))
        ratios[column] = round(placeholders / len(populated), 4)
    return ratios


def _date_parse_failures(dataframe, fields):
    failures = {}
    for field in fields:
        if field not in dataframe.columns:
            continue
        bad_rows = []
        for _, row in dataframe[['__source_row_number', field]].iterrows():
            value = row[field]
            if value in (None, ''):
                continue
            if parse_date_value(value) is None:
                bad_rows.append(int(row['__source_row_number']))
        if bad_rows:
            failures[field] = bad_rows
    return failures


def _count_duplicate_candidates(dataframe, fields):
    if dataframe.empty:
        return 0
    subset = [field for field in fields if field in dataframe.columns]
    if not subset:
        return 0
    working = dataframe[subset].copy()
    for field in subset:
        if 'Date' in field or 'date' in field:
            working[field] = working[field].map(lambda value: parse_date_value(value).isoformat() if parse_date_value(value) else '')
        else:
            working[field] = working[field].fillna('').astype(str).str.strip()
    return int(working.duplicated(subset=subset, keep=False).sum())


def _count_derived_candidates(dataframe, fields):
    if dataframe.empty:
        return 0
    keywords = ('result', 'summary', 'subtotal', 'total')
    count = 0
    for _, row in dataframe.iterrows():
        texts = []
        for field in fields:
            value = row.get(field)
            if value is not None:
                texts.append(str(value).lower())
        if any(any(keyword in text for keyword in keywords) for text in texts):
            count += 1
    return count


def evaluate_sheet(run, sheet_name, payload):
    spec = SOURCE_SPECS[sheet_name]
    if payload is None:
        return SheetEvaluation(
            sheet_name=sheet_name,
            dataframe=pd.DataFrame(),
            status=SheetStatusChoices.FAILED,
            hard_fail=True,
            findings=[
                {
                    'severity': FindingSeverity.ERROR,
                    'code': 'missing_required_sheet',
                    'message': f'Required sheet {sheet_name} is missing from the workbook.',
                    'details_json': {},
                }
            ],
        )

    actual_headers = payload.original_headers
    actual_counter = Counter(actual_headers)
    expected_counter = Counter(spec['header_sequence'])
    required_counter = Counter(spec['required_headers'])
    missing_required = _count_difference(required_counter, actual_counter)
    new_columns = _count_difference(actual_counter, expected_counter)
    row_count = len(payload.dataframe.index)

    signature = stable_hash(actual_headers)
    snapshot = SchemaSnapshot.objects.filter(header_signature=signature).first()
    if snapshot is None:
        max_version = SchemaSnapshot.objects.filter(sheet_name=sheet_name).aggregate(value=Max('version'))['value'] or 0
        snapshot = SchemaSnapshot.objects.create(
            sheet_name=sheet_name,
            version=max_version + 1,
            header_signature=signature,
            headers_json=actual_headers,
            required_headers_json=spec['required_headers'],
        )
    schema_version = f"{sheet_name}-v{snapshot.version}"

    placeholder_ratios = _placeholder_ratios(payload.dataframe)
    date_failures = _date_parse_failures(payload.dataframe, spec['date_fields'])
    duplicate_candidates = _count_duplicate_candidates(payload.dataframe, spec['duplicate_key_fields'])
    derived_candidates = _count_derived_candidates(payload.dataframe, spec['record_class_fields'])

    previous_sheet = SheetRun.objects.filter(sheet_name=sheet_name, run__status=RunStatusChoices.COMPLETED).order_by('-run__started_at').first()
    previous_ratios = previous_sheet.metrics_json.get('placeholder_ratios', {}) if previous_sheet else {}
    placeholder_flags = []
    for column, ratio in placeholder_ratios.items():
        previous_ratio = previous_ratios.get(column) if previous_sheet else None
        threshold_breach = ratio >= 0.20
        delta_breach = previous_ratio is not None and (ratio - previous_ratio) >= 0.10
        if threshold_breach or delta_breach:
            placeholder_flags.append(
                {
                    'column': column,
                    'ratio': ratio,
                    'previous_ratio': previous_ratio,
                    'baseline_available': previous_ratio is not None,
                    'triggered_by': 'threshold_and_delta' if threshold_breach and delta_breach else 'threshold' if threshold_breach else 'delta',
                }
            )

    findings = []
    status = SheetStatusChoices.PASSED
    if missing_required:
        status = SheetStatusChoices.FAILED
        findings.append({'severity': FindingSeverity.ERROR, 'code': 'missing_required_headers', 'message': 'Critical columns are missing from the worksheet.', 'details_json': {'headers': missing_required}})
    if new_columns:
        if status != SheetStatusChoices.FAILED:
            status = SheetStatusChoices.WARNING
        findings.append({'severity': FindingSeverity.WARNING, 'code': 'unexpected_columns', 'message': 'The worksheet contains columns not seen in the expected pilot schema.', 'details_json': {'headers': new_columns}})
    if date_failures:
        if status != SheetStatusChoices.FAILED:
            status = SheetStatusChoices.WARNING
        findings.append({'severity': FindingSeverity.WARNING, 'code': 'date_parse_failures', 'message': 'Some date values could not be parsed cleanly.', 'details_json': date_failures})
    if placeholder_flags:
        if status != SheetStatusChoices.FAILED:
            status = SheetStatusChoices.WARNING
        findings.append({'severity': FindingSeverity.WARNING, 'code': 'placeholder_spike', 'message': 'Placeholder density is high or increasing in one or more columns.', 'details_json': {'columns': placeholder_flags}})
    if duplicate_candidates:
        if status != SheetStatusChoices.FAILED:
            status = SheetStatusChoices.WARNING
        findings.append({'severity': FindingSeverity.WARNING, 'code': 'duplicate_candidates', 'message': 'Potential duplicate transaction candidates were detected before normalization.', 'details_json': {'count': duplicate_candidates}})
    if derived_candidates:
        findings.append({'severity': FindingSeverity.INFO, 'code': 'derived_rows_detected', 'message': 'Rows that look like derived or summary output were detected.', 'details_json': {'count': derived_candidates}})

    return SheetEvaluation(
        sheet_name=sheet_name,
        dataframe=payload.dataframe,
        schema_version=schema_version,
        status=status,
        hard_fail=bool(missing_required),
        row_count=row_count,
        metrics={
            'placeholder_ratios': placeholder_ratios,
            'placeholder_flags': placeholder_flags,
            'date_parse_failures': date_failures,
            'duplicate_candidate_count': duplicate_candidates,
            'derived_candidate_count': derived_candidates,
        },
        missing_required=missing_required,
        new_columns=new_columns,
        findings=findings,
    )
