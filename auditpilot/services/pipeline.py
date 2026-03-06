from django.utils import timezone

from auditpilot.models import AuditRun, DQFinding, RunStatusChoices, SheetRun
from auditpilot.services.constants import REQUIRED_SHEETS
from auditpilot.services.dq import evaluate_sheet
from auditpilot.services.ingest import load_workbook_payload, store_uploaded_workbook
from auditpilot.services.normalize import normalize_sheet
from auditpilot.services.rules import ensure_starter_controls, run_controls_for_run


def process_uploaded_workbook(uploaded_file, as_of_label='', uploaded_by=''):
    ensure_starter_controls()
    archive = store_uploaded_workbook(uploaded_file, as_of_label=as_of_label, uploaded_by=uploaded_by)
    run = AuditRun.objects.create(source_file=archive, as_of_label=as_of_label, uploaded_by=uploaded_by)
    try:
        workbook = load_workbook_payload(archive.stored_file.path)
        evaluations = {}
        total_rows = 0
        total_warnings = 0
        total_errors = 0
        for sheet_name in REQUIRED_SHEETS:
            evaluation = evaluate_sheet(run, sheet_name, workbook.sheets.get(sheet_name))
            evaluations[sheet_name] = evaluation
            total_rows += evaluation.row_count
            sheet_run = SheetRun.objects.create(
                run=run,
                sheet_name=sheet_name,
                status=evaluation.status,
                row_count=evaluation.row_count,
                schema_version=evaluation.schema_version,
                required_missing_json=evaluation.missing_required,
                new_columns_json=evaluation.new_columns,
                metrics_json=evaluation.metrics,
            )
            for finding in evaluation.findings:
                DQFinding.objects.create(run=run, sheet_run=sheet_run, severity=finding['severity'], code=finding['code'], message=finding['message'], details_json=finding.get('details_json', {}))
                if finding['severity'] == 'WARNING':
                    total_warnings += 1
                if finding['severity'] == 'ERROR':
                    total_errors += 1
        run.total_rows = total_rows
        run.total_warnings = total_warnings
        run.total_errors = total_errors
        if any(evaluation.hard_fail for evaluation in evaluations.values()):
            run.status = RunStatusChoices.FAILED_DQ
            run.overall_message = 'Run stopped at the data quality gate.'
            run.completed_at = timezone.now()
            run.save(update_fields=['total_rows', 'total_warnings', 'total_errors', 'status', 'overall_message', 'completed_at'])
            return run
        for sheet_name, evaluation in evaluations.items():
            records, metrics = normalize_sheet(run, sheet_name, evaluation.dataframe)
            created = run.normalized_records.bulk_create(records) if records else []
            sheet_run = run.sheet_runs.get(sheet_name=sheet_name)
            sheet_run.normalized_row_count = len(created)
            sheet_run.base_transaction_count = metrics['base_transaction_count']
            sheet_run.derived_row_count = metrics['derived_row_count']
            sheet_run.save(update_fields=['normalized_row_count', 'base_transaction_count', 'derived_row_count'])
        run.total_exceptions = run_controls_for_run(run)
        run.status = RunStatusChoices.COMPLETED
        run.overall_message = 'Run completed successfully.'
        run.completed_at = timezone.now()
        run.save(update_fields=['total_rows', 'total_warnings', 'total_errors', 'total_exceptions', 'status', 'overall_message', 'completed_at'])
    except Exception as exc:
        run.status = RunStatusChoices.FAILED_PROCESSING
        run.overall_message = str(exc)
        run.completed_at = timezone.now()
        run.save(update_fields=['status', 'overall_message', 'completed_at'])
    return run
