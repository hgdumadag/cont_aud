from django.conf import settings
from django.utils import timezone

from auditpilot.models import AuditRun, DQFinding, RunStatusChoices, SheetRun
from auditpilot.services.constants import REQUIRED_SHEETS
from auditpilot.services.background import submit_audit_run_processing
from auditpilot.services.dq import evaluate_sheet
from auditpilot.services.ingest import load_workbook_payload, store_uploaded_workbook
from auditpilot.services.normalize import normalize_sheet
from auditpilot.services.rules import ensure_starter_controls, run_controls_for_run


def _update_run(run, *, stage=None, message=None, status=None, total_rows=None, total_warnings=None, total_errors=None, total_exceptions=None):
    fields = []
    if stage is not None:
        run.processing_stage = stage
        fields.append('processing_stage')
    if message is not None:
        run.overall_message = message
        fields.append('overall_message')
    if status is not None:
        run.status = status
        fields.append('status')
    if total_rows is not None:
        run.total_rows = total_rows
        fields.append('total_rows')
    if total_warnings is not None:
        run.total_warnings = total_warnings
        fields.append('total_warnings')
    if total_errors is not None:
        run.total_errors = total_errors
        fields.append('total_errors')
    if total_exceptions is not None:
        run.total_exceptions = total_exceptions
        fields.append('total_exceptions')
    if fields:
        run.save(update_fields=fields)


def create_run_submission(uploaded_file, as_of_label='', uploaded_by=''):
    ensure_starter_controls()
    archive = store_uploaded_workbook(uploaded_file, as_of_label=as_of_label, uploaded_by=uploaded_by)
    return AuditRun.objects.create(
        source_file=archive,
        as_of_label=as_of_label,
        uploaded_by=uploaded_by,
        status=RunStatusChoices.QUEUED,
        processing_stage='Queued',
        overall_message='Run submitted and waiting to start.',
    )


def _process_existing_run(run):
    try:
        _update_run(run, stage='Workbook archived', message='Workbook archived. Beginning validation.', status=RunStatusChoices.RUNNING)
        workbook = load_workbook_payload(
            run.source_file.stored_file.path,
            progress_callback=lambda sheet_name: _update_run(
                run,
                stage=f'Loading {sheet_name}',
                message=f'Loading {sheet_name} worksheet from the workbook.',
                status=RunStatusChoices.RUNNING,
            ),
        )
        _update_run(run, stage='Workbook loaded', message='Workbook loaded. Running data quality checks.', status=RunStatusChoices.RUNNING)
        evaluations = {}
        total_rows = 0
        total_warnings = 0
        total_errors = 0
        for sheet_name in REQUIRED_SHEETS:
            _update_run(run, stage=f'Validating {sheet_name}', message=f'Validating {sheet_name} sheet.', status=RunStatusChoices.RUNNING)
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
            _update_run(run, total_rows=total_rows, total_warnings=total_warnings, total_errors=total_errors, status=RunStatusChoices.RUNNING)
        if any(evaluation.hard_fail for evaluation in evaluations.values()):
            _update_run(run, stage='Data quality gate failed', message='Run stopped at the data quality gate.', status=RunStatusChoices.FAILED_DQ)
            run.completed_at = timezone.now()
            run.save(update_fields=['completed_at'])
            return run
        _update_run(run, stage='Normalizing sheets', message='Data quality checks passed. Normalizing sheets.', status=RunStatusChoices.RUNNING)
        for sheet_name, evaluation in evaluations.items():
            _update_run(run, stage=f'Normalizing {sheet_name}', message=f'Normalizing {sheet_name} sheet.', status=RunStatusChoices.RUNNING)
            records, metrics = normalize_sheet(run, sheet_name, evaluation.dataframe)
            created = run.normalized_records.bulk_create(records, batch_size=1000) if records else []
            sheet_run = run.sheet_runs.get(sheet_name=sheet_name)
            sheet_run.normalized_row_count = len(created)
            sheet_run.base_transaction_count = metrics['base_transaction_count']
            sheet_run.derived_row_count = metrics['derived_row_count']
            sheet_run.save(update_fields=['normalized_row_count', 'base_transaction_count', 'derived_row_count'])
        _update_run(run, stage='Running controls', message='Normalization complete. Running control library.', status=RunStatusChoices.RUNNING)
        total_exceptions = run_controls_for_run(run)
        _update_run(run, total_exceptions=total_exceptions, stage='Completed', message='Run completed successfully.', status=RunStatusChoices.COMPLETED)
        run.completed_at = timezone.now()
        run.save(update_fields=['completed_at'])
    except Exception as exc:
        _update_run(run, stage='Processing failed', message=str(exc), status=RunStatusChoices.FAILED_PROCESSING)
        run.completed_at = timezone.now()
        run.save(update_fields=['completed_at'])
    return run


def process_audit_run(run_id):
    run = AuditRun.objects.select_related('source_file').get(pk=run_id)
    ensure_starter_controls()
    return _process_existing_run(run)


def process_uploaded_workbook(uploaded_file, as_of_label='', uploaded_by=''):
    run = create_run_submission(uploaded_file, as_of_label=as_of_label, uploaded_by=uploaded_by)
    if getattr(settings, 'AUDITPILOT_BACKGROUND_PROCESSING', True):
        submit_audit_run_processing(run.id)
        return run
    return process_audit_run(run.id)
