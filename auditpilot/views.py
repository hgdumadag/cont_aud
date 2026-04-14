from django.contrib import messages
from django.db.models import Count
from django.http import Http404
from django.http import HttpResponse
from django.urls import reverse
from django.shortcuts import get_object_or_404, redirect, render

from .forms import ControlCatalogForm, ExceptionUpdateForm, UploadWorkbookForm
from .models import AuditRun, ControlCatalog, DQFinding, ExceptionEvent, ExceptionRecord, ExceptionStatusChoices, SeverityChoices
from .services.constants import SOURCE_SPECS
from .services.exports import build_weekly_pack
from .services.ingest import uniquify_headers
from .services.pipeline import process_uploaded_workbook
from .services.visual_reports import build_weekly_visual_pack_context, get_board_context, render_visual_board_png


OPEN_EXCEPTION_STATUSES = [
    ExceptionStatusChoices.NEW,
    ExceptionStatusChoices.OPEN,
    ExceptionStatusChoices.VALIDATED,
    ExceptionStatusChoices.IN_PROGRESS,
]

CANONICAL_DETAIL_FIELDS = [
    ('source_row_number', 'Source row number'),
    ('record_class', 'Record class'),
    ('company_code', 'Company Code'),
    ('vendor_code', 'Vendor Code'),
    ('vendor_name', 'Vendor Name'),
    ('fiscal_year', 'Fiscal year'),
    ('payment_document', 'Payment Document'),
    ('accounting_document', 'Accounting document'),
    ('payment_method', 'Payment Method'),
    ('document_type', 'Document Type'),
    ('process_group', 'Process Group'),
    ('nature_of_transaction', 'Nature of Transaction'),
    ('status_code', 'Status'),
    ('status_description', 'Status Description'),
    ('document_date', 'Document Date'),
    ('baseline_date', 'Baseline Date'),
    ('posting_date', 'Posting Date'),
    ('payment_entry_date', 'Payment Entry Date'),
    ('payment_release_date', 'Payment Release Date'),
    ('clearing_date', 'Clearing Date'),
    ('net_due_date', 'Net Due Date'),
    ('status_date', 'Status Date'),
    ('amount_local', 'Amount (Local)'),
    ('amount_document', 'Amount (Document)'),
    ('days_of_status', 'Days of Status'),
    ('pot', 'POT'),
    ('payment_cycle_time', 'Payment Cycle Time'),
    ('total_processing_time', 'Total Processing Time'),
    ('transaction_key', 'Transaction Key'),
    ('payment_key', 'Payment Key'),
]


def _display_value(value):
    if value in (None, ''):
        return '-'
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    return str(value)


def _display_header(header_name):
    if '__' in header_name:
        base, occurrence = header_name.rsplit('__', 1)
        if occurrence.isdigit():
            return f'{base} ({occurrence})'
    return header_name


def _humanize_key(key):
    text = key.replace('_', ' ').strip()
    return text[:1].upper() + text[1:] if text else key


def _build_exception_context_rows(exception):
    rows = [('Finding scope', 'Row-level exception' if exception.normalized_record_id else 'Sheet-level / column-level exception')]
    for key, value in (exception.extra_context or {}).items():
        if key == 'previous_ratio' and value is None:
            display = 'No baseline'
        elif isinstance(value, float) and 'ratio' in key:
            display = f'{value:.0%}'
        elif isinstance(value, bool):
            display = 'Yes' if value else 'No'
        else:
            display = _display_value(value)
        rows.append((_humanize_key(key), display))
    return rows


def _build_canonical_detail_rows(record):
    return [(label, _display_value(getattr(record, field_name, None))) for field_name, label in CANONICAL_DETAIL_FIELDS]


def _build_source_row_rows(record):
    spec = SOURCE_SPECS.get(record.entity)
    if not spec:
        return []
    source_to_attr = {source_field: attr_name for attr_name, source_field in spec['canonical_map'].items()}
    ordered_rows = []
    for header_name in uniquify_headers(spec['header_sequence']):
        if header_name in record.source_payload:
            value = record.source_payload.get(header_name)
        else:
            base_header = header_name.split('__', 1)[0]
            attr_name = source_to_attr.get(base_header)
            value = getattr(record, attr_name, None) if attr_name and header_name == base_header else record.source_payload.get(header_name)
        ordered_rows.append((_display_header(header_name), _display_value(value)))
    return ordered_rows


def dashboard(request):
    recent_runs = list(AuditRun.objects.select_related('source_file')[:8])
    open_exceptions = ExceptionRecord.objects.filter(status__in=OPEN_EXCEPTION_STATUSES)
    exception_counts = {
        'open': open_exceptions.count(),
        'high_or_critical': open_exceptions.filter(severity__in=[SeverityChoices.HIGH, SeverityChoices.CRITICAL]).count(),
        'total_runs': AuditRun.objects.count(),
        'dq_warnings': DQFinding.objects.filter(severity='WARNING').count(),
    }
    entity_counts = list(open_exceptions.values('entity').annotate(count=Count('id')).order_by('-count', 'entity'))
    severity_counts = list(open_exceptions.values('severity').annotate(count=Count('id')).order_by('-count', 'severity'))
    max_entity = max([row['count'] for row in entity_counts], default=1)
    max_run_exceptions = max([run.total_exceptions for run in recent_runs], default=1)
    for row in entity_counts:
        row['width'] = int((row['count'] / max_entity) * 100)
    run_series = [
        {
            'label': run.as_of_label or run.started_at.strftime('%Y-%m-%d'),
            'exceptions': run.total_exceptions,
            'warnings': run.total_warnings,
            'width': int((run.total_exceptions / max_run_exceptions) * 100) if max_run_exceptions else 0,
            'run': run,
        }
        for run in reversed(recent_runs)
    ]
    return render(
        request,
        'auditpilot/dashboard.html',
        {
            'recent_runs': recent_runs,
            'exception_counts': exception_counts,
            'entity_counts': entity_counts,
            'severity_counts': severity_counts,
            'run_series': run_series,
        },
    )


def upload_run(request):
    if request.method == 'POST':
        form = UploadWorkbookForm(request.POST, request.FILES)
        if form.is_valid():
            run = process_uploaded_workbook(
                uploaded_file=form.cleaned_data['workbook'],
                as_of_label=form.cleaned_data['as_of_label'],
                uploaded_by=form.cleaned_data['uploaded_by'],
            )
            if run.status == 'FAILED_DQ':
                messages.warning(request, 'Workbook ingested but failed the data quality gate.')
            elif run.status == 'FAILED_PROCESSING':
                messages.error(request, 'Workbook processing failed. Check the run detail for diagnostics.')
            else:
                messages.success(request, 'Workbook processed successfully.')
            return redirect('auditpilot:run_detail', run_id=run.id)
    else:
        form = UploadWorkbookForm()
    return render(request, 'auditpilot/upload.html', {'form': form})


def run_detail(request, run_id):
    run = get_object_or_404(
        AuditRun.objects.select_related('source_file').prefetch_related('sheet_runs', 'dq_findings', 'control_executions__control', 'exceptions__control'),
        pk=run_id,
    )
    visual_pack = build_weekly_visual_pack_context(run)
    exceptions = run.exceptions.select_related('control').order_by('-opened_at')[:30]
    dq_findings = run.dq_findings.select_related('sheet_run')
    return render(
        request,
        'auditpilot/run_detail.html',
        {
            'run': run,
            'exceptions': exceptions,
            'dq_findings': dq_findings,
            'visual_pack': visual_pack,
        },
    )


def exception_list(request):
    exceptions = ExceptionRecord.objects.select_related('control', 'run').order_by('-opened_at')
    status = request.GET.get('status')
    entity = request.GET.get('entity')
    severity = request.GET.get('severity')
    run_id = request.GET.get('run')
    if status:
        exceptions = exceptions.filter(status=status)
    if entity:
        exceptions = exceptions.filter(entity=entity)
    if severity:
        exceptions = exceptions.filter(severity=severity)
    if run_id:
        exceptions = exceptions.filter(run_id=run_id)
    context = {
        'exceptions': exceptions[:200],
        'status': status or '',
        'entity': entity or '',
        'severity': severity or '',
        'run_id': run_id or '',
        'statuses': ExceptionRecord._meta.get_field('status').choices,
        'severities': ExceptionRecord._meta.get_field('severity').choices,
        'entities': ExceptionRecord.objects.order_by().values_list('entity', flat=True).distinct(),
        'runs': AuditRun.objects.order_by('-started_at')[:20],
    }
    return render(request, 'auditpilot/exceptions.html', context)


def exception_detail(request, exception_id):
    exception = get_object_or_404(
        ExceptionRecord.objects.select_related('control', 'run', 'normalized_record'),
        exception_id=exception_id,
    )
    before = {
        'status': exception.status,
        'disposition': exception.disposition,
        'owner_name': exception.owner_name,
        'due_date': exception.due_date.isoformat() if exception.due_date else '',
        'root_cause': exception.root_cause,
        'comment': exception.comment,
    }
    if request.method == 'POST':
        form = ExceptionUpdateForm(request.POST, instance=exception)
        if form.is_valid():
            exception = form.save()
            after = {
                'status': exception.status,
                'disposition': exception.disposition,
                'owner_name': exception.owner_name,
                'due_date': exception.due_date.isoformat() if exception.due_date else '',
                'root_cause': exception.root_cause,
                'comment': exception.comment,
            }
            changes = {key: {'before': before[key], 'after': after[key]} for key in before if before[key] != after[key]}
            if changes:
                ExceptionEvent.objects.create(exception=exception, event_type='manual_update', note='Exception updated from UI', metadata_json=changes)
            messages.success(request, 'Exception updated.')
            return redirect('auditpilot:exception_detail', exception_id=exception.exception_id)
    else:
        form = ExceptionUpdateForm(instance=exception)
    normalized_record = exception.normalized_record
    return render(
        request,
        'auditpilot/exception_detail.html',
        {
            'exception': exception,
            'form': form,
            'exception_context_rows': _build_exception_context_rows(exception),
            'canonical_detail_rows': _build_canonical_detail_rows(normalized_record) if normalized_record else [],
            'source_row_rows': _build_source_row_rows(normalized_record) if normalized_record else [],
        },
    )


def control_catalog(request):
    controls = ControlCatalog.objects.all()
    grouped = {}
    for control in controls:
        grouped.setdefault(control.worksheet_scope, []).append(control)
    return render(request, 'auditpilot/controls.html', {'grouped_controls': grouped, 'controls': controls})


def control_edit(request, control_id):
    control = get_object_or_404(ControlCatalog, pk=control_id)
    if request.method == 'POST':
        form = ControlCatalogForm(request.POST, instance=control)
        if form.is_valid():
            form.save()
            messages.success(request, 'Control updated.')
            return redirect('auditpilot:control_catalog')
    else:
        form = ControlCatalogForm(instance=control)
    return render(request, 'auditpilot/control_edit.html', {'control': control, 'form': form})


def export_run_pack(request, run_id):
    run = get_object_or_404(AuditRun, pk=run_id)
    workbook = build_weekly_pack(run)
    response = HttpResponse(workbook, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f'attachment; filename=weekly-pack-run-{run.id}.xlsx'
    return response


def _visual_board_response(request, run_id, entity=None):
    run = get_object_or_404(AuditRun.objects.select_related('source_file'), pk=run_id)
    visual_pack = build_weekly_visual_pack_context(run)
    board = get_board_context(visual_pack, entity)
    download_view = 'auditpilot:visual_summary_png' if entity is None else 'auditpilot:visual_entity_png'
    preview_view = 'auditpilot:visual_summary' if entity is None else 'auditpilot:visual_entity'
    context = {
        'run': run,
        'visual_pack': visual_pack,
        'board': board,
        'board_type': 'summary' if entity is None else 'entity',
        'entity': entity,
        'download_url': reverse(download_view, args=[run.id] if entity is None else [run.id, entity]),
        'back_url': reverse('auditpilot:run_detail', args=[run.id]),
        'preview_url': reverse(preview_view, args=[run.id] if entity is None else [run.id, entity]),
    }
    return context


def visual_summary(request, run_id):
    context = _visual_board_response(request, run_id)
    return render(request, 'auditpilot/visual_board.html', context)


def visual_summary_png(request, run_id):
    run = get_object_or_404(AuditRun.objects.select_related('source_file'), pk=run_id)
    board = get_board_context(build_weekly_visual_pack_context(run), None)
    png = render_visual_board_png(board)
    response = HttpResponse(png, content_type='image/png')
    response['Content-Disposition'] = f'attachment; filename=weekly-visual-summary-run-{run.id}.png'
    return response


def visual_entity(request, run_id, entity):
    if entity not in SOURCE_SPECS:
        raise Http404('Unknown entity')
    context = _visual_board_response(request, run_id, entity)
    return render(request, 'auditpilot/visual_board.html', context)


def visual_entity_png(request, run_id, entity):
    if entity not in SOURCE_SPECS:
        raise Http404('Unknown entity')
    run = get_object_or_404(AuditRun.objects.select_related('source_file'), pk=run_id)
    board = get_board_context(build_weekly_visual_pack_context(run), entity)
    png = render_visual_board_png(board)
    response = HttpResponse(png, content_type='image/png')
    response['Content-Disposition'] = f'attachment; filename=weekly-visual-{entity.lower()}-run-{run.id}.png'
    return response
