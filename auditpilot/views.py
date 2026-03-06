from django.contrib import messages
from django.db.models import Count
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect, render

from .forms import ControlCatalogForm, ExceptionUpdateForm, UploadWorkbookForm
from .models import AuditRun, ControlCatalog, DQFinding, ExceptionEvent, ExceptionRecord, ExceptionStatusChoices, SeverityChoices
from .services.exports import build_weekly_pack
from .services.pipeline import process_uploaded_workbook


OPEN_EXCEPTION_STATUSES = [
    ExceptionStatusChoices.NEW,
    ExceptionStatusChoices.OPEN,
    ExceptionStatusChoices.VALIDATED,
    ExceptionStatusChoices.IN_PROGRESS,
]


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
    exceptions = run.exceptions.select_related('control').order_by('-opened_at')[:30]
    dq_findings = run.dq_findings.select_related('sheet_run')
    return render(request, 'auditpilot/run_detail.html', {'run': run, 'exceptions': exceptions, 'dq_findings': dq_findings})


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
    exception = get_object_or_404(ExceptionRecord.objects.select_related('control', 'run'), exception_id=exception_id)
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
    return render(request, 'auditpilot/exception_detail.html', {'exception': exception, 'form': form})


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
