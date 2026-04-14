import uuid

from django.db import models


class SeverityChoices(models.TextChoices):
    LOW = 'LOW', 'Low'
    MEDIUM = 'MEDIUM', 'Medium'
    HIGH = 'HIGH', 'High'
    CRITICAL = 'CRITICAL', 'Critical'


class FindingSeverity(models.TextChoices):
    INFO = 'INFO', 'Info'
    WARNING = 'WARNING', 'Warning'
    ERROR = 'ERROR', 'Error'


class RunStatusChoices(models.TextChoices):
    PENDING = 'PENDING', 'Pending'
    QUEUED = 'QUEUED', 'Queued'
    RUNNING = 'RUNNING', 'Running'
    COMPLETED = 'COMPLETED', 'Completed'
    FAILED_DQ = 'FAILED_DQ', 'Failed - Data Quality'
    FAILED_PROCESSING = 'FAILED_PROCESSING', 'Failed - Processing'


class SheetStatusChoices(models.TextChoices):
    PASSED = 'PASSED', 'Passed'
    WARNING = 'WARNING', 'Warning'
    FAILED = 'FAILED', 'Failed'


class ControlExecutionStatus(models.TextChoices):
    SUCCEEDED = 'SUCCEEDED', 'Succeeded'
    SKIPPED = 'SKIPPED', 'Skipped'
    FAILED = 'FAILED', 'Failed'


class ExceptionStatusChoices(models.TextChoices):
    NEW = 'NEW', 'New'
    OPEN = 'OPEN', 'Open'
    VALIDATED = 'VALIDATED', 'Validated'
    IN_PROGRESS = 'IN_PROGRESS', 'In Progress'
    CLOSED = 'CLOSED', 'Closed'


class DispositionChoices(models.TextChoices):
    UNDER_REVIEW = 'UNDER_REVIEW', 'Under review'
    ISSUE_REMEDIATE = 'ISSUE_REMEDIATE', 'Issue - Remediate'
    ISSUE_ACCEPTED = 'ISSUE_ACCEPTED', 'Issue - Accept risk / exception'
    DQ_DEFECT = 'DQ_DEFECT', 'Not an issue - Data quality defect'
    EXPECTED_PATTERN = 'EXPECTED_PATTERN', 'Not an issue - Expected pattern'


class RecordClassChoices(models.TextChoices):
    BASE_TRANSACTION = 'base_transaction', 'Base transaction'
    DERIVED_RESULT = 'derived_result', 'Derived result line'


class TemplateTypeChoices(models.TextChoices):
    REQUIRED_WHEN = 'required_when', 'Required when'
    DATE_ORDER = 'date_order', 'Date order'
    NON_NEGATIVE_METRIC = 'non_negative_metric', 'Non-negative metric'
    AGE_OVER_THRESHOLD = 'age_over_threshold', 'Age over threshold'
    DUPLICATE_KEY = 'duplicate_key', 'Duplicate key'
    PLACEHOLDER_SPIKE = 'placeholder_spike', 'Placeholder spike'
    RECURRENCE_WINDOW = 'recurrence_window', 'Recurrence window'
    DQ_FINDING = 'dq_finding', 'DQ finding'


class SourceFileArchive(models.Model):
    original_name = models.CharField(max_length=255)
    stored_file = models.FileField(upload_to='raw_snapshots/%Y/%m/%d')
    sha256 = models.CharField(max_length=64, db_index=True)
    size_bytes = models.PositiveBigIntegerField(default=0)
    uploaded_by = models.CharField(max_length=120, blank=True)
    as_of_label = models.CharField(max_length=120, blank=True)
    metadata_json = models.JSONField(default=dict, blank=True)
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.original_name} ({self.sha256[:10]})"


class AuditRun(models.Model):
    source_file = models.ForeignKey(SourceFileArchive, on_delete=models.PROTECT, related_name='runs')
    run_identifier = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    status = models.CharField(max_length=24, choices=RunStatusChoices.choices, default=RunStatusChoices.PENDING)
    processing_stage = models.CharField(max_length=120, blank=True)
    as_of_label = models.CharField(max_length=120, blank=True)
    uploaded_by = models.CharField(max_length=120, blank=True)
    overall_message = models.TextField(blank=True)
    total_rows = models.PositiveIntegerField(default=0)
    total_exceptions = models.PositiveIntegerField(default=0)
    total_warnings = models.PositiveIntegerField(default=0)
    total_errors = models.PositiveIntegerField(default=0)
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-started_at']

    def __str__(self):
        return f"Run {self.pk} - {self.status}"


class SheetRun(models.Model):
    run = models.ForeignKey(AuditRun, on_delete=models.CASCADE, related_name='sheet_runs')
    sheet_name = models.CharField(max_length=40)
    status = models.CharField(max_length=16, choices=SheetStatusChoices.choices, default=SheetStatusChoices.PASSED)
    row_count = models.PositiveIntegerField(default=0)
    normalized_row_count = models.PositiveIntegerField(default=0)
    base_transaction_count = models.PositiveIntegerField(default=0)
    derived_row_count = models.PositiveIntegerField(default=0)
    schema_version = models.CharField(max_length=32, blank=True)
    required_missing_json = models.JSONField(default=list, blank=True)
    new_columns_json = models.JSONField(default=list, blank=True)
    metrics_json = models.JSONField(default=dict, blank=True)

    class Meta:
        unique_together = ('run', 'sheet_name')
        ordering = ['sheet_name']

    def __str__(self):
        return f"{self.run_id}:{self.sheet_name}"


class SchemaSnapshot(models.Model):
    sheet_name = models.CharField(max_length=40)
    version = models.PositiveIntegerField()
    header_signature = models.CharField(max_length=64, unique=True)
    headers_json = models.JSONField(default=list)
    required_headers_json = models.JSONField(default=list)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('sheet_name', 'version')
        ordering = ['sheet_name', '-version']

    def __str__(self):
        return f"{self.sheet_name} v{self.version}"


class DQFinding(models.Model):
    run = models.ForeignKey(AuditRun, on_delete=models.CASCADE, related_name='dq_findings')
    sheet_run = models.ForeignKey(SheetRun, on_delete=models.CASCADE, related_name='dq_findings', null=True, blank=True)
    severity = models.CharField(max_length=16, choices=FindingSeverity.choices)
    code = models.CharField(max_length=64)
    message = models.TextField()
    details_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['sheet_run__sheet_name', 'severity', 'code']

    def __str__(self):
        return f"{self.code} ({self.severity})"


class NormalizedRecord(models.Model):
    run = models.ForeignKey(AuditRun, on_delete=models.CASCADE, related_name='normalized_records')
    entity = models.CharField(max_length=40)
    source_sheet = models.CharField(max_length=40)
    source_row_number = models.PositiveIntegerField()
    company_code = models.CharField(max_length=64, blank=True)
    vendor_code = models.CharField(max_length=64, blank=True)
    vendor_name = models.CharField(max_length=255, blank=True)
    fiscal_year = models.IntegerField(null=True, blank=True)
    payment_document = models.CharField(max_length=128, blank=True)
    accounting_document = models.CharField(max_length=128, blank=True)
    payment_method = models.CharField(max_length=128, blank=True)
    document_type = models.CharField(max_length=128, blank=True)
    process_group = models.CharField(max_length=128, blank=True)
    nature_of_transaction = models.CharField(max_length=255, blank=True)
    status_code = models.CharField(max_length=128, blank=True)
    status_description = models.CharField(max_length=255, blank=True)
    document_date = models.DateField(null=True, blank=True)
    baseline_date = models.DateField(null=True, blank=True)
    posting_date = models.DateField(null=True, blank=True)
    payment_entry_date = models.DateField(null=True, blank=True)
    payment_release_date = models.DateField(null=True, blank=True)
    clearing_date = models.DateField(null=True, blank=True)
    net_due_date = models.DateField(null=True, blank=True)
    status_date = models.DateField(null=True, blank=True)
    amount_local = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    amount_document = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    days_of_status = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    pot = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    payment_cycle_time = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    total_processing_time = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    transaction_key = models.CharField(max_length=40, db_index=True)
    payment_key = models.CharField(max_length=40, db_index=True)
    record_class = models.CharField(max_length=24, choices=RecordClassChoices.choices)
    record_fingerprint = models.CharField(max_length=40, db_index=True)
    source_payload = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ['source_sheet', 'source_row_number']
        indexes = [
            models.Index(fields=['run', 'entity']),
            models.Index(fields=['run', 'record_class']),
        ]

    def __str__(self):
        return f"{self.source_sheet}:{self.source_row_number}"


class ControlCatalog(models.Model):
    control_id = models.CharField(max_length=80, unique=True)
    name = models.CharField(max_length=140)
    worksheet_scope = models.CharField(max_length=40)
    template_type = models.CharField(max_length=40, choices=TemplateTypeChoices.choices)
    severity = models.CharField(max_length=16, choices=SeverityChoices.choices, default=SeverityChoices.MEDIUM)
    owner_role = models.CharField(max_length=120, blank=True)
    description = models.TextField(blank=True)
    parameters_json = models.JSONField(default=dict, blank=True)
    enabled = models.BooleanField(default=True)
    effective_from = models.DateField(null=True, blank=True)
    effective_to = models.DateField(null=True, blank=True)
    version = models.PositiveIntegerField(default=1)
    display_order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['display_order', 'control_id']

    def __str__(self):
        return self.control_id


class ControlExecution(models.Model):
    run = models.ForeignKey(AuditRun, on_delete=models.CASCADE, related_name='control_executions')
    control = models.ForeignKey(ControlCatalog, on_delete=models.CASCADE, related_name='executions')
    sheet_name = models.CharField(max_length=40)
    status = models.CharField(max_length=16, choices=ControlExecutionStatus.choices, default=ControlExecutionStatus.SUCCEEDED)
    finding_count = models.PositiveIntegerField(default=0)
    details_json = models.JSONField(default=dict, blank=True)
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['sheet_name', 'control__display_order', 'control__control_id']


class ExceptionRecord(models.Model):
    exception_id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    run = models.ForeignKey(AuditRun, on_delete=models.CASCADE, related_name='exceptions')
    control = models.ForeignKey(ControlCatalog, on_delete=models.SET_NULL, null=True, blank=True, related_name='exceptions')
    normalized_record = models.ForeignKey(NormalizedRecord, on_delete=models.SET_NULL, null=True, blank=True, related_name='exceptions')
    recurrence_of = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, blank=True, related_name='recurrences')
    entity = models.CharField(max_length=40)
    record_fingerprint = models.CharField(max_length=40, db_index=True)
    severity = models.CharField(max_length=16, choices=SeverityChoices.choices)
    title = models.CharField(max_length=255)
    detail = models.TextField(blank=True)
    status = models.CharField(max_length=24, choices=ExceptionStatusChoices.choices, default=ExceptionStatusChoices.NEW)
    disposition = models.CharField(max_length=32, choices=DispositionChoices.choices, default=DispositionChoices.UNDER_REVIEW)
    owner_name = models.CharField(max_length=120, blank=True)
    due_date = models.DateField(null=True, blank=True)
    root_cause = models.CharField(max_length=255, blank=True)
    comment = models.TextField(blank=True)
    extra_context = models.JSONField(default=dict, blank=True)
    opened_at = models.DateTimeField(auto_now_add=True)
    closed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-opened_at', '-id']
        indexes = [
            models.Index(fields=['status', 'entity']),
            models.Index(fields=['run', 'severity']),
        ]

    @property
    def is_open(self):
        return self.status != ExceptionStatusChoices.CLOSED


class ExceptionEvent(models.Model):
    exception = models.ForeignKey(ExceptionRecord, on_delete=models.CASCADE, related_name='events')
    event_type = models.CharField(max_length=40)
    note = models.TextField(blank=True)
    metadata_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['created_at']
