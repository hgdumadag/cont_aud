from django.contrib import admin

from .models import (
    AuditRun,
    ControlCatalog,
    ControlExecution,
    DQFinding,
    ExceptionEvent,
    ExceptionRecord,
    NormalizedRecord,
    SchemaSnapshot,
    SheetRun,
    SourceFileArchive,
)


@admin.register(AuditRun)
class AuditRunAdmin(admin.ModelAdmin):
    list_display = ('id', 'status', 'as_of_label', 'uploaded_by', 'started_at', 'completed_at')
    list_filter = ('status',)
    search_fields = ('as_of_label', 'uploaded_by', 'source_file__original_name')


@admin.register(ControlCatalog)
class ControlCatalogAdmin(admin.ModelAdmin):
    list_display = ('control_id', 'worksheet_scope', 'template_type', 'severity', 'enabled')
    list_filter = ('worksheet_scope', 'template_type', 'severity', 'enabled')
    search_fields = ('control_id', 'name', 'description')


admin.site.register(SourceFileArchive)
admin.site.register(SheetRun)
admin.site.register(SchemaSnapshot)
admin.site.register(DQFinding)
admin.site.register(NormalizedRecord)
admin.site.register(ControlExecution)
admin.site.register(ExceptionRecord)
admin.site.register(ExceptionEvent)
