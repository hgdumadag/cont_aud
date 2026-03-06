from django import forms
from django.utils import timezone

from .models import ControlCatalog, ExceptionRecord, ExceptionStatusChoices


class UploadWorkbookForm(forms.Form):
    workbook = forms.FileField(help_text='Upload the weekly SOC/JGS workbook in .xlsx format.')
    as_of_label = forms.CharField(max_length=120, required=False, help_text='Optional label like 2026-W10.')
    uploaded_by = forms.CharField(max_length=120, required=False, help_text='Name of the person who provided the workbook.')

    def clean_workbook(self):
        workbook = self.cleaned_data['workbook']
        if not workbook.name.lower().endswith('.xlsx'):
            raise forms.ValidationError('Upload a valid .xlsx workbook.')
        return workbook


class ExceptionUpdateForm(forms.ModelForm):
    due_date = forms.DateField(required=False, widget=forms.DateInput(attrs={'type': 'date'}))

    class Meta:
        model = ExceptionRecord
        fields = ['status', 'disposition', 'owner_name', 'due_date', 'root_cause', 'comment']
        widgets = {
            'comment': forms.Textarea(attrs={'rows': 4}),
        }

    def save(self, commit=True):
        instance = super().save(commit=False)
        if instance.status == ExceptionStatusChoices.CLOSED and instance.closed_at is None:
            instance.closed_at = timezone.now()
        if instance.status != ExceptionStatusChoices.CLOSED:
            instance.closed_at = None
        if commit:
            instance.save()
        return instance


class ControlCatalogForm(forms.ModelForm):
    parameters_json = forms.JSONField(widget=forms.Textarea(attrs={'rows': 8}))
    effective_from = forms.DateField(required=False, widget=forms.DateInput(attrs={'type': 'date'}))
    effective_to = forms.DateField(required=False, widget=forms.DateInput(attrs={'type': 'date'}))

    class Meta:
        model = ControlCatalog
        fields = [
            'name',
            'worksheet_scope',
            'template_type',
            'severity',
            'owner_role',
            'description',
            'parameters_json',
            'enabled',
            'effective_from',
            'effective_to',
            'version',
            'display_order',
        ]
        widgets = {
            'description': forms.Textarea(attrs={'rows': 4}),
        }
