from django.db import migrations


STARTER_CONTROLS = [
    {
        'control_id': 'ALL-LIFECYCLE-DATE-ORDER',
        'name': 'Lifecycle dates in logical order',
        'worksheet_scope': 'ALL',
        'template_type': 'date_order',
        'severity': 'HIGH',
        'owner_role': 'AP Control Owner',
        'description': 'Flags records whose critical lifecycle dates move backward in time.',
        'parameters_json': {'pairs': [['document_date', 'posting_date'], ['baseline_date', 'net_due_date'], ['posting_date', 'payment_release_date'], ['payment_release_date', 'clearing_date']], 'record_class': 'base_transaction'},
        'version': 1,
        'display_order': 10,
        'enabled': True,
    },
    {
        'control_id': 'ALL-NON-NEGATIVE-METRICS',
        'name': 'Cycle-time metrics within plausible range',
        'worksheet_scope': 'ALL',
        'template_type': 'non_negative_metric',
        'severity': 'HIGH',
        'owner_role': 'Process Owner',
        'description': 'Flags negative or implausibly high TAT values.',
        'parameters_json': {'record_class': 'base_transaction', 'fields': {'days_of_status': {'min': 0, 'max': 180}, 'payment_cycle_time': {'min': 0, 'max': 365}, 'total_processing_time': {'min': 0, 'max': 365}}},
        'version': 1,
        'display_order': 20,
        'enabled': True,
    },
    {
        'control_id': 'ALL-AGING-BREACH',
        'name': 'Aging over threshold',
        'worksheet_scope': 'ALL',
        'template_type': 'age_over_threshold',
        'severity': 'MEDIUM',
        'owner_role': 'Remediation Owner',
        'description': 'Flags backlog items aging beyond threshold days.',
        'parameters_json': {'field': 'days_of_status', 'threshold': 30, 'exclude_status_contains': ['clear', 'closed', 'released'], 'record_class': 'base_transaction'},
        'version': 1,
        'display_order': 30,
        'enabled': True,
    },
    {
        'control_id': 'ALL-DUPLICATE-TRANSACTION-KEY',
        'name': 'Duplicate transaction candidates',
        'worksheet_scope': 'ALL',
        'template_type': 'duplicate_key',
        'severity': 'HIGH',
        'owner_role': 'Continuous Audit COE',
        'description': 'Flags duplicate normalized transaction keys in a run.',
        'parameters_json': {'field': 'transaction_key', 'record_class': 'base_transaction'},
        'version': 1,
        'display_order': 40,
        'enabled': True,
    },
    {
        'control_id': 'ALL-PLACEHOLDER-SPIKE',
        'name': 'Placeholder density spike',
        'worksheet_scope': 'ALL',
        'template_type': 'placeholder_spike',
        'severity': 'MEDIUM',
        'owner_role': 'Data Owner',
        'description': 'Flags columns whose placeholder rate is high or spikes week over week.',
        'parameters_json': {'threshold': 0.20, 'delta_threshold': 0.10},
        'version': 1,
        'display_order': 50,
        'enabled': True,
    },
    {
        'control_id': 'SOC-CHECK-REFERENCE-REQUIRED',
        'name': 'SOC check payments need reference details',
        'worksheet_scope': 'SOC',
        'template_type': 'required_when',
        'severity': 'HIGH',
        'owner_role': 'AP Control Owner',
        'description': 'Requires key reference fields for check-based SOC payments.',
        'parameters_json': {'record_class': 'base_transaction', 'when': [{'field': 'payment_method', 'op': 'icontains', 'value': 'check'}], 'required_fields': ['Check Number', 'OR Number', 'payment_release_date']},
        'version': 1,
        'display_order': 60,
        'enabled': True,
    },
    {
        'control_id': 'JGS-CHECK-REFERENCE-REQUIRED',
        'name': 'JGS check payments need check details',
        'worksheet_scope': 'JGS',
        'template_type': 'required_when',
        'severity': 'HIGH',
        'owner_role': 'AP Control Owner',
        'description': 'Requires check details for check-based JGS payments.',
        'parameters_json': {'record_class': 'base_transaction', 'when': [{'field': 'payment_method', 'op': 'icontains', 'value': 'check'}], 'required_fields': ['Check Number', 'payment_release_date']},
        'version': 1,
        'display_order': 70,
        'enabled': True,
    },
    {
        'control_id': 'JGS-BANK-REFERENCE-REQUIRED',
        'name': 'JGS transfer payments need bank reference',
        'worksheet_scope': 'JGS',
        'template_type': 'required_when',
        'severity': 'MEDIUM',
        'owner_role': 'Treasury Owner',
        'description': 'Requires bank reference for transfer-based JGS payments.',
        'parameters_json': {'record_class': 'base_transaction', 'when': [{'field': 'payment_method', 'op': 'icontains', 'value': 'transfer'}], 'required_fields': ['Bank Reference']},
        'version': 1,
        'display_order': 80,
        'enabled': True,
    },
    {
        'control_id': 'ALL-RECURRENCE-WINDOW',
        'name': 'Recent repeat exceptions',
        'worksheet_scope': 'ALL',
        'template_type': 'recurrence_window',
        'severity': 'MEDIUM',
        'owner_role': 'IA Governance',
        'description': 'Flags records that recur within a recent lookback window.',
        'parameters_json': {'days': 60, 'record_class': 'base_transaction'},
        'version': 1,
        'display_order': 90,
        'enabled': True,
    },
]


def seed_controls(apps, schema_editor):
    ControlCatalog = apps.get_model('auditpilot', 'ControlCatalog')
    for control in STARTER_CONTROLS:
        ControlCatalog.objects.update_or_create(control_id=control['control_id'], defaults=control)


def unseed_controls(apps, schema_editor):
    ControlCatalog = apps.get_model('auditpilot', 'ControlCatalog')
    ControlCatalog.objects.filter(control_id__in=[control['control_id'] for control in STARTER_CONTROLS]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ('auditpilot', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(seed_controls, unseed_controls),
    ]
