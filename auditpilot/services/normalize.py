from auditpilot.models import NormalizedRecord, RecordClassChoices
from auditpilot.services.constants import SOURCE_SPECS
from auditpilot.services.utils import clean_scalar, parse_date_value, parse_decimal_value, parse_int_value, stable_hash


DERIVED_KEYWORDS = ('result', 'summary', 'subtotal', 'total')


def _is_derived_row(row, fields):
    for field in fields:
        value = clean_scalar(row.get(field))
        if value and any(keyword in str(value).lower() for keyword in DERIVED_KEYWORDS):
            return True
    return False


def normalize_sheet(run, sheet_name, dataframe):
    spec = SOURCE_SPECS[sheet_name]
    mapped_headers = set(spec['canonical_map'].values())
    records = []
    base_count = 0
    derived_count = 0
    for raw_row in dataframe.to_dict(orient='records'):
        cleaned_row = {key: clean_scalar(value) for key, value in raw_row.items()}
        canonical = {}
        for target, source in spec['canonical_map'].items():
            value = cleaned_row.get(source)
            if target in {'document_date', 'baseline_date', 'posting_date', 'payment_entry_date', 'payment_release_date', 'clearing_date', 'net_due_date', 'status_date'}:
                canonical[target] = parse_date_value(value)
            elif target in {'amount_local', 'amount_document', 'days_of_status', 'pot', 'payment_cycle_time', 'total_processing_time'}:
                canonical[target] = parse_decimal_value(value)
            elif target == 'fiscal_year':
                canonical[target] = parse_int_value(value)
            else:
                canonical[target] = '' if value is None else str(value)
        record_class = RecordClassChoices.DERIVED_RESULT if _is_derived_row(cleaned_row, spec['record_class_fields']) else RecordClassChoices.BASE_TRANSACTION
        if record_class == RecordClassChoices.BASE_TRANSACTION:
            base_count += 1
        else:
            derived_count += 1
        transaction_key = stable_hash([
            sheet_name,
            canonical.get('company_code'),
            canonical.get('accounting_document'),
            canonical.get('vendor_code'),
            canonical.get('amount_local'),
            canonical.get('posting_date'),
        ])
        payment_key = stable_hash([
            sheet_name,
            canonical.get('payment_document'),
            cleaned_row.get('Check Number') or cleaned_row.get('Bank Reference'),
            canonical.get('payment_release_date'),
        ])
        record_fingerprint = payment_key if canonical.get('payment_document') else transaction_key
        source_payload = {key: value for key, value in cleaned_row.items() if key not in mapped_headers and key != '__source_row_number'}
        records.append(
            NormalizedRecord(
                run=run,
                entity=sheet_name,
                source_sheet=sheet_name,
                source_row_number=cleaned_row.get('__source_row_number') or 0,
                company_code=canonical.get('company_code', ''),
                vendor_code=canonical.get('vendor_code', ''),
                vendor_name=canonical.get('vendor_name', ''),
                fiscal_year=canonical.get('fiscal_year'),
                payment_document=canonical.get('payment_document', ''),
                accounting_document=canonical.get('accounting_document', ''),
                payment_method=canonical.get('payment_method', ''),
                document_type=canonical.get('document_type', ''),
                process_group=canonical.get('process_group', ''),
                nature_of_transaction=canonical.get('nature_of_transaction', ''),
                status_code=canonical.get('status_code', ''),
                status_description=canonical.get('status_description', ''),
                document_date=canonical.get('document_date'),
                baseline_date=canonical.get('baseline_date'),
                posting_date=canonical.get('posting_date'),
                payment_entry_date=canonical.get('payment_entry_date'),
                payment_release_date=canonical.get('payment_release_date'),
                clearing_date=canonical.get('clearing_date'),
                net_due_date=canonical.get('net_due_date'),
                status_date=canonical.get('status_date'),
                amount_local=canonical.get('amount_local'),
                amount_document=canonical.get('amount_document'),
                days_of_status=canonical.get('days_of_status'),
                pot=canonical.get('pot'),
                payment_cycle_time=canonical.get('payment_cycle_time'),
                total_processing_time=canonical.get('total_processing_time'),
                transaction_key=transaction_key,
                payment_key=payment_key,
                record_class=record_class,
                record_fingerprint=record_fingerprint,
                source_payload=source_payload,
            )
        )
    return records, {'base_transaction_count': base_count, 'derived_row_count': derived_count}
