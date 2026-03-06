import hashlib
from datetime import date
from decimal import Decimal, InvalidOperation

import pandas as pd


PLACEHOLDER_TOKENS = {
    '',
    '#',
    'not assigned',
    'ugl/not assigned',
    'pep/not assigned',
    'pep/ph/not assigned',
    'nan',
    'none',
    'null',
    'n/a',
}


def normalize_text(value):
    if value is None:
        return ''
    if isinstance(value, str):
        return ' '.join(value.replace('\n', ' ').strip().split())
    return str(value).strip()


def is_placeholder(value):
    text = normalize_text(value).lower()
    if text in PLACEHOLDER_TOKENS:
        return True
    return 'not assigned' in text and '/' in text


def clean_scalar(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if pd.isna(value):
        return None
    if isinstance(value, str):
        text = normalize_text(value)
        return None if is_placeholder(text) else text
    return value


def parse_date_value(value):
    value = clean_scalar(value)
    if value is None:
        return None
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.date()


def parse_decimal_value(value):
    value = clean_scalar(value)
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value.quantize(Decimal('0.01'))
    if isinstance(value, (int, float)):
        return Decimal(str(value)).quantize(Decimal('0.01'))
    text = normalize_text(value)
    if not text:
        return None
    text = text.replace(',', '').replace('PHP', '').replace('Php', '')
    if text.startswith('(') and text.endswith(')'):
        text = f"-{text[1:-1]}"
    try:
        return Decimal(text).quantize(Decimal('0.01'))
    except (InvalidOperation, ValueError):
        return None


def parse_int_value(value):
    decimal_value = parse_decimal_value(value)
    if decimal_value is None:
        return None
    return int(decimal_value)


def stable_hash(parts):
    material = '|'.join('' if part is None else str(part) for part in parts)
    return hashlib.sha1(material.encode('utf-8')).hexdigest()
