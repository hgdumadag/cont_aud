from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Iterable, Sequence

from django.db.models import Count
from django.utils import timezone
from PIL import Image, ImageColor, ImageDraw, ImageFilter, ImageFont

from auditpilot.models import (
    AuditRun,
    DQFinding,
    ExceptionRecord,
    ExceptionStatusChoices,
    FindingSeverity,
    RunStatusChoices,
    SeverityChoices,
)
from auditpilot.services.constants import REQUIRED_SHEETS


BOARD_WIDTH = 3200
BOARD_HEIGHT = 1800
MARGIN_X = 88
MARGIN_Y = 74
GAP = 32

PALETTE = {
    'paper': '#f6f1e6',
    'panel': '#fffdf8',
    'panel_soft': '#f6f2ea',
    'ink': '#16242f',
    'muted': '#61707c',
    'line': '#d7d0c3',
    'shadow': '#bfb8aa',
    'teal': '#0d827a',
    'teal_soft': '#d9f0ee',
    'orange': '#d26d31',
    'orange_soft': '#f5e4d8',
    'gold': '#b98d2b',
    'gold_soft': '#f4e7c4',
    'slate': '#304b60',
    'slate_soft': '#dde4ea',
    'red': '#ba4943',
    'red_soft': '#f6ddd9',
}

FONT_CANDIDATES = {
    'regular': [
        Path('C:/Windows/Fonts/arial.ttf'),
        Path('C:/Windows/Fonts/SegoeUI.ttf'),
    ],
    'bold': [
        Path('C:/Windows/Fonts/arialbd.ttf'),
        Path('C:/Windows/Fonts/SegoeUIBold.ttf'),
    ],
}


@dataclass
class BoardMeta:
    run_id: int
    run_label: str
    as_of_label: str
    uploaded_by: str
    generated_at: str
    baseline_label: str
    baseline_available: bool
    scope_label: str
    filename: str
    board_title: str
    subtitle: str


@dataclass
class KpiTile:
    label: str
    value: str
    sublabel: str = ''
    accent: str = 'teal'
    trend: str = ''


@dataclass
class ComparisonRow:
    label: str
    current: int
    previous: int | None
    current_label: str
    previous_label: str
    delta: int | None
    delta_label: str
    baseline_available: bool
    current_width: int = 0
    previous_width: int = 0
    accent: str = 'slate'


@dataclass
class ThemeRow:
    label: str
    count: int
    share_label: str
    current_width: int = 0


@dataclass
class AlertRow:
    title: str
    detail: str
    severity: str
    code: str


@dataclass
class SummaryBoardContext:
    board_type: str = 'summary'
    meta: BoardMeta | None = None
    kpis: list[KpiTile] = field(default_factory=list)
    entity_rows: list[ComparisonRow] = field(default_factory=list)
    severity_rows: list[ComparisonRow] = field(default_factory=list)
    control_rows: list[ComparisonRow] = field(default_factory=list)
    disposition_rows: list[ComparisonRow] = field(default_factory=list)
    dq_rows: list[AlertRow] = field(default_factory=list)
    theme_rows: list[ThemeRow] = field(default_factory=list)
    action_items: list[str] = field(default_factory=list)


@dataclass
class EntityBoardContext:
    board_type: str = 'entity'
    entity: str = ''
    meta: BoardMeta | None = None
    kpis: list[KpiTile] = field(default_factory=list)
    severity_rows: list[ComparisonRow] = field(default_factory=list)
    control_rows: list[ComparisonRow] = field(default_factory=list)
    theme_rows: list[ThemeRow] = field(default_factory=list)
    dq_rows: list[AlertRow] = field(default_factory=list)
    recent_exceptions: list[ThemeRow] = field(default_factory=list)
    action_items: list[str] = field(default_factory=list)


@dataclass
class WeeklyVisualPackContext:
    run: AuditRun
    baseline_run: AuditRun | None
    summary: SummaryBoardContext
    entities: dict[str, EntityBoardContext]


def _completed_baseline_run(run: AuditRun) -> AuditRun | None:
    return AuditRun.objects.filter(status=RunStatusChoices.COMPLETED, started_at__lt=run.started_at).order_by('-started_at').first()


def _count_query(items: Iterable, key_func) -> Counter:
    counter = Counter()
    for item in items:
        key = key_func(item)
        if key:
            counter[key] += 1
    return counter


def _format_int(value: int) -> str:
    return f'{value:,}'


def _format_signed(value: int | None) -> str:
    if value is None:
        return 'No baseline'
    sign = '+' if value > 0 else ''
    return f'{sign}{value:,}'


def _format_ratio(current: int, previous: int | None) -> str:
    if previous in (None, 0):
        return 'No baseline' if previous is None else 'New baseline'
    delta = current - previous
    pct = (delta / previous) * 100 if previous else 0
    return f'{pct:+.0f}%'


def _pick_accent(index: int) -> str:
    palette = ['teal', 'orange', 'gold', 'slate']
    return palette[index % len(palette)]


def _value_width(value: int, maximum: int) -> int:
    if maximum <= 0:
        return 0
    return max(4, int(round((value / maximum) * 100))) if value else 0


def _comparison_rows(current_rows: Counter, previous_rows: Counter | None, label_prefix: str = '') -> list[ComparisonRow]:
    previous_rows = previous_rows or Counter()
    labels = sorted(set(current_rows) | set(previous_rows))
    maximum = max(
        max(current_rows.values(), default=0),
        max(previous_rows.values(), default=0),
        1,
    )
    rows = []
    for index, label in enumerate(labels):
        current = int(current_rows.get(label, 0))
        previous = previous_rows.get(label)
        delta = None if previous is None else current - int(previous)
        rows.append(
            ComparisonRow(
                label=f'{label_prefix}{label}' if label_prefix else label,
                current=current,
                previous=int(previous) if previous is not None else None,
                current_label=_format_int(current),
                previous_label=_format_int(int(previous)) if previous is not None else 'No baseline',
                delta=delta,
                delta_label=_format_signed(delta),
                baseline_available=previous is not None,
                current_width=_value_width(current, maximum),
                previous_width=_value_width(int(previous), maximum) if previous is not None else 0,
                accent=_pick_accent(index),
            )
        )
    return rows


def _theme_rows(counter: Counter, limit: int = 5) -> list[ThemeRow]:
    total = sum(counter.values()) or 1
    rows = []
    for index, (label, count) in enumerate(counter.most_common(limit)):
        rows.append(
            ThemeRow(
                label=label,
                count=int(count),
                share_label=f'{(count / total) * 100:.0f}%',
                current_width=_value_width(int(count), max(counter.values(), default=1)),
            )
        )
    return rows


def _dq_alert_rows(findings: Sequence[DQFinding], limit: int = 6) -> list[AlertRow]:
    rows = []
    for finding in findings[:limit]:
        sheet_name = finding.sheet_run.sheet_name if finding.sheet_run else 'Run'
        rows.append(
            AlertRow(
                title=f'{sheet_name} · {finding.code}',
                detail=finding.message,
                severity=finding.severity,
                code=finding.code,
            )
        )
    return rows


def _baseline_label(baseline_run: AuditRun | None) -> str:
    if baseline_run is None:
        return 'No previous completed run'
    return f'Run {baseline_run.id} · {baseline_run.as_of_label or baseline_run.started_at.strftime("%Y-%m-%d")}'


def _board_meta(run: AuditRun, baseline_run: AuditRun | None, entity: str | None) -> BoardMeta:
    scope_label = 'Portfolio summary' if entity is None else f'{entity} entity'
    filename = 'weekly-visual-summary-run-{0}.png'.format(run.id) if entity is None else f'weekly-visual-{entity.lower()}-run-{run.id}.png'
    title = 'Weekly visual findings summary' if entity is None else f'{entity} visual findings'
    subtitle = 'Current run vs previous completed run' if baseline_run else 'Current run only'
    return BoardMeta(
        run_id=run.id,
        run_label=f'Run {run.id}',
        as_of_label=run.as_of_label or run.started_at.strftime('%Y-%m-%d'),
        uploaded_by=run.uploaded_by or '-',
        generated_at=timezone.now().strftime('%Y-%m-%d %H:%M'),
        baseline_label=_baseline_label(baseline_run),
        baseline_available=baseline_run is not None,
        scope_label=scope_label,
        filename=filename,
        board_title=title,
        subtitle=subtitle,
    )


def _current_open_exceptions() -> list[ExceptionRecord]:
    return list(
        ExceptionRecord.objects.exclude(status=ExceptionStatusChoices.CLOSED).select_related('control', 'run').order_by('-opened_at')
    )


def _completed_closed_since(previous_run: AuditRun | None, current_run: AuditRun) -> int:
    if previous_run is None:
        return 0
    start = previous_run.completed_at or previous_run.started_at
    end = current_run.completed_at or current_run.started_at or timezone.now()
    return ExceptionRecord.objects.filter(closed_at__gt=start, closed_at__lte=end).count()


def _count_exceptions_by(items: Sequence[ExceptionRecord], key_func) -> Counter:
    return _count_query(items, key_func)


def _count_dq_by_sheet(findings: Sequence[DQFinding]) -> Counter:
    return _count_query(findings, lambda finding: finding.sheet_run.sheet_name if finding.sheet_run else 'Run')


def _top_controls(items: Sequence[ExceptionRecord], entity: str | None = None, limit: int = 5) -> list[ThemeRow]:
    if entity:
        items = [item for item in items if item.entity == entity]
    counter = Counter()
    for item in items:
        label = item.control.control_id if item.control else 'Unassigned'
        counter[label] += 1
    return _theme_rows(counter, limit=limit)


def _top_themes(items: Sequence[ExceptionRecord], entity: str | None = None, limit: int = 5) -> list[ThemeRow]:
    if entity:
        items = [item for item in items if item.entity == entity]
    counter = Counter(item.title for item in items)
    return _theme_rows(counter, limit=limit)


def _recent_exceptions(items: Sequence[ExceptionRecord], entity: str | None = None, limit: int = 5) -> list[ThemeRow]:
    if entity:
        items = [item for item in items if item.entity == entity]
    rows = []
    for item in items[:limit]:
        rows.append(
            ThemeRow(
                label=item.title,
                count=1,
                share_label=f'{item.severity} · {item.status}',
                current_width=100,
            )
        )
    return rows


def _build_action_items(
    baseline_run: AuditRun | None,
    current_open_count: int,
    high_critical_count: int,
    dq_warning_count: int,
    control_rows: Sequence[ThemeRow],
    entity_label: str | None = None,
) -> list[str]:
    items = []
    if baseline_run is None:
        items.append('No previous completed run. Comparison tiles will activate after the next completed run.')
    if high_critical_count:
        items.append(f'{_format_int(high_critical_count)} high/critical open exceptions require review.')
    if dq_warning_count:
        items.append(f'{_format_int(dq_warning_count)} DQ warnings need a data-owner check before sign-off.')
    if control_rows:
        items.append(f'Top trigger: {control_rows[0].label} with {_format_int(control_rows[0].count)} findings.')
    if current_open_count:
        scope = f' for {entity_label}' if entity_label else ''
        items.append(f'Open backlog{scope}: {_format_int(current_open_count)} items currently remain active.')
    return items[:4]


def build_weekly_visual_pack_context(run: AuditRun) -> WeeklyVisualPackContext:
    baseline_run = _completed_baseline_run(run)
    current_exceptions = list(run.exceptions.select_related('control', 'normalized_record').order_by('-opened_at'))
    baseline_exceptions = list(baseline_run.exceptions.select_related('control').order_by('-opened_at')) if baseline_run else []
    current_open = _current_open_exceptions()
    current_dq_findings = list(run.dq_findings.select_related('sheet_run').order_by('sheet_run__sheet_name', 'severity', 'code'))
    warning_dq_findings = [finding for finding in current_dq_findings if finding.severity == FindingSeverity.WARNING]

    current_entity_counts = _count_exceptions_by(current_exceptions, lambda item: item.entity)
    baseline_entity_counts = _count_exceptions_by(baseline_exceptions, lambda item: item.entity) if baseline_run else Counter()
    current_severity_counts = _count_exceptions_by(current_exceptions, lambda item: item.severity)
    baseline_severity_counts = _count_exceptions_by(baseline_exceptions, lambda item: item.severity) if baseline_run else Counter()
    current_disposition_counts = _count_exceptions_by(current_exceptions, lambda item: item.disposition)
    baseline_disposition_counts = _count_exceptions_by(baseline_exceptions, lambda item: item.disposition) if baseline_run else Counter()
    current_control_counts = _count_exceptions_by(current_exceptions, lambda item: item.control.control_id if item.control else 'Unassigned')
    baseline_control_counts = _count_exceptions_by(baseline_exceptions, lambda item: item.control.control_id if item.control else 'Unassigned') if baseline_run else Counter()
    current_title_counts = _count_exceptions_by(current_exceptions, lambda item: item.title)

    open_count = len(current_open)
    high_critical_open = sum(1 for item in current_open if item.severity in {SeverityChoices.HIGH, SeverityChoices.CRITICAL})
    closed_since_previous = _completed_closed_since(baseline_run, run)
    new_this_run = len(current_exceptions)
    net_change = new_this_run - closed_since_previous
    dq_warning_count = len(warning_dq_findings)

    summary = SummaryBoardContext(
        meta=_board_meta(run, baseline_run, None),
        kpis=[
            KpiTile('Open exceptions', _format_int(open_count), 'Current backlog', 'slate'),
            KpiTile('New this run', _format_int(new_this_run), 'Run intake', 'orange'),
            KpiTile('Closed since previous', _format_int(closed_since_previous), 'Resolution movement', 'teal'),
            KpiTile('Net change', _format_signed(net_change), 'New minus closed', 'gold'),
            KpiTile('High/Critical open', _format_int(high_critical_open), 'Priority queue', 'red'),
            KpiTile('DQ warnings', _format_int(dq_warning_count), 'Current run', 'orange'),
        ],
        entity_rows=_comparison_rows(current_entity_counts, baseline_entity_counts, ''),
        severity_rows=_comparison_rows(current_severity_counts, baseline_severity_counts, ''),
        control_rows=_comparison_rows(current_control_counts, baseline_control_counts, ''),
        disposition_rows=_comparison_rows(current_disposition_counts, baseline_disposition_counts, ''),
        dq_rows=_dq_alert_rows(warning_dq_findings),
        theme_rows=_theme_rows(current_title_counts),
        action_items=_build_action_items(baseline_run, open_count, high_critical_open, dq_warning_count, _top_controls(current_exceptions)),
    )

    entities = {}
    for entity in REQUIRED_SHEETS:
        current_entity_exceptions = [item for item in current_exceptions if item.entity == entity]
        baseline_entity_exceptions = [item for item in baseline_exceptions if item.entity == entity] if baseline_run else []
        current_entity_open = [item for item in current_open if item.entity == entity]
        current_entity_dq = [finding for finding in current_dq_findings if finding.sheet_run and finding.sheet_run.sheet_name == entity]
        warning_entity_dq = [finding for finding in current_entity_dq if finding.severity == FindingSeverity.WARNING]
        current_entity_severity_counts = _count_exceptions_by(current_entity_exceptions, lambda item: item.severity)
        baseline_entity_severity_counts = _count_exceptions_by(baseline_entity_exceptions, lambda item: item.severity) if baseline_run else Counter()
        current_entity_control_counts = _count_exceptions_by(current_entity_exceptions, lambda item: item.control.control_id if item.control else 'Unassigned')
        baseline_entity_control_counts = _count_exceptions_by(baseline_entity_exceptions, lambda item: item.control.control_id if item.control else 'Unassigned') if baseline_run else Counter()
        current_entity_title_counts = _count_exceptions_by(current_entity_exceptions, lambda item: item.title)
        high_critical_entity = sum(1 for item in current_entity_open if item.severity in {SeverityChoices.HIGH, SeverityChoices.CRITICAL})
        closed_since_previous_entity = _completed_closed_since(baseline_run, run) if baseline_run else 0
        new_this_run_entity = len(current_entity_exceptions)
        net_change_entity = new_this_run_entity - closed_since_previous_entity
        dq_entity_count = len(warning_entity_dq)
        entity_controls = _top_controls(current_entity_exceptions, entity=entity)

        entities[entity] = EntityBoardContext(
            entity=entity,
            meta=_board_meta(run, baseline_run, entity),
            kpis=[
                KpiTile('Open backlog', _format_int(len(current_entity_open)), 'Current backlog', 'slate'),
                KpiTile('New this run', _format_int(new_this_run_entity), 'Run intake', 'orange'),
                KpiTile('Closed since previous', _format_int(closed_since_previous_entity), 'Resolution movement', 'teal'),
                KpiTile('Net change', _format_signed(net_change_entity), 'New minus closed', 'gold'),
                KpiTile('High/Critical open', _format_int(high_critical_entity), 'Priority queue', 'red'),
                KpiTile('DQ warnings', _format_int(dq_entity_count), 'Current sheet alerts', 'orange'),
            ],
            severity_rows=_comparison_rows(current_entity_severity_counts, baseline_entity_severity_counts, ''),
            control_rows=_comparison_rows(current_entity_control_counts, baseline_entity_control_counts, ''),
            theme_rows=_theme_rows(current_entity_title_counts),
            dq_rows=_dq_alert_rows(warning_entity_dq),
            recent_exceptions=_recent_exceptions(current_entity_exceptions),
            action_items=_build_action_items(baseline_run, len(current_entity_open), high_critical_entity, dq_entity_count, entity_controls, entity_label=entity),
        )

    return WeeklyVisualPackContext(run=run, baseline_run=baseline_run, summary=summary, entities=entities)


def get_board_context(pack: WeeklyVisualPackContext, entity: str | None = None) -> SummaryBoardContext | EntityBoardContext:
    if entity:
        return pack.entities[entity]
    return pack.summary


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = FONT_CANDIDATES['bold' if bold else 'regular']
    for candidate in candidates:
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size=size)
    return ImageFont.load_default()


def _wrap_text(draw: ImageDraw.ImageDraw, text: str, font, max_width: int, max_lines: int | None = None) -> list[str]:
    text = text or ''
    if not text:
        return ['']
    lines = []
    for paragraph in text.splitlines() or ['']:
        words = paragraph.split()
        if not words:
            lines.append('')
            continue
        current = words[0]
        for word in words[1:]:
            candidate = f'{current} {word}'
            if draw.textlength(candidate, font=font) <= max_width:
                current = candidate
            else:
                lines.append(current)
                current = word
                if max_lines and len(lines) >= max_lines:
                    break
        if max_lines and len(lines) >= max_lines:
            break
        lines.append(current)
        if max_lines and len(lines) >= max_lines:
            break
    if max_lines and len(lines) > max_lines:
        lines = lines[:max_lines]
        last = lines[-1]
        ellipsis = '...'
        while draw.textlength(last + ellipsis, font=font) > max_width and last:
            last = last[:-1]
        lines[-1] = f'{last}{ellipsis}' if last else ellipsis
    return lines


def _draw_text_block(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    text: str,
    font,
    fill: str,
    max_width: int,
    line_gap: int = 8,
    max_lines: int | None = None,
) -> int:
    lines = _wrap_text(draw, text, font, max_width, max_lines=max_lines)
    current_y = y
    bbox = draw.textbbox((0, 0), 'Ag', font=font)
    line_height = bbox[3] - bbox[1]
    for line in lines:
        draw.text((x, current_y), line, font=font, fill=fill)
        current_y += line_height + line_gap
    return current_y - y


def _draw_panel(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], fill: str, outline: str, radius: int = 28) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=2)


def _accent_fill(accent: str) -> str:
    return {
        'teal': PALETTE['teal_soft'],
        'orange': PALETTE['orange_soft'],
        'gold': PALETTE['gold_soft'],
        'slate': PALETTE['slate_soft'],
        'red': PALETTE['red_soft'],
    }.get(accent, PALETTE['panel_soft'])


def _accent_color(accent: str) -> str:
    return {
        'teal': PALETTE['teal'],
        'orange': PALETTE['orange'],
        'gold': PALETTE['gold'],
        'slate': PALETTE['slate'],
        'red': PALETTE['red'],
    }.get(accent, PALETTE['slate'])


def _draw_kpi_tiles(draw: ImageDraw.ImageDraw, x: int, y: int, width: int, tiles: Sequence[KpiTile], title_font, value_font, body_font) -> int:
    if not tiles:
        return 0
    tile_gap = 24
    tile_width = (width - tile_gap * (len(tiles) - 1)) // len(tiles)
    tile_height = 184
    for index, tile in enumerate(tiles):
        left = x + index * (tile_width + tile_gap)
        right = left + tile_width
        box = (left, y, right, y + tile_height)
        _draw_panel(draw, box, _accent_fill(tile.accent), PALETTE['line'], radius=28)
        draw.rounded_rectangle((left, y, right, y + tile_height), radius=28, outline=_accent_color(tile.accent), width=3)
        draw.text((left + 24, y + 20), tile.label.upper(), font=body_font, fill=PALETTE['muted'])
        draw.text((left + 24, y + 56), tile.value, font=value_font, fill=PALETTE['ink'])
        if tile.sublabel:
            _draw_text_block(draw, left + 24, y + 118, tile.sublabel, body_font, PALETTE['muted'], tile_width - 48, max_lines=2)
        if tile.trend:
            draw.text((right - 24 - draw.textlength(tile.trend, font=body_font), y + 20), tile.trend, font=body_font, fill=_accent_color(tile.accent))
    return tile_height


def _draw_row_chart(
    draw: ImageDraw.ImageDraw,
    box: tuple[int, int, int, int],
    title: str,
    rows: Sequence[ComparisonRow | ThemeRow],
    title_font,
    label_font,
    body_font,
    show_previous: bool = True,
) -> None:
    x0, y0, x1, y1 = box
    _draw_panel(draw, box, PALETTE['panel'], PALETTE['line'])
    draw.text((x0 + 24, y0 + 20), title, font=title_font, fill=PALETTE['ink'])
    body_top = y0 + 78
    row_height = 88 if rows and isinstance(rows[0], ComparisonRow) else 84
    max_rows = max(1, int((y1 - body_top - 18) / row_height))
    for index, row in enumerate(rows[:max_rows]):
        row_y = body_top + index * row_height
        if isinstance(row, ComparisonRow):
            draw.text((x0 + 24, row_y), row.label, font=label_font, fill=PALETTE['ink'])
            current_bar_x = x0 + 270
            bar_width = x1 - current_bar_x - 170
            draw.rounded_rectangle((current_bar_x, row_y + 10, current_bar_x + bar_width, row_y + 30), radius=10, fill='#edf1f4')
            draw.rounded_rectangle((current_bar_x, row_y + 10, current_bar_x + int(bar_width * (row.current_width / 100)), row_y + 30), radius=10, fill=_accent_color(row.accent))
            if show_previous and row.baseline_available:
                draw.rounded_rectangle((current_bar_x, row_y + 38, current_bar_x + bar_width, row_y + 58), radius=10, fill='#edf1f4')
                draw.rounded_rectangle((current_bar_x, row_y + 38, current_bar_x + int(bar_width * (row.previous_width / 100)), row_y + 58), radius=10, fill=PALETTE['gold'])
            current_text = row.current_label
            previous_text = row.previous_label if row.baseline_available else 'No baseline'
            draw.text((x1 - 150, row_y - 2), current_text, font=body_font, fill=PALETTE['ink'])
            draw.text((x1 - 150, row_y + 30), previous_text, font=body_font, fill=PALETTE['muted'])
        else:
            draw.text((x0 + 24, row_y), row.label, font=label_font, fill=PALETTE['ink'])
            draw.text((x1 - 116, row_y), row.share_label, font=body_font, fill=PALETTE['muted'])
            draw.text((x1 - 116, row_y + 30), _format_int(row.count), font=body_font, fill=PALETTE['ink'])
            bar_width = x1 - (x0 + 270) - 130
            draw.rounded_rectangle((x0 + 270, row_y + 10, x0 + 270 + bar_width, row_y + 30), radius=10, fill='#edf1f4')
            draw.rounded_rectangle((x0 + 270, row_y + 10, x0 + 270 + int(bar_width * (row.current_width / 100)), row_y + 30), radius=10, fill=PALETTE['teal'])


def _draw_alert_list(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], title: str, rows: Sequence[AlertRow], title_font, label_font, body_font) -> None:
    x0, y0, x1, y1 = box
    _draw_panel(draw, box, PALETTE['panel'], PALETTE['line'])
    draw.text((x0 + 24, y0 + 20), title, font=title_font, fill=PALETTE['ink'])
    if not rows:
        draw.text((x0 + 24, y0 + 84), 'No data-quality warnings in the current selection.', font=body_font, fill=PALETTE['muted'])
        return
    row_y = y0 + 80
    for row in rows[:5]:
        label = row.title
        _draw_text_block(draw, x0 + 24, row_y, label, label_font, PALETTE['ink'], x1 - x0 - 280, max_lines=2)
        detail_top = row_y + 36
        _draw_text_block(draw, x0 + 24, detail_top, row.detail, body_font, PALETTE['muted'], x1 - x0 - 280, max_lines=3)
        swatch = _accent_color('orange' if row.severity == FindingSeverity.WARNING else 'red')
        draw.rounded_rectangle((x1 - 174, row_y + 4, x1 - 24, row_y + 38), radius=10, fill=_accent_fill('orange' if row.severity == FindingSeverity.WARNING else 'red'))
        draw.text((x1 - 154, row_y + 8), row.severity, font=label_font, fill=swatch)
        row_y += 108


def _draw_action_items(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], title: str, items: Sequence[str], title_font, body_font) -> None:
    x0, y0, x1, y1 = box
    _draw_panel(draw, box, PALETTE['panel'], PALETTE['line'])
    draw.text((x0 + 24, y0 + 20), title, font=title_font, fill=PALETTE['ink'])
    if not items:
        draw.text((x0 + 24, y0 + 84), 'No additional actions were generated for this board.', font=body_font, fill=PALETTE['muted'])
        return
    current_y = y0 + 84
    for item in items[:4]:
        draw.ellipse((x0 + 24, current_y + 7, x0 + 38, current_y + 21), fill=PALETTE['orange'])
        current_y += _draw_text_block(draw, x0 + 52, current_y, item, body_font, PALETTE['ink'], x1 - x0 - 86, max_lines=3) + 18


def _draw_footer(draw: ImageDraw.ImageDraw, meta: BoardMeta, box: tuple[int, int, int, int], body_font, label_font) -> None:
    x0, y0, x1, y1 = box
    draw.line((x0, y0, x1, y0), fill=PALETTE['line'], width=2)
    draw.text((x0, y0 + 14), f"{meta.run_label} · {meta.scope_label} · Generated {meta.generated_at}", font=label_font, fill=PALETTE['muted'])
    right_text = f'Baseline: {meta.baseline_label}'
    draw.text((x1 - draw.textlength(right_text, font=label_font), y0 + 14), right_text, font=label_font, fill=PALETTE['muted'])


def render_visual_board_png(board: SummaryBoardContext | EntityBoardContext) -> bytes:
    meta = board.meta
    if meta is None:
        raise ValueError('Board metadata is required')

    canvas = Image.new('RGBA', (BOARD_WIDTH, BOARD_HEIGHT), ImageColor.getcolor(PALETTE['paper'], 'RGBA'))
    draw = ImageDraw.Draw(canvas)
    title_font = _load_font(28, bold=True)
    body_font = _load_font(22)
    body_bold_font = _load_font(22, bold=True)
    hero_font = _load_font(54, bold=True)
    hero_small_font = _load_font(24)
    kpi_title_font = _load_font(20, bold=True)
    kpi_value_font = _load_font(42, bold=True)

    draw.rounded_rectangle((MARGIN_X, MARGIN_Y, BOARD_WIDTH - MARGIN_X, BOARD_HEIGHT - MARGIN_Y), radius=40, fill=PALETTE['panel_soft'], outline=PALETTE['line'], width=2)
    draw.rounded_rectangle((MARGIN_X + 26, MARGIN_Y + 22, BOARD_WIDTH - MARGIN_X - 26, MARGIN_Y + 184), radius=34, fill=PALETTE['slate'], outline=PALETTE['slate'])
    draw.text((MARGIN_X + 56, MARGIN_Y + 54), meta.board_title, font=hero_font, fill='white')
    draw.text((MARGIN_X + 58, MARGIN_Y + 122), meta.subtitle, font=hero_small_font, fill='#d8e4ea')
    baseline_text = 'No previous completed run' if not meta.baseline_available else meta.baseline_label
    draw.rounded_rectangle((BOARD_WIDTH - MARGIN_X - 760, MARGIN_Y + 44, BOARD_WIDTH - MARGIN_X - 44, MARGIN_Y + 122), radius=24, fill='#f0ece3', outline='#c7beb0', width=2)
    draw.text((BOARD_WIDTH - MARGIN_X - 724, MARGIN_Y + 68), baseline_text, font=body_font, fill=PALETTE['ink'])

    content_top = MARGIN_Y + 212
    kpi_height = _draw_kpi_tiles(draw, MARGIN_X + 14, content_top, BOARD_WIDTH - 2 * MARGIN_X - 28, board.kpis, kpi_title_font, kpi_value_font, body_font)
    cards_top = content_top + kpi_height + 34
    card_height = 566
    card_width = (BOARD_WIDTH - 2 * MARGIN_X - GAP) // 2
    left_x = MARGIN_X
    right_x = left_x + card_width + GAP

    if board.board_type == 'summary':
        _draw_row_chart(draw, (left_x, cards_top, left_x + card_width, cards_top + card_height), 'Comparison by entity', board.entity_rows, title_font, body_bold_font, body_font)
        _draw_row_chart(draw, (right_x, cards_top, right_x + card_width, cards_top + card_height), 'Top triggered controls', board.control_rows, title_font, body_bold_font, body_font, show_previous=False)
        lower_top = cards_top + card_height + GAP
        lower_height = 426
        _draw_row_chart(draw, (left_x, lower_top, left_x + card_width, lower_top + lower_height), 'Severity mix', board.severity_rows, title_font, body_bold_font, body_font)
        _draw_alert_list(draw, (right_x, lower_top, right_x + card_width, lower_top + lower_height), 'DQ alerts', board.dq_rows, title_font, body_bold_font, body_font)
        footer_box = (MARGIN_X + 36, BOARD_HEIGHT - MARGIN_Y - 76, BOARD_WIDTH - MARGIN_X - 36, BOARD_HEIGHT - MARGIN_Y - 40)
        _draw_footer(draw, meta, footer_box, body_font, body_bold_font)
    else:
        _draw_row_chart(draw, (left_x, cards_top, left_x + card_width, cards_top + card_height), 'Severity mix', board.severity_rows, title_font, body_bold_font, body_font)
        _draw_row_chart(draw, (right_x, cards_top, right_x + card_width, cards_top + card_height), 'Top controls', board.control_rows, title_font, body_bold_font, body_font, show_previous=False)
        lower_top = cards_top + card_height + GAP
        lower_height = 426
        _draw_row_chart(draw, (left_x, lower_top, left_x + card_width, lower_top + lower_height), 'Top exception themes', board.theme_rows, title_font, body_bold_font, body_font, show_previous=False)
        _draw_action_items(draw, (right_x, lower_top, right_x + card_width, lower_top + lower_height), 'Action items', board.action_items, title_font, body_font)
        footer_box = (MARGIN_X + 36, BOARD_HEIGHT - MARGIN_Y - 76, BOARD_WIDTH - MARGIN_X - 36, BOARD_HEIGHT - MARGIN_Y - 40)
        _draw_footer(draw, meta, footer_box, body_font, body_bold_font)

    output = BytesIO()
    canvas.convert('RGB').save(output, format='PNG', optimize=True)
    return output.getvalue()


def build_board_context(run: AuditRun, entity: str | None = None) -> SummaryBoardContext | EntityBoardContext:
    pack = build_weekly_visual_pack_context(run)
    return get_board_context(pack, entity)
