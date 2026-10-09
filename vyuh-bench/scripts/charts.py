#!/usr/bin/env python3
from __future__ import annotations

from html import escape
from pathlib import Path
import math
import shutil
from typing import Any


WIDTH = 1200
HEIGHT = 720
PLOT = (110, 165, 1080, 560)
FRAMEWORKS = ["vyuh", "axum", "rocket", "actix", "fastapi", "drf"]
COLORS = {
    "vyuh": "#0f766e",
    "axum": "#2563eb",
    "rocket": "#d97706",
    "actix": "#dc2626",
    "fastapi": "#059669",
    "drf": "#4f46e5",
    "other": "#64748b",
}


def row_value(rows: list[dict[str, Any]], tier: str, scenario: str, concurrency: int) -> list[tuple[str, float]]:
    values: list[tuple[str, float]] = []
    for framework in FRAMEWORKS:
        match = next(
            (
                row
                for row in rows
                if row["tier"] == tier
                and row["framework"] == framework
                and row["scenario"] == scenario
                and row["concurrency"] == concurrency
            ),
            None,
        )
        if match is not None:
            values.append((framework, float(match["requests_per_second"])))
    return values


def peak_memory(rows: list[dict[str, Any]], tier: str) -> list[tuple[str, float]]:
    values: list[tuple[str, float]] = []
    for framework in FRAMEWORKS:
        subset = [row for row in rows if row["tier"] == tier and row["framework"] == framework]
        if subset:
            values.append((framework, max(row["rss_after_kb"] for row in subset) / 1024))
    return values


def metric_values(metrics: list[dict[str, Any]], tier: str, key: str) -> list[tuple[str, float]]:
    values: list[tuple[str, float]] = []
    for framework in FRAMEWORKS:
        target = f"{tier}/{framework}"
        match = next((item for item in metrics if item["target"] == target), None)
        if match is not None:
            values.append((framework, float(match[key])))
    return values


def nice_max(value: float) -> float:
    if value <= 0:
        return 1
    exp = math.floor(math.log10(value))
    base = 10**exp
    for step in [1, 2, 5, 10]:
        candidate = step * base
        if candidate >= value:
            return candidate
    return value


def svg_chart(path: Path, title: str, subtitle: str, unit: str, data: list[tuple[str, float]]) -> None:
    max_value = nice_max(max((value for _, value in data), default=1) * 1.12)
    left, top, right, bottom = PLOT
    width = right - left
    bar_gap = 24
    bar_width = max(28, (width - bar_gap * (len(data) + 1)) / max(1, len(data)))
    parts = svg_header(title, subtitle)
    parts.extend(svg_grid(max_value, unit))
    for index, (label, value) in enumerate(data):
        x = left + bar_gap + index * (bar_width + bar_gap)
        height = (value / max_value) * (bottom - top)
        y = bottom - height
        color = COLORS.get(label, COLORS["other"])
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{height:.1f}" rx="14" fill="{color}"/>')
        parts.append(f'<text x="{x + bar_width / 2:.1f}" y="{y - 14:.1f}" text-anchor="middle" class="value">{value:.0f}</text>')
        parts.append(f'<text x="{x + bar_width / 2:.1f}" y="615" text-anchor="middle" class="label">{escape(label)}</text>')
    parts.append("</svg>\n")
    path.write_text("\n".join(parts))


def svg_header(title: str, subtitle: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}" width="{WIDTH}" height="{HEIGHT}">',
        "<style>",
        "text{font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}",
        ".title{font-size:38px;font-weight:780;fill:#0f172a}.subtitle{font-size:20px;fill:#475569}",
        ".tick{font-size:15px;fill:#64748b}.label{font-size:20px;font-weight:700;fill:#0f172a}.value{font-size:18px;font-weight:700;fill:#0f172a}",
        "</style>",
        '<rect width="1200" height="720" rx="32" fill="#f8fafc"/>',
        '<path d="M72 610 C240 520 330 660 520 575 S850 460 1128 535" fill="none" stroke="#ccfbf1" stroke-width="80" stroke-linecap="round" opacity=".8"/>',
        f'<text x="74" y="72" class="title">{escape(title)}</text>',
        f'<text x="76" y="106" class="subtitle">{escape(subtitle)}</text>',
    ]


def svg_grid(max_value: float, unit: str) -> list[str]:
    left, top, right, bottom = PLOT
    parts = [f'<line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="#334155" stroke-width="2"/>']
    for tick in range(5):
        value = max_value * tick / 4
        y = bottom - (bottom - top) * tick / 4
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{right}" y2="{y:.1f}" stroke="#cbd5e1" stroke-width="1"/>')
        parts.append(f'<text x="{left - 18}" y="{y + 5:.1f}" text-anchor="end" class="tick">{value:.0f} {escape(unit)}</text>')
    return parts


def png_chart(path: Path, title: str, subtitle: str, unit: str, data: list[tuple[str, float]]) -> bool:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        return False
    image = Image.new("RGB", (WIDTH, HEIGHT), "#f8fafc")
    draw = ImageDraw.Draw(image)
    fonts = load_fonts(ImageFont)
    draw.rounded_rectangle((54, 36, 1146, 684), radius=30, fill="#ffffff", outline="#e2e8f0", width=2)
    draw.arc((-80, 450, 620, 890), start=200, end=340, fill="#ccfbf1", width=70)
    draw.text((74, 54), title, fill="#0f172a", font=fonts["title"])
    draw.text((76, 102), subtitle, fill="#475569", font=fonts["subtitle"])
    draw_grid(draw, fonts, unit, data)
    draw_bars(draw, fonts, data)
    image.save(path)
    return True


def load_fonts(image_font: Any) -> dict[str, Any]:
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    ]
    regular = next((path for path in candidates if Path(path).exists() and "Bold" not in path), "")
    bold = next((path for path in candidates if Path(path).exists() and "Bold" in path), regular)
    if regular:
        return {
            "title": image_font.truetype(bold, 38),
            "subtitle": image_font.truetype(regular, 21),
            "tick": image_font.truetype(regular, 16),
            "label": image_font.truetype(bold, 19),
            "value": image_font.truetype(bold, 18),
        }
    default = image_font.load_default()
    return {"title": default, "subtitle": default, "tick": default, "label": default, "value": default}


def draw_grid(draw: Any, fonts: dict[str, Any], unit: str, data: list[tuple[str, float]]) -> None:
    left, top, right, bottom = PLOT
    max_value = nice_max(max((value for _, value in data), default=1) * 1.12)
    draw.line((left, bottom, right, bottom), fill="#334155", width=2)
    for tick in range(5):
        value = max_value * tick / 4
        y = bottom - (bottom - top) * tick / 4
        draw.line((left, y, right, y), fill="#cbd5e1", width=1)
        draw.text((20, y - 9), f"{value:.0f} {unit}", fill="#64748b", font=fonts["tick"])


def draw_bars(draw: Any, fonts: dict[str, Any], data: list[tuple[str, float]]) -> None:
    left, top, right, bottom = PLOT
    max_value = nice_max(max((value for _, value in data), default=1) * 1.12)
    bar_gap = 24
    bar_width = max(28, (right - left - bar_gap * (len(data) + 1)) / max(1, len(data)))
    for index, (label, value) in enumerate(data):
        x = left + bar_gap + index * (bar_width + bar_gap)
        height = (value / max_value) * (bottom - top)
        y = bottom - height
        color = COLORS.get(label, COLORS["other"])
        draw.rounded_rectangle((x, y, x + bar_width, bottom), radius=14, fill=color)
        draw.text((x + 8, y - 28), f"{value:.0f}", fill="#0f172a", font=fonts["value"])
        draw.text((x + 8, 604), label, fill="#0f172a", font=fonts["label"])


def chart_specs(rows: list[dict[str, Any]], metrics: list[dict[str, Any]]) -> list[tuple[str, str, str, str, list[tuple[str, float]]]]:
    return [
        ("t1-echo-c64-rps", "Tier 1 Echo Throughput", "Concurrency 64, typed JSON echo", "rps", row_value(rows, "t1", "echo", 64)),
        ("t2-write-c256-rps", "Tier 2 Write Throughput", "Postgres event writes at concurrency 256", "rps", row_value(rows, "t2", "events_write", 256)),
        ("t2-peak-rss-mib", "Tier 2 Peak Memory", "Peak server RSS after measured scenarios", "MiB", peak_memory(rows, "t2")),
        ("t2-source-tokens", "Tier 2 Source Tokens", "Implementation source-token count", "tok", metric_values(metrics, "t2", "tokens")),
        ("t2-source-loc", "Tier 2 Source LOC", "Non-blank, non-comment implementation lines", "LOC", metric_values(metrics, "t2", "loc")),
    ]


def emit(rows: list[dict[str, Any]], metrics: list[dict[str, Any]], raw: Path, root: Path) -> list[dict[str, str]]:
    charts_root = root / "charts"
    raw_dir = charts_root / raw.name
    latest_dir = charts_root / "latest"
    raw_dir.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)
    emitted: list[dict[str, str]] = []
    for slug, title, subtitle, unit, data in chart_specs(rows, metrics):
        if not data:
            continue
        svg_path = raw_dir / f"{slug}.svg"
        png_path = raw_dir / f"{slug}.png"
        svg_chart(svg_path, title, subtitle, unit, data)
        has_png = png_chart(png_path, title, subtitle, unit, data)
        copy_latest(svg_path, latest_dir / svg_path.name)
        if has_png:
            copy_latest(png_path, latest_dir / png_path.name)
        emitted.append({"name": title, "svg": str(svg_path.relative_to(root)), "png": str(png_path.relative_to(root)) if has_png else ""})
    return emitted


def copy_latest(source: Path, target: Path) -> None:
    shutil.copy2(source, target)
