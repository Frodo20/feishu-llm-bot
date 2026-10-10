"""Small, loss-aware views of saved Libra statistics; never infer significance."""

import math

STAT_FIELDS = (
    "value", "relative_diff", "absolute_diff", "p_val", "confidence", "margin", "mde",
)


def numeric(value):
    if type(value) in (int, float):
        return math.isfinite(value)
    if isinstance(value, list):
        return any(numeric(v) for v in value)
    return False


def statistics(data, arguments, *, offset=0, limit=12):
    """Preserve original units, comparator maps and paths, with bounded pagination."""
    selected = {str(v) for v in arguments.get("selected_metric_ids", [])}
    rows = []

    def walk(value, path):
        if isinstance(value, dict):
            if "value" in value and (not selected or selected.intersection(path)):
                rows.append({"path": path, **{k: value[k] for k in STAT_FIELDS if k in value}})
                return
            for key, child in value.items():
                walk(child, [*path, str(key)])
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, [*path, str(index)])

    for key in ("merge_data", "time_series_data"):
        if key in data:
            walk(data[key], [key])
    available = sum(numeric(row["value"]) for row in rows)
    present = {metric for metric in selected if any(metric in row["path"] for row in rows)}
    absent = selected - present
    explicit_missing = data.get("has_stats") == 0
    missing = explicit_missing or (selected and not rows) or (rows and not available)
    status = "missing" if missing else (
        "partial" if (available < len(rows) or absent or data.get("miss_data_date")
                      or data.get("has_full_stats") is False) else
        "available" if available else "unknown"
    )
    # Stop before the serialized page grows unbounded on long time series.
    import json

    page, size = [], 0
    for row in rows[offset:offset + limit]:
        encoded = json.dumps(row, ensure_ascii=False)
        if len(encoded) > 12000:
            # Do not slice series and misalign dates; raw output remains accessible.
            row = {"path": row["path"], "requires_raw_artifact": True}
            encoded = json.dumps(row)
        if page and size + len(encoded) > 12000:
            break
        page.append(row)
        size += len(encoded)
    dimensions = data.get("dim")
    if len(json.dumps(dimensions, ensure_ascii=False)) > 4000:
        dimensions = {"requires_raw_artifact": True}
    return {
        "statistics_status": status,
        "data_available": status in {"available", "partial"},
        "coverage_complete": status == "available",
        "metric_ids": sorted(selected),
        "absent_metric_ids": sorted(absent),
        "window": {k: data.get(k, arguments.get(k)) for k in ("start_date", "end_date")},
        "period_type": arguments.get("period_type"),
        "dimensions": dimensions,
        "view_type": arguments.get("view_type"),
        "base_vid": arguments.get("base_vid"),
        "metadata": {k: data[k] for k in (
            "has_stats", "has_full_stats", "data_status", "query_ready", "merge_type",
        ) if k in data},
        "missing_dates": data.get("miss_data_date", [])[:31],
        "rows": page,
        "total_rows": len(rows),
        "next_offset": offset + len(page) if offset + len(page) < len(rows) else None,
        "interpretation": "Values and confidence fields use the original API units; "
        "do not treat null as zero or infer significance from a status flag.",
    }
