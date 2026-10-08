"""Reviewed read endpoints for Libra CLI 0.2.1, with shell-free parameter contracts.

important-impact and tip-info use POST to query report previews; neither updates
experiments. Mutation, raw body and force-export options are deliberately absent.
"""

from __future__ import annotations

import csv
import io
import json
import re
from datetime import datetime

DEFAULT_LIBRA_CLI = "libra-cli"
CONTRACT_VERSION = "libra-0.2.1-20260928"


def field(kind, *, required=False, default=None, enum=None):
    result = {"type": kind, "required": required}
    if default is not None:
        result["default"] = default
    if enum is not None:
        result["enum"] = enum
    return result


ID = field("id", required=True)
OPTIONAL_ID = field("id")
DATES = {"start_date": field("date", required=True), "end_date": field("date", required=True)}
REPORT = {
    "experiment_id": ID,
    "app_id": ID,
    "base_version_id": ID,
    "version_ids": field("ids", required=True),
    "bundle_id": ID,
    **DATES,
    "data_region": field("string", default="other", enum=["other", "eu_ttp", "tx"]),
    "combine": field("boolean", default=False),
    "mult_cmp_corr": field("bit", default=1),
}
ACTIONS = {
    "experiment_get": {
        "command": ["experiment", "get"],
        "fields": {
            "experiment_id": ID,
            "app_id": OPTIONAL_ID,
            "with": field("blocks", default=["versions", "analysis", "real_traffic"]),
            "with_version_config": field("boolean", default=True),
        },
    },
    "metric_search": {
        "command": ["metrics", "search"],
        "fields": {
            "experiment_id": ID,
            "metric_keys": field("strings", required=True),
            "top": field("top", default=10),
            "workers": field("workers", default=8),
        },
    },
    "report_data": {
        "command": ["metrics", "report-data"],
        "fields": {
            "experiment_id": ID,
            "app_id": OPTIONAL_ID,
            "metric_group": ID,
            **DATES,
            "period_type": field("string", required=True, enum=["d", "h"]),
            "base_vid": OPTIONAL_ID,
            "selected_metric_ids": field("ids"),
            "view_type": field("string", default="merge", enum=["merge", "series"]),
            "merge_type": field("string", default="avg", enum=["avg", "sum", "total"]),
            "data_region": REPORT["data_region"],
            "combine": field("boolean", default=False),
            "mult_cmp_corr": field("boolean", default=True),
            "confidence_threshold": field("probability"),
            "selected_vids": field("ids"),
        },
    },
    "report_bundle": {
        "command": ["conclusion", "report-bundle"],
        "fields": {"experiment_id": ID, "app_id": ID},
    },
    "important_impact": {"command": ["conclusion", "important-impact"], "fields": REPORT},
    "tip_info": {
        "command": ["conclusion", "tip-info"],
        "fields": {
            **REPORT,
            "type": field("strings", default=["risk", "impact"]),
            "global_dimension_with_metric": field("boolean", default=False),
            "global_dimensions": field("dimensions", default=[]),
            "is_bundle_report": field("boolean", default=True),
        },
    },
    "recycle": {"command": ["conclusion", "recycle"], "fields": {"experiment_id": ID}},
}


def _value(name, value, spec):
    kind = spec["type"]
    good = False
    if kind in {"id", "top", "workers", "bit"}:
        bounds = {"id": (1, 2**63 - 1), "top": (1, 50), "workers": (1, 16), "bit": (0, 1)}
        low, high = bounds[kind]
        good = type(value) is int and low <= value <= high
    elif kind == "boolean":
        good = type(value) is bool
    elif kind in {"string", "date"}:
        good = isinstance(value, str) and 0 < len(value) <= 256 and "\x00" not in value
    elif kind == "probability":
        good = type(value) in {int, float} and 0 < value < 1
    elif kind in {"ids", "strings", "blocks"}:
        good = isinstance(value, list) and 1 <= len(value) <= 50
        if good:
            item_spec = {"type": "id" if kind == "ids" else "string"}
            value = [_value(name + "[]", v, item_spec) for v in value]
        if kind == "blocks" and good:
            good = set(value) <= {
                "versions",
                "review",
                "relations",
                "layer",
                "analysis",
                "real_traffic",
                "domain_group",
                "launch_info",
            }
    elif kind == "dimensions":
        good = isinstance(value, list) and len(value) <= 20
        if good:
            for dimension in value:
                if not isinstance(dimension, dict) or set(dimension) != {
                    "global_dim_id",
                    "global_dim_vals",
                }:
                    good = False
                    break
                _value("global_dim_id", dimension["global_dim_id"], ID)
                _value("global_dim_vals", dimension["global_dim_vals"], {"type": "ids"})
    if not good or ("enum" in spec and value not in spec["enum"]):
        raise ValueError(
            f"arguments.{name} must match {kind}" + (f" {spec['enum']}" if "enum" in spec else "")
        )
    return value


def request(inputs):
    if set(inputs) - {"operation_key", "action", "arguments", "timeout_seconds"}:
        raise ValueError("Unsupported Libra arguments")
    action, arguments = inputs.get("action"), inputs.get("arguments")
    if not isinstance(action, str) or action not in ACTIONS:
        raise ValueError("Unknown Libra read action; call libra_read action=help")
    if not isinstance(arguments, dict):
        raise ValueError("arguments must be an object; metric_keys must be a JSON array")
    fields = ACTIONS[action]["fields"]
    if set(arguments) - set(fields):
        raise ValueError(
            "Unsupported arguments: " + ", ".join(sorted(set(arguments) - set(fields)))
        )
    normalized = {}
    for key, spec in fields.items():
        if key in arguments:
            normalized[key] = _value(key, arguments[key], spec)
        elif "default" in spec:
            normalized[key] = spec["default"]
        elif spec["required"]:
            raise ValueError(f"arguments.{key} is required")
    if "start_date" in normalized:
        fmt = "%Y-%m-%d %H:%M" if normalized.get("period_type") == "h" else "%Y-%m-%d"
        try:
            start, end = [datetime.strptime(normalized[k], fmt) for k in DATES]
        except ValueError as exc:
            raise ValueError(f"start_date/end_date must match {fmt}") from exc
        if start > end:
            raise ValueError("end_date must not precede start_date")
    timeout = inputs.get("timeout_seconds", 60)
    if type(timeout) is not int or not 1 <= timeout <= 180:
        raise ValueError("Libra timeout_seconds must be within 1-180")
    return {"action": action, "arguments": normalized}, {"timeout_seconds": timeout}


def argv(cli, semantic):
    action, arguments = semantic["action"], semantic["arguments"]
    result = [cli, "--json", *ACTIONS[action]["command"], "--no-track"]
    for name, value in arguments.items():
        if name == "selected_vids":
            name, value = "extra_query", {"selected_vids": ",".join(map(str, value))}
        if name in {"with", "selected_metric_ids"}:
            value = ",".join(map(str, value))
        elif isinstance(value, (list, dict, bool)):
            value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        result += ["--" + name.replace("_", "-"), str(value)]
    return result


def response(payload, semantic):
    """Validate the transport/business envelope; never infer statistical significance."""
    if not isinstance(payload, dict) or payload.get("status") != "success":
        return None
    body = payload.get("data")
    if not isinstance(body, dict) or type(body.get("code")) is not int:
        return None
    if body["code"] not in {0, 200} or "data" not in body:
        return None
    data, action = body["data"], semantic["action"]
    if action == "experiment_get":
        if not isinstance(data, dict) or not isinstance(data.get("experiment"), dict):
            return None
        if data["experiment"].get("id") != semantic["arguments"]["experiment_id"]:
            return None
    elif action == "report_bundle":
        if type(data) is not int or data <= 0:
            return None
    elif action == "metric_search" or not isinstance(data, (dict, list)):
        return None
    return {"format": "json", "data_available": bool(data), "data": data}


CSV_COLUMNS = {
    "query",
    "result_type",
    "metric_group_id",
    "metric_group_name",
    "metric_id",
    "metric_name",
    "match_field",
    "match_score",
    "resolution_status",
}


def csv_response(text, semantic):
    # CLI distributions also wrap the CSV string in their normal JSON envelope.
    wrapped = False
    try:
        envelope = json.loads(text)
    except ValueError:
        pass
    else:
        if not isinstance(envelope, dict) or envelope.get("status") != "success":
            return None
        text = envelope.get("data")
        if not isinstance(text, str):
            return None
        wrapped = True
    if (not wrapped and not text.endswith("\n")) or "\ufffd" in text:
        return None
    try:
        reader = csv.DictReader(io.StringIO(text), strict=True)
        if set(reader.fieldnames or []) != CSV_COLUMNS:
            return None
        rows = list(reader)
        queries = semantic["arguments"]["metric_keys"]
        if not rows or any(set(r) != CSV_COLUMNS or None in r.values() for r in rows):
            return None
        if {r["query"] for r in rows} != set(queries):
            return None
        # Empty matches are valid query results, not evidence that an experiment has no data.
        return {
            "format": "csv",
            "data_available": any(r["metric_id"] for r in rows),
            "rows": rows,
            "row_count": len(rows),
        }
    except (csv.Error, ValueError):
        return None


def classify_failure(result, payload=None):
    text = json.dumps(payload, ensure_ascii=False) if payload else ""
    text += " " + result.get("stderr_tail", "") + " " + result.get("output_tail", "")
    text = text.lower()
    reason = result.get("reason_code")
    if re.search(r"\b401\b|unauthori[sz]ed|unauthenticated|token.expired|needs_auth", text):
        reason = "needs_auth"
    elif re.search(r"\b403\b|forbidden|access.denied|permission.denied", text):
        reason = "access_denied"
    elif result.get("exit_code") == 2 or "invalid value" in text or "badparameter" in text:
        reason = "invalid_arguments"
    elif re.search(r"\b429\b|rate.limit|too many requests", text):
        reason = "rate_limited"
    elif re.search(r"\b50[234]\b|connection.error|connection reset|timed? ?out|temporarily", text):
        reason = "transient_read_error"
    reason = reason or "invalid_response"
    result.update(
        reason_code=reason,
        retryable=reason in {"command_timeout", "rate_limited", "transient_read_error"},
    )
    return "failed"
