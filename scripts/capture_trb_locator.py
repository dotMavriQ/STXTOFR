#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

from app.core.exceptions import ProviderFetchError
from app.providers.trb.adapter import TRBAdapter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Capture TRB store-locator data, save the raw widget payload, and emit "
            "normalised flat station records alongside the raw widget payload."
        )
    )
    parser.add_argument(
        "--output-dir",
        default="tmp/trb_capture",
        help="Directory where capture artifacts should be written.",
    )
    parser.add_argument(
        "--response-file",
        help=(
            "Optional file containing a previously saved widget response body. "
            "Useful when you copy the JSONP response from browser devtools."
        ),
    )
    parser.add_argument(
        "--uid",
        help="Optional widget UID override. If omitted, the script extracts it from the TRB station page.",
    )
    return parser


def capture_live(adapter: TRBAdapter) -> dict[str, Any]:
    page_response = adapter.http.get(
        adapter.STATION_PAGE_URL,
        headers=adapter._page_headers(),
    )
    uid = adapter._extract_widget_uid(page_response.text)
    request_url = adapter.WIDGET_JSON_URL_TEMPLATE.format(uid=uid)
    params = {"callback": "slw"}
    try:
        api_response = adapter.http.get(
            request_url,
            headers=adapter._widget_json_headers(),
            params=params,
            timeout=10,
        )
        payload = adapter._decode_widget_payload(api_response.text)
        records = adapter._extract_records(payload, uid=uid)
        return {
            "uid": uid,
            "token_present": None,
            "query": None,
            "request_url": request_url,
            "request_params": params,
            "request_headers": adapter._widget_json_headers(),
            "page_html": page_response.text,
            "widget_script": None,
            "raw_response_text": api_response.text,
            "decoded_payload": payload,
            "records": records,
            "errors": [],
        }
    except (ProviderFetchError, ValueError, KeyError) as exc:
        return {
            "uid": uid,
            "token_present": None,
            "request_params": params,
            "request_headers": adapter._widget_json_headers(),
            "page_html": page_response.text,
            "widget_script": None,
            "raw_response_text": None,
            "decoded_payload": None,
            "records": [],
            "errors": [{"query": None, "request_url": request_url, "error": str(exc)}],
        }


def capture_from_file(adapter: TRBAdapter, response_file: Path, uid: str | None) -> dict[str, Any]:
    raw_response_text = response_file.read_text(encoding="utf-8")
    payload = adapter._decode_widget_payload(raw_response_text)
    resolved_uid = uid or "manual-capture"
    records = adapter._extract_records(payload, uid=resolved_uid)
    return {
        "uid": resolved_uid,
        "token_present": None,
        "query": None,
        "request_url": None,
        "request_params": None,
        "request_headers": None,
        "page_html": None,
        "widget_script": None,
        "raw_response_text": raw_response_text,
        "decoded_payload": payload,
        "records": records,
        "errors": [],
    }


def to_flat_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flat_records: list[dict[str, Any]] = []
    for record in records:
        flat_records.append(
            {
                "dataset": "trb",
                "place": record.get("name"),
                "long": record.get("longitude"),
                "lat": record.get("latitude"),
                "address": record.get("address"),
                "city": record.get("city"),
                "county": record.get("region"),
                "zip": record.get("postal_code"),
                "country": "se",
                "phone": record.get("phone"),
                "hours": record.get("opening_hours"),
                "fuels": record.get("fuels") or [],
                "services": record.get("services") or [],
                "description": record.get("description"),
                "ttverified": False,
            }
        )
    return sorted(flat_records, key=lambda item: str(item.get("place") or ""))


def write_capture(output_dir: Path, capture: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    manifest = {
        "captured_at": timestamp,
        "uid": capture.get("uid"),
        "token_present": capture.get("token_present"),
        "query": capture.get("query"),
        "request_url": capture.get("request_url"),
        "request_params": capture.get("request_params"),
        "request_headers": capture.get("request_headers"),
        "record_count": len(capture.get("records") or []),
        "errors": capture.get("errors") or [],
    }
    (output_dir / f"{timestamp}_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if capture.get("page_html") is not None:
        (output_dir / f"{timestamp}_page.html").write_text(str(capture["page_html"]), encoding="utf-8")
    if capture.get("widget_script") is not None:
        (output_dir / f"{timestamp}_widget.js").write_text(str(capture["widget_script"]), encoding="utf-8")
    if capture.get("raw_response_text") is not None:
        (output_dir / f"{timestamp}_widget_response.txt").write_text(
            str(capture["raw_response_text"]),
            encoding="utf-8",
        )
    if capture.get("decoded_payload") is not None:
        (output_dir / f"{timestamp}_decoded_payload.json").write_text(
            json.dumps(capture["decoded_payload"], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    records = capture.get("records") or []
    (output_dir / f"{timestamp}_records.json").write_text(
        json.dumps(records, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / f"{timestamp}_flat_records.json").write_text(
        json.dumps(to_flat_records(records), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def print_summary(output_dir: Path, capture: dict[str, Any]) -> None:
    print(f"output_dir: {output_dir}")
    print(f"uid: {capture.get('uid')}")
    print(f"record_count: {len(capture.get('records') or [])}")
    if capture.get("errors"):
        print("errors:")
        for error in capture["errors"]:
            print(f"  - query={error.get('query')} error={error.get('error')}")
    if not capture.get("records"):
        print("next_step: save a successful widget JSONP response from browser devtools and rerun with --response-file")
        print(
            "rerun_command: "
            f"PYTHONPATH=. python scripts/capture_trb_locator.py --response-file /path/to/trb_widget_response.txt "
            f"--uid {capture.get('uid') or 'manual-capture'} --output-dir {output_dir}"
        )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    adapter = TRBAdapter()
    output_dir = Path(args.output_dir)

    if args.response_file:
        capture = capture_from_file(adapter, Path(args.response_file), uid=args.uid)
    else:
        capture = capture_live(adapter)

    write_capture(output_dir, capture)
    print_summary(output_dir, capture)


if __name__ == "__main__":
    main()
