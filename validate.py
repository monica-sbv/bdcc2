"""P2 transaction validator. Run 'python validate.py --help' for the file contract."""
import argparse
import csv
import hashlib
import json
import os
import platform
import re
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

FIELDS = """transaction_id event_timestamp ingestion_timestamp customer_id card_id
issuer_bank_id acquirer_bank_id operation_cd payment_method_cd service_id merchant_id
terminal_id terminal_type_cd channel_cd terminal_country_cd card_country_cd currency_cd
amount_minor response_cd entry_mode_cd authentication_cd device_id related_transaction_id
source_system batch_id""".split()
BANKS = {f"B{i:03}" for i in range(1, 10)}
STAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
AMOUNT = re.compile(r"[0-9]+")
VERSION = "p2-1.0"


def valid_time(value):
    if not STAMP.fullmatch(value):
        return False
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def reason(row):
    # P2 exercise: which other identifier does the P1 contract require?
    if not row["transaction_id"].strip():
        return "missing_identifier"
    event, arrival = row["event_timestamp"], row["ingestion_timestamp"]
    if not valid_time(event) or not valid_time(arrival) or arrival < event:
        return "invalid_timestamp"
    digits = row["amount_minor"].lstrip("0")
    if (not AMOUNT.fullmatch(row["amount_minor"]) or not digits or len(digits) > 19
            or int(digits) > 9223372036854775807):
        return "invalid_amount"
    if row["issuer_bank_id"] not in BANKS or row["acquirer_bank_id"] not in BANKS:
        return "unknown_bank"
    return ""


def rows(path):
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source, strict=True)
        if reader.fieldnames != FIELDS:
            raise ValueError("expected the 25 P1 columns in their supplied order")
        for line, row in enumerate(reader, start=2):
            if None in row or None in row.values():
                raise ValueError(f"wrong number of fields in record {line}")
            yield line, row


def checksum(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def process(source, staging, run):
    started = datetime.now(timezone.utc)
    clock = time.monotonic()
    input_hash = checksum(source)
    # A replay can arrive earlier in the file. Select a valid winner before writing.
    winners = {}
    for line, row in rows(source):
        if not reason(row):
            key = row["transaction_id"]
            order = (row["ingestion_timestamp"], row["batch_id"], line)
            if key not in winners or order < winners[key]:
                winners[key] = order

    counts = dict(delivered=0, accepted=0, rejected=0, duplicates=0)
    reasons = Counter()
    from contextlib import ExitStack
    with ExitStack() as stack:
        writers = {}
        for name, extra in [("accepted", []), ("rejected", ["source_record", "reason"]),
                            ("duplicates", ["source_record", "kept_record"])]:
            stream = stack.enter_context((staging / f"{name}.csv").open("w", newline="", encoding="utf-8"))
            writers[name] = csv.DictWriter(stream, fieldnames=extra + FIELDS)
            writers[name].writeheader()
        for line, row in rows(source):
            counts["delivered"] += 1
            issue = reason(row)
            if issue:
                group = "rejected"
                row.update(source_record=line, reason=issue)
                reasons[issue] += 1
            elif winners[row["transaction_id"]][2] != line:
                group = "duplicates"
                row.update(source_record=line, kept_record=winners[row["transaction_id"]][2])
            else:
                group = "accepted"
            writers[group].writerow(row)
            counts[group] += 1

    if checksum(source) != input_hash:
        raise ValueError("input changed during the run")
    manifest = dict(status="complete", validator_version=VERSION, run=run,
                    input=str(source), input_sha256=input_hash, counts=counts,
                    rejection_reasons=dict(reasons), python=platform.python_version(),
                    platform=platform.machine(), image_id=os.getenv("IMAGE_ID", "unrecorded"),
                    program_sha256=checksum(Path(__file__)), start_utc=started.isoformat(),
                    elapsed_seconds=round(time.monotonic()-clock, 3),
                    output_sha256={p.name: checksum(p) for p in sorted(staging.glob("*.csv"))})
    (staging / "run_record.json").write_text(json.dumps(manifest, indent=2)+"\n", encoding="utf-8")
    return counts


def main():
    parser = argparse.ArgumentParser(description="Validate a P1 CSV into a new run folder.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True, help="existing writable parent")
    parser.add_argument("--run", required=True, help="new folder name within the output directory")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", args.run):
        parser.error("use letters, digits, dash or underscore for the run name")
    if not args.input.is_file():
        print(f"INPUT ERROR: file unavailable: {args.input}", file=sys.stderr)
        return 2
    target = args.output_dir / args.run
    try:
        if target.exists():
            raise FileExistsError(f"{target} already exists; choose a new run name")
        with tempfile.TemporaryDirectory(dir=args.output_dir) as tmp:
            staged = Path(tmp) / "results"
            staged.mkdir()
            counts = process(args.input, staged, args.run)
            staged.rename(target)
    except (ValueError, csv.Error) as error:
        print(f"INPUT ERROR: {error}", file=sys.stderr)
        return 2
    except OSError as error:
        if error.filename and Path(error.filename) == args.input:
            print(f"INPUT ERROR: {error}", file=sys.stderr)
            return 2
        print(f"OUTPUT ERROR: {error}", file=sys.stderr)
        return 3
    print(f"COMPLETE {args.run}: {json.dumps(counts)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
