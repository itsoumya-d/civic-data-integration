# civic-data-integration

[![ci](https://github.com/itsoumya-d/civic-data-integration/actions/workflows/ci.yml/badge.svg)](https://github.com/itsoumya-d/civic-data-integration/actions/workflows/ci.yml)

A small, tested reference for the data flows behind council and utility
integrations:

- Map source asset-register fields into one canonical asset model.
- Validate asset identity, lifecycle status, coordinates, values and dates.
- Idempotent upserts keyed by source system and source record.
- Dead-letter queue with retry attempts for malformed records.
- Reconcile the canonical asset register against a finance ledger:
  missing rows, value mismatches and status mismatches.
- Audit every accepted, changed, rejected and reconciled record.
- FastAPI endpoints and a pytest suite.

This is a **reference implementation**, not a claim of production FME or
council-system experience. Live FME, GIS, asset-management and finance
connectors are out of scope; the mapping, validation, reconciliation and audit
core is tested.

## Run

```bash
uv venv --python 3.12
source .venv/bin/activate
pip install -e ".[dev]"
pytest
uvicorn civicintegrate.api:app --reload
```

## License

MIT
