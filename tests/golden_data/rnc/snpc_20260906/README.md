# CultivarWeb official capture — 2026-09-06

Public MAPA/SNPC forms and CSV records captured through the normal public HTTP session. Complete exports (5,424 protected records and 38,335 registered records) are kept outside the repository. No authentication credentials or personal cache were used.

HTML fixtures retain the original form attributes and relevant hidden/submit controls. Other page layout, selection options and result rows are omitted. Session token values are replaced by `REDACTED_CSRF_TOKEN`; cookies are absent. Protocol tests supply explicitly synthetic, changing token values.

CSV fixtures contain the original header and selected complete records: first/last records, absences, and examples needing CSV quoting. Cells are preserved by the standard-library CSV reader/writer. No official values were invented. Existing fixtures in the parent directory remain unchanged.

`expected.json` records original CSV cells and one-based source record numbers. The independent oracle uses explicit documented column meanings, string trimming and `datetime.strptime`, without importing agrobr or pandas. It verifies all cells of 10 protected and 10 registered sample records, including dates, absences and the source text indicating a pending definitive certificate. Non-date text retains the parser's existing missing-date policy; the literal source text remains in the CSV and oracle.

`PROVENANCE.json` preserves URLs, UTC timestamps, raw capture hashes, transformation descriptions and final fixture hashes. Full HTML captures redact tokens; their original hashes and redacted hashes are distinguished.
