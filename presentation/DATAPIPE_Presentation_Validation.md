# DATAPIPE Presentation Validation

## Deliverables checked

- 8-slide PowerPoint, 16:9, with speaker notes on every slide.
- Slide-wise DOCX and plain-text script generated from those notes.
- PowerShell recording guide with unique output/checkpoint paths.

## Script timing

The spoken narration contains **628 words**, counted from the eight PowerPoint speaker notes after excluding slide timing and screen cues. At a conversational 125 words per minute, the estimate is **5.02 minutes**. Actual delivery speed and pauses may shift the run time; rehearse with a timer. Slide time cues span 0:00 through 5:00.

## Project facts checked

- Project metadata and README identify distribution `datapipe-prisha`, import `datapipe`, CLI `datapipe`, and Python >=3.12.
- The release shown is `0.1.0` on TestPyPI. Prior fresh-environment smoke evidence showed the import path under the isolated environment's `site-packages`, and CLI version/help/config validation were successful.
- The YAML example reads CSV and JSONL, maps `amount` to `value`, deduplicates by `id`, and writes date-partitioned JSONL. README documents the output, dead-letter, report, and checkpoint mechanisms.
- README reports a verified two-source example summary of 7 fetched, 4 valid, 3 invalid, 1 duplicate removed, and 3 written. The recording guide recommends a new unique output path so existing demo files are preserved.
- README and `examples/plugin` document a separate entry-point plugin package. Prior integration evidence showed `sample` discovery and one plugin record written.
- Latest Windows test run: **147 passed, 2 skipped**. The explicit core branch checker measured **238/276 = 86.23% branch coverage**; pytest's combined statement-and-branch coverage was **91.41%**. The two POSIX signal subprocess tests were skipped on Windows; this presentation does not claim Linux signal validation.
- `BENCHMARKS.md` records the aggregation comparison: 50,000 rows, batch size 50,000, 15 interleaved repetitions, exact-Python median 0.417812 s and PyArrow median 0.135114 s, or 3.09×, with normalized outputs equal. The claim is limited to that in-memory workload. Other batch sizes and the end-to-end disk-backed pipeline did not meet 3×.
- `BENCHMARKS.md` records streaming deduplication RSS growth of 36,372,480 bytes for 2 million records and 36,438,016 bytes for 5 million records. These are RSS-growth measurements; they do not include total disk consumption, which increases with unique keys.

## Artifact validation

The PPTX finalizer reported 8 slides, 16:9 dimensions, zero package-integrity findings, zero layout findings, and successful Artifact Tool import. Its editable bar chart has a materialized embedded workbook; the workbook/chart relationships and numeric cache passed structural validation. All eight slide renders were inspected during authoring for visible clipping or overlap. Font-policy checking passed for Aptos and Arial, although the validator does not confirm identical native font rendering in PowerPoint.

The DOCX and TXT were generated from the final PPTX notes and use matching slide order, narration, timing, and screen cues.

## Not verified or limitations

- The user-mentioned `script.txt` was not present in the accessible project workspace or provided attachment. The new narration was written from the current implementation and checked documentation/evidence; no source-script content could be compared or retained.
- A native PowerPoint application was unavailable for opening the finished file. Package integrity/layout checks and Artifact Tool import passed, but there is no claim of a PowerPoint GUI open test. The DOCX render helper could not render because bundled LibreOffice (`soffice.exe`) is unavailable; the DOCX package and generated text were checked structurally, but page layout was not visually verified.
- The TestPyPI fresh-environment smoke test cited above is prior direct evidence from this project context and was not re-executed during this presentation task. The unique-path CSV/JSONL sample run and separately installed plugin discovery/execution were re-run while preparing these deliverables; both produced the expected records.
- Linux/POSIX signal tests remain unverified in this Windows environment.
- The 3.09× result is not a general pipeline speedup and does not cover disk-backed execution.
- The sample commands rely on PowerShell and local `.venv`/`.venv-demo` environments. The optional fresh-install rehearsal requires network access to PyPI and TestPyPI.
