# CLAUDE.md — Healthcare (Veeva OpenData)

## About
HCP/HCO duplicate detection tools for pharmaceutical clients (Sanofi, Novartis, Pfizer). All code must prioritize data integrity — director-level review depends on 100% accuracy.

## VID Handling — CRITICAL, NEVER VIOLATE
- Veeva VIDs (HCP, HCO, Address) are 18-digit numeric identifiers
- ALWAYS treat as strings at every stage: read, process, write
- NEVER cast to int, float, or any numeric type
- Read files with `dtype=str`; write Excel with `number_format='@'`
- Column patterns: `vid`, `*_vid`, `*_id` (18-digit), `veeva_id`, `opendata_id`

## Deduplication — Rule-Based ONLY
- NEVER use probabilistic tools (splink, dedupe) — they fail on Veeva data (<1% dup rate)
- Always use custom rule-based engine in `Tools/hcp_dupe_tool/`
- Dedup scope: parent HCO VIDs only — resolve dept records to parent VID first
- Dept detection: presence of `/` in HCO name = department record

## Locality Strip — CRITICAL BUG PREVENTION
- Veeva names follow `"HCO Name - Locality"` convention
- MUST split on ` - ` BEFORE calling `norm_text()`, not after
- Wrong: `norm_text(raw_name)` — bakes city into comparison string
- Right: `raw_name.split(" - ")[0].strip()` then `norm_text()`

## 3-Layer Audit + Independent Recheck — MANDATORY
- Layer 1: Source traceability — every number traces to a source file
- Layer 2: Computational verification — independently recompute all derived numbers with asserts
- Layer 3: Cross-reference integrity — numbers in multiple locations must match
- Independent recheck: reload raw files, re-derive all numbers via different method, compare
- NEVER present results with known failures

## Excel Output Standards
- Header colors: `1F4E79` (summary), `375623` (AUTO), `BF8F00` (REVIEW), `7B2C2C` (findings)
- VIDs formatted as text (`@`) in all Excel output
- Auto-fit columns, bold white headers, wrap text

## Standard Rules
- Always use `utf-8` encoding for file I/O
- Log every file read/write with row counts
- Print validation summary: total rows, unique VIDs, duplicates found
- Use `try/except` around file operations with meaningful error messages
