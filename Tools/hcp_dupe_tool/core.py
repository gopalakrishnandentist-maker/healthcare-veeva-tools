"""
core.py — Normalizers, similarity functions, blocking, and data structures.

This module contains all the low-level building blocks shared between
the HCP and HCO duplicate-detection pipelines.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, FrozenSet, Optional, Set, Tuple

import pandas as pd

logger = logging.getLogger("dupe_tool")

# ── Optional fast fuzzy library ──────────────────────────────────────
try:
    from rapidfuzz import fuzz as _rf

    def _token_sort(a: str, b: str) -> float:
        return _rf.token_sort_ratio(a, b)

    def _token_set(a: str, b: str) -> float:
        return _rf.token_set_ratio(a, b)

except ImportError:
    import difflib

    def _token_sort(a: str, b: str) -> float:
        return 100.0 * difflib.SequenceMatcher(None, a, b).ratio()

    def _token_set(a: str, b: str) -> float:
        # Approximate token-set via sorted tokens
        sa = " ".join(sorted(a.split()))
        sb = " ".join(sorted(b.split()))
        return 100.0 * difflib.SequenceMatcher(None, sa, sb).ratio()


# ── Soundex (simple implementation — no external dep) ────────────────
_SOUNDEX_MAP = {
    c: d
    for d, chars in enumerate(
        ["bfpv", "cgjkqsxz", "dt", "l", "mn", "r"], start=1
    )
    for c in chars
}


def soundex(name: str) -> str:
    """American Soundex code (4 chars) for a single word."""
    name = re.sub(r"[^a-z]", "", name.lower())
    if not name:
        return "Z000"
    code = [name[0].upper()]
    prev = _SOUNDEX_MAP.get(name[0], 0)
    for ch in name[1:]:
        digit = _SOUNDEX_MAP.get(ch, 0)
        if digit and digit != prev:
            code.append(str(digit))
        prev = digit if digit else prev
        if len(code) == 4:
            break
    return "".join(code).ljust(4, "0")


# ── Text Normalization ───────────────────────────────────────────────

def norm_text(x: Any) -> str:
    """Generic text normalizer: lowercase, strip accents, collapse whitespace."""
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_email(x: Any) -> Optional[str]:
    """Normalize an email address; return None if invalid."""
    if pd.isna(x):
        return None
    s = str(x).strip().lower().replace(" ", "")
    if "@" not in s:
        return None
    s = re.sub(r"^mailto:", "", s)
    return s if len(s) >= 6 else None


def norm_phone(x: Any) -> Optional[str]:
    """Extract last 10 digits from a phone string; return None if < 7 digits."""
    if pd.isna(x):
        return None
    digits = re.sub(r"\D", "", str(x))
    if len(digits) < 7:
        return None
    if len(digits) > 10:
        digits = digits[-10:]
    return digits


def norm_license(x: Any) -> Optional[str]:
    """Uppercase, strip separators."""
    if pd.isna(x):
        return None
    s = str(x).strip().upper()
    s = re.sub(r"[\s\-_/\.]", "", s)
    return s or None


def strip_name_suffixes(name: str, suffixes: list[str]) -> str:
    """Remove trailing suffixes/titles from a name string.

    E.g., 'John Smith MD Jr' → 'John Smith' given suffixes=['md','jr'].
    """
    tokens = name.split()
    while tokens and tokens[-1] in suffixes:
        tokens.pop()
    # Also check leading titles
    while tokens and tokens[0] in suffixes:
        tokens.pop(0)
    return " ".join(tokens)


# ── Name Similarity ──────────────────────────────────────────────────

def name_similarity(a: str, b: str) -> float:
    """Combined name similarity: max of token-sort and token-set ratios.

    Using the max of both catches cases where one name is a subset
    of the other (e.g., 'John Smith' vs 'John Michael Smith').
    """
    if not a or not b:
        return 0.0
    return max(_token_sort(a, b), _token_set(a, b))


# ── Aggregation helpers ──────────────────────────────────────────────

def agg_set(series: pd.Series, norm_fn: Callable) -> Set[str]:
    """Apply a normalizer to a Series and collect non-None results."""
    out: set[str] = set()
    for v in series:
        nv = norm_fn(v)
        if nv:
            out.add(nv)
    return out


# ── Disjoint Set Union (for clustering) ─────────────────────────────

class DSU:
    """Union-Find / Disjoint-Set-Union with path compression + union by rank."""

    def __init__(self) -> None:
        self.parent: dict[str, str] = {}
        self.rank: dict[str, int] = {}

    def find(self, x: str) -> str:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


# ── Shared Contact Detector ─────────────────────────────────────────

class SharedContactDetector:
    """Identifies phone/email values used by many VIDs (e.g., hospital switchboard)."""

    def __init__(self, threshold: int = 5) -> None:
        self.threshold = threshold
        self.phone_freq: Counter = Counter()
        self.email_freq: Counter = Counter()
        self._shared_phones: set[str] = set()
        self._shared_emails: set[str] = set()

    def feed(self, phones: set[str], emails: set[str]) -> None:
        for p in phones:
            self.phone_freq[p] += 1
        for e in emails:
            self.email_freq[e] += 1

    def finalize(self) -> None:
        self._shared_phones = {p for p, c in self.phone_freq.items() if c >= self.threshold}
        self._shared_emails = {e for e, c in self.email_freq.items() if c >= self.threshold}
        logger.info(
            "Shared contacts: %d phones, %d emails (threshold=%d)",
            len(self._shared_phones),
            len(self._shared_emails),
            self.threshold,
        )

    def is_shared(self, phones: set[str], emails: set[str]) -> bool:
        return bool(phones & self._shared_phones) or bool(emails & self._shared_emails)

    def shared_rows(self) -> list[tuple[str, str, int]]:
        rows = []
        for p, c in self.phone_freq.items():
            if c >= self.threshold:
                rows.append(("phone", p, c))
        for e, c in self.email_freq.items():
            if c >= self.threshold:
                rows.append(("email", e, c))
        return sorted(rows, key=lambda r: -r[2])


# ── Blocking Engine ──────────────────────────────────────────────────

class BlockingEngine:
    """Generates candidate pairs from multiple blocking keys.

    Supports:
      - last-name + city
      - first-initial + last-name
      - soundex(last-name) + city
      - specialty + last-name
      - postal-code + last-name-prefix
      - HCO VID
      - license (body + number)
      - email exact
      - phone exact
    """

    def __init__(
        self,
        max_block_size: int = 500,
        phonetic: bool = True,
        first_initial: bool = True,
    ) -> None:
        self.max_block_size = max_block_size
        self.phonetic = phonetic
        self.first_initial = first_initial
        self._blocks: dict[tuple, list[str]] = defaultdict(list)
        self._skipped_blocks = 0

    def add_hcp(self, vid: str, rec: dict[str, Any]) -> None:
        ln = rec.get("last_name_norm", "")
        fn = rec.get("first_name_norm", "")
        city = rec.get("city_cda_norm", "")
        ln_prefix = ln[:4] if ln else ""

        # Last-name + city
        if ln and city:
            self._blocks[("ln_city", ln, city)].append(vid)

        # First-initial + last-name (broader recall)
        if self.first_initial and fn and ln:
            self._blocks[("fi_ln", fn[0], ln)].append(vid)

        # Phonetic: soundex(last) + city
        if self.phonetic and ln and city:
            self._blocks[("sdx_city", soundex(ln), city)].append(vid)

        # Specialty + last name
        for spec in rec.get("specialties", []):
            if spec and ln:
                self._blocks[("spec_ln", spec, ln)].append(vid)

        # Postal code + last-name prefix
        for pin in rec.get("pins", []):
            if pin and ln_prefix:
                self._blocks[("pin_ln", pin, ln_prefix)].append(vid)

        # HCO VID
        for hco in rec.get("hco_vids", []):
            if hco:
                self._blocks[("hco", hco)].append(vid)

        # License (active)
        for lnum, lbody in rec.get("active_licenses", []):
            if lnum and lbody:
                self._blocks[("lic", lbody, lnum)].append(vid)

        # Contact exact
        for e in rec.get("emails", []):
            if e:
                self._blocks[("email", e)].append(vid)
        for p in rec.get("phones", []):
            if p:
                self._blocks[("phone", p)].append(vid)

    def add_hco(self, vid: str, rec: dict[str, Any]) -> None:
        """Add an HCO record's blocking keys."""
        name = rec.get("name_norm", "")
        city = rec.get("city_norm", "")
        postal = rec.get("postal_norm", "")
        phone = rec.get("phone_norm", "")
        fax = rec.get("fax_norm", "")
        name_prefix = name[:6] if name else ""

        if name and city:
            self._blocks[("hco_name_city", name, city)].append(vid)
        if name_prefix and postal:
            self._blocks[("hco_name_postal", name_prefix, postal)].append(vid)
        if phone:
            self._blocks[("hco_phone", phone)].append(vid)
        if fax:
            self._blocks[("hco_fax", fax)].append(vid)
        if self.phonetic and name:
            first_word = name.split()[0] if name.split() else ""
            if first_word and city:
                self._blocks[("hco_sdx_city", soundex(first_word), city)].append(vid)

    def candidate_pairs(self) -> set[tuple[str, str]]:
        pairs: set[tuple[str, str]] = set()
        for key, vids in self._blocks.items():
            if len(vids) < 2:
                continue
            if len(vids) > self.max_block_size:
                self._skipped_blocks += 1
                logger.warning(
                    "Block %s has %d members (cap=%d) — truncating. "
                    "Consider tighter blocking or raising max_block_size.",
                    key, len(vids), self.max_block_size,
                )
                vids = vids[: self.max_block_size]
            for i in range(len(vids)):
                for j in range(i + 1, len(vids)):
                    a, b = vids[i], vids[j]
                    if a > b:
                        a, b = b, a
                    pairs.add((a, b))
        logger.info(
            "Blocking produced %d candidate pairs from %d blocks (%d oversized).",
            len(pairs), len(self._blocks), self._skipped_blocks,
        )
        return pairs


# ── Memory utilities ────────────────────────────────────────────────

def estimate_dataframe_memory(df: pd.DataFrame) -> float:
    """Estimate DataFrame memory usage in MB.

    Args:
        df: pandas DataFrame to measure

    Returns:
        Estimated memory usage in megabytes
    """
    bytes_used = df.memory_usage(deep=True).sum()
    return bytes_used / (1024 * 1024)
