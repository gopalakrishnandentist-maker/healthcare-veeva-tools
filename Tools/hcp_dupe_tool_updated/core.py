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


def strip_hco_name_affixes(
    name: str,
    prefixes: list[str],
    suffixes: list[str],
) -> str:
    """Remove known boilerplate prefixes and suffixes from an HCO name.

    Unlike strip_name_suffixes (single-token), this handles multi-word
    phrases like 'medical agency' or 'pvt ltd'.  Phrases must already be
    normalized (lowercase, space-separated).

    If stripping removes all tokens, the original name is returned unchanged.
    """
    if not name:
        return name

    original = name

    # Sort by length descending so longer phrases match first
    sorted_prefixes = sorted(prefixes, key=len, reverse=True)
    sorted_suffixes = sorted(suffixes, key=len, reverse=True)

    # Strip prefixes
    changed = True
    while changed:
        changed = False
        for pfx in sorted_prefixes:
            if name.startswith(pfx + " "):
                name = name[len(pfx):].strip()
                changed = True
                break
            elif name == pfx:
                name = ""
                changed = True
                break

    # Strip suffixes
    changed = True
    while changed:
        changed = False
        for sfx in sorted_suffixes:
            if name.endswith(" " + sfx):
                name = name[:-len(sfx)].strip()
                changed = True
                break
            elif name == sfx:
                name = ""
                changed = True
                break

    name = name.strip()
    return name if name else original


# ── Specialty Synonym Resolver ───────────────────────────────────────

class SpecialtySynonymResolver:
    """Resolves specialty names to canonical group names using a synonym map.

    Built from the specialty_synonyms config section. Each specialty string
    maps to a canonical group name so that e.g. "gp" and "general medicine"
    are treated as equivalent.
    """

    def __init__(self, synonym_groups: list[dict[str, Any]] | None = None) -> None:
        self._lookup: dict[str, str] = {}
        if not synonym_groups:
            return
        for entry in synonym_groups:
            group = entry.get("group", "")
            for name in entry.get("names", []):
                normed = norm_text(name)
                if normed:
                    self._lookup[normed] = group

    @property
    def enabled(self) -> bool:
        return bool(self._lookup)

    def resolve(self, specialty: str) -> str:
        """Return canonical group name if known, else the original string."""
        return self._lookup.get(specialty, specialty)

    def resolve_set(self, specialties: list[str] | set[str]) -> set[str]:
        """Resolve a set of specialties to their canonical group names."""
        return {self._lookup.get(s, s) for s in specialties if s}


# ── Name Similarity ──────────────────────────────────────────────────

def name_similarity(a: str, b: str) -> float:
    """Combined name similarity: max of token-sort and token-set ratios.

    Using the max of both catches cases where one name is a subset
    of the other (e.g., 'John Smith' vs 'John Michael Smith').
    """
    if not a or not b:
        return 0.0
    ts = _token_sort(a, b)
    # Short-circuit: if token_sort already >= 95, token_set won't beat it
    # meaningfully — skip the second expensive call.
    if ts >= 95.0:
        return ts
    return max(ts, _token_set(a, b))


# ── Acronym Detection ────────────────────────────────────────────────

_ACRONYM_STOPWORDS: frozenset[str] = frozenset({
    # Articles / prepositions
    "of", "and", "the", "a", "an", "for", "in", "at", "to", "by", "from", "&",
    # Honorifics — "dr" starts virtually every Indian clinic name so it's meaningless
    # as an acronym discriminator
    "dr", "prof", "professor", "mr", "mrs", "ms", "smt", "shri", "sri",
})


def compute_acronym(name_norm: str) -> str:
    """Compute acronym from a normalized name, skipping stop words and single chars.

    E.g., "ganesh siddha venkateswara medical college" → "gsvmc"
          "all india institute of medical sciences"    → "aiims"
    """
    words = [w for w in name_norm.split() if w not in _ACRONYM_STOPWORDS and len(w) > 1]
    return "".join(w[0] for w in words)


def is_acronym_of(short: str, long_norm: str) -> bool:
    """Return True if `short` is an acronym of meaningful words in `long_norm`.

    Rules:
      - `short` must be 2–8 lowercase alpha chars only
      - `short` must match the leading chars of compute_acronym(long_norm)

    Examples:
      is_acronym_of("gsv",   "ganesh siddha venkateswara medical college") → True
      is_acronym_of("aiims", "all india institute of medical sciences")    → True
      is_acronym_of("kem",   "king edward memorial hospital")              → True
    """
    if not short or not long_norm:
        return False
    if not re.match(r"^[a-z]{2,8}$", short):
        return False
    acronym = compute_acronym(long_norm)
    return len(acronym) >= len(short) and acronym.startswith(short)


# ── Name-similarity cache for large datasets ─────────────────────────

class NameSimilarityCache:
    """LRU-style cache for name_similarity to avoid recomputing for
    the same name pairs (common in large Indian datasets with
    many records sharing the same canonical name)."""

    def __init__(self, max_size: int = 200_000) -> None:
        self._cache: dict[tuple[str, str], float] = {}
        self._max_size = max_size

    def get(self, a: str, b: str) -> float:
        key = (a, b) if a <= b else (b, a)
        v = self._cache.get(key)
        if v is not None:
            return v
        v = name_similarity(a, b)
        if len(self._cache) < self._max_size:
            self._cache[key] = v
        return v


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
        spec_resolver: SpecialtySynonymResolver | None = None,
    ) -> None:
        self.max_block_size = max_block_size
        self.phonetic = phonetic
        self.first_initial = first_initial
        self.spec_resolver = spec_resolver
        self._blocks: dict[tuple, list[str]] = defaultdict(list)
        self._skipped_blocks = 0

    def add_hcp(self, vid: str, rec: dict[str, Any]) -> None:
        ln = rec.get("last_name_norm", "")
        fn = rec.get("first_name_norm", "")
        city = rec.get("city_cda_norm", "")
        ln_prefix = ln[:4] if ln else ""
        states = rec.get("states", [])

        # Last-name + city
        if ln and city:
            self._blocks[("ln_city", ln, city)].append(vid)

        # First-name + city (catches swapped first/last names)
        if fn and city and fn != ln:
            self._blocks[("ln_city", fn, city)].append(vid)

        # Last-name + state (broader recall — catches same-state, different-city)
        for st in states:
            if ln and st:
                self._blocks[("ln_state", ln, st)].append(vid)
            # Swapped: first-name + state
            if fn and st and fn != ln:
                self._blocks[("ln_state", fn, st)].append(vid)

        # First-initial + last-name (broader recall)
        if self.first_initial and fn and ln:
            self._blocks[("fi_ln", fn[0], ln)].append(vid)
            # Swapped: last-initial + first-name
            self._blocks[("fi_ln", ln[0], fn)].append(vid)

        # Phonetic: soundex(last) + city
        if self.phonetic and ln and city:
            self._blocks[("sdx_city", soundex(ln), city)].append(vid)
        # Swapped: soundex(first) + city
        if self.phonetic and fn and city and fn != ln:
            self._blocks[("sdx_city", soundex(fn), city)].append(vid)

        # Specialty + last name (resolve synonyms so GP and General Medicine share a block)
        for spec in rec.get("specialties", []):
            if spec and ln:
                resolved = self.spec_resolver.resolve(spec) if self.spec_resolver else spec
                self._blocks[("spec_ln", resolved, ln)].append(vid)
            # Swapped: specialty + first name
            if spec and fn and fn != ln:
                resolved = self.spec_resolver.resolve(spec) if self.spec_resolver else spec
                self._blocks[("spec_ln", resolved, fn)].append(vid)

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

        # Acronym blocking: catch "GSV Medical College" ↔ "Ganesh Siddha Venkateswara Medical College"
        # Strategy: both the abbreviation and the full name share the same 3-char acronym prefix key
        if city:
            first_token = name.split()[0] if name.split() else ""
            name_acronym = compute_acronym(name)
            # If this record's first token IS an abbreviation (2–6 alpha chars), index it directly
            if re.match(r"^[a-z]{2,6}$", first_token):
                self._blocks[("hco_acronym", first_token, city)].append(vid)
            # Also index the first 3 chars of this record's own acronym
            # so full-name records share a block with abbreviation records
            if len(name_acronym) >= 3:
                self._blocks[("hco_acronym", name_acronym[:3], city)].append(vid)

    def probe_hco(self, rec: dict[str, Any]) -> set[str]:
        """Find all VIDs in the index that share at least one HCO blocking key with `rec`.

        Does NOT add `rec` to the index. Used for cross-dataset lookups (Target vs Master).
        """
        candidates: set[str] = set()
        name = rec.get("name_norm", "")
        city = rec.get("city_norm", "")
        postal = rec.get("postal_norm", "")
        phones: list[str] = rec.get("phones", [])
        faxes: list[str] = rec.get("faxes", [])
        name_prefix = name[:6] if name else ""

        keys: list[tuple] = []
        if name and city:
            keys.append(("hco_name_city", name, city))
        if name_prefix and postal:
            keys.append(("hco_name_postal", name_prefix, postal))
        for p in phones:
            if p:
                keys.append(("hco_phone", p))
        for f in faxes:
            if f:
                keys.append(("hco_fax", f))
        if self.phonetic and name:
            first_word = name.split()[0] if name.split() else ""
            if first_word and city:
                keys.append(("hco_sdx_city", soundex(first_word), city))
        # Acronym probing
        if city:
            first_token = name.split()[0] if name.split() else ""
            name_acronym = compute_acronym(name)
            if re.match(r"^[a-z]{2,6}$", first_token):
                keys.append(("hco_acronym", first_token, city))
            if len(name_acronym) >= 3:
                keys.append(("hco_acronym", name_acronym[:3], city))

        for key in keys:
            if key in self._blocks:
                candidates.update(self._blocks[key])
        return candidates

    def probe_hcp(self, rec: dict[str, Any]) -> set[str]:
        """Find all VIDs that share at least one blocking key with `rec`.

        This does NOT add `rec` to the index — it only probes.
        Used for single-record lookups against a pre-built reference DB.
        """
        candidates: set[str] = set()
        ln = rec.get("last_name_norm", "")
        fn = rec.get("first_name_norm", "")
        city = rec.get("city_cda_norm", "")
        ln_prefix = ln[:4] if ln else ""

        keys_to_probe: list[tuple] = []
        if ln and city:
            keys_to_probe.append(("ln_city", ln, city))
        # Swapped: probe first-name as last-name + city
        if fn and city and fn != ln:
            keys_to_probe.append(("ln_city", fn, city))
        for st in rec.get("states", []):
            if ln and st:
                keys_to_probe.append(("ln_state", ln, st))
            if fn and st and fn != ln:
                keys_to_probe.append(("ln_state", fn, st))
        if self.first_initial and fn and ln:
            keys_to_probe.append(("fi_ln", fn[0], ln))
            # Swapped: last-initial + first-name
            keys_to_probe.append(("fi_ln", ln[0], fn))
        if self.phonetic and ln and city:
            keys_to_probe.append(("sdx_city", soundex(ln), city))
        if self.phonetic and fn and city and fn != ln:
            keys_to_probe.append(("sdx_city", soundex(fn), city))
        for spec in rec.get("specialties", []):
            if spec and ln:
                resolved = self.spec_resolver.resolve(spec) if self.spec_resolver else spec
                keys_to_probe.append(("spec_ln", resolved, ln))
            if spec and fn and fn != ln:
                resolved = self.spec_resolver.resolve(spec) if self.spec_resolver else spec
                keys_to_probe.append(("spec_ln", resolved, fn))
        for pin in rec.get("pins", []):
            if pin and ln_prefix:
                keys_to_probe.append(("pin_ln", pin, ln_prefix))
        for hco in rec.get("hco_vids", []):
            if hco:
                keys_to_probe.append(("hco", hco))
        for lnum, lbody in rec.get("active_licenses", []):
            if lnum and lbody:
                keys_to_probe.append(("lic", lbody, lnum))
        for e in rec.get("emails", []):
            if e:
                keys_to_probe.append(("email", e))
        for p in rec.get("phones", []):
            if p:
                keys_to_probe.append(("phone", p))

        for key in keys_to_probe:
            if key in self._blocks:
                for vid in self._blocks[key]:
                    candidates.add(vid)
        return candidates

    def candidate_pairs(self, max_pairs: int = 0) -> set[tuple[str, str]]:
        """Generate candidate pairs from blocking keys.

        Args:
            max_pairs: Global cap on total pairs (0 = unlimited).
                       Prevents runaway pair generation for very large datasets.
        """
        pairs: set[tuple[str, str]] = set()
        pair_cap_hit = False

        # Process blocks sorted by size (smallest first) so that
        # precise blocks (email, license, phone) are processed before
        # large imprecise blocks (last-name+city).
        sorted_blocks = sorted(self._blocks.items(), key=lambda x: len(x[1]))

        for key, vids in sorted_blocks:
            if len(vids) < 2:
                continue
            if max_pairs and len(pairs) >= max_pairs:
                pair_cap_hit = True
                break
            if len(vids) > self.max_block_size:
                self._skipped_blocks += 1
                logger.warning(
                    "Block %s has %d members (cap=%d) — truncating.",
                    key, len(vids), self.max_block_size,
                )
                vids = vids[: self.max_block_size]
            for i in range(len(vids)):
                for j in range(i + 1, len(vids)):
                    a, b = vids[i], vids[j]
                    if a > b:
                        a, b = b, a
                    pairs.add((a, b))
                    if max_pairs and len(pairs) >= max_pairs:
                        pair_cap_hit = True
                        break
                if pair_cap_hit:
                    break
            if pair_cap_hit:
                break

        if pair_cap_hit:
            logger.warning(
                "Global pair cap reached (%d). Some candidate pairs "
                "were not generated. Increase max_pairs or tighten blocking.",
                max_pairs,
            )

        logger.info(
            "Blocking produced %d candidate pairs from %d blocks "
            "(%d oversized, cap_hit=%s).",
            len(pairs), len(self._blocks), self._skipped_blocks, pair_cap_hit,
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


# ── Centralized HCO Column Mapping ──────────────────────────────────

# Canonical standard names (what the config/pipeline expects) → list of known aliases.
# Order matters: first match wins, so put most-specific aliases first.
HCO_COL_ALIASES: dict[str, list[str]] = {
    "hco_entity_vid": [
        "hco.vid__v (VID)", "hco.vid__v (NETWORK ID)", "hco.vid__v",
    ],
    "hco_name": [
        "hco.primary_name__v (NAME)",
        "hco.corporate_name__v (CORPORATE NAME)", "hco.corporate_name__v (CORPORATE_NAME)",
        "corporate_name__v)", "corporate_name__v",
        "Corporate Name",
    ],
    "hco_type": [
        "hco.hco_type__v (TYPE)",
        "hco.hco_type__v (HCO TYPE)", "hco.hco_type__v (HCO_TYPE)",
        "hco_type__v",
        "HCO Type",
    ],
    "hco_phone": [
        "hco.phone__v (PHONE)",
        "address.phone_1__v (PHONE 1)", "address.phone_1__v (PHONE)",
        "phone_1__v (1 )", "phone_1__v (1)", "phone_1__v",
        "Phone 1", "Phone",
    ],
    "hco_fax": [
        "hco.fax__v (FAX)",
        "address.fax_1__v (FAX 1)", "address.fax_1__v (FAX)",
        "fax_1__v (1 )", "fax_1__v (1)", "fax_1__v",
        "Fax 1", "Fax",
    ],
    "hco_city": [
        "hco.city__v (CITY)",
        "address.locality__v (CITY)",
        "locality__v",
        "City",
    ],
    "hco_state": [
        "hco.state__v (STATE)",
        "address.administrative_area__v (STATE/PROVINCE)", "address.administrative_area__v (STATE)",
        "administrative_area__v",
        "State/Province", "State",
    ],
    "hco_postal": [
        "hco.postal_code__v (POSTAL CODE)",
        "address.postal_code__v (ZIP/POSTAL CODE)", "address.postal_code__v (POSTAL CODE)",
        "address.postal_code__v (ZIP)", "postal_code__v",
        "ZIP/Postal Code", "Postal Code", "ZIP Code",
    ],
    "hco_addr_line1": [
        "hco.address_line_1__v (ADDRESS LINE 1)",
        "address.address_line_1__v (ADDRESS LINE 1)",
        "address_line_1__v",
        "Address Line 1",
    ],
    "hco_addr_line2": [
        "hco.address_line_2__v (ADDRESS LINE 2)",
        "address.address_line_2__v (ADDRESS LINE 2)",
        "address_line_2__v",
        "Address Line 2",
    ],
    "hco_status": [
        "hco.hco_status__v (STATUS)",
        "hco_status__v",
        "Status",
    ],
}

# Keyword-based fallback: when no exact/case-insensitive alias matches,
# try matching df columns by semantic keywords. Each key maps to a list
# of keyword sets — a column matches if ALL keywords in any set are present.
# More-specific sets should come first to avoid false positives.
_HCO_COL_KEYWORDS: dict[str, list[list[str]]] = {
    "hco_name":      [["corporate", "name"], ["primary", "name"], ["hco", "name"]],
    "hco_type":      [["hco", "type"]],
    "hco_phone":     [["phone"]],
    "hco_fax":       [["fax"]],
    "hco_city":      [["city"], ["locality"]],
    "hco_state":     [["state"], ["province"]],
    "hco_postal":    [["postal"], ["zip"]],
    "hco_addr_line1":[["address", "line", "1"], ["address", "line"]],
    "hco_addr_line2":[["address", "line", "2"]],
    "hco_status":    [["status"]],
}


def resolve_hco_column(
    df_columns: list[str],
    key: str,
    config_cols: dict[str, str],
    claimed: set[str] | None = None,
) -> str | None:
    """Resolve a logical HCO field name to an actual DataFrame column.

    Resolution order:
      1. Config-specified column name (exact match)
      2. Exact alias match from HCO_COL_ALIASES
      3. Case-insensitive alias match
      4. Keyword-based fuzzy match (lowest priority)

    Args:
        df_columns: list of column names in the DataFrame
        key: logical field key (e.g. "hco_name", "hco_city")
        config_cols: the cfg["columns"] dict
        claimed: set of df columns already mapped (to avoid double-mapping)

    Returns:
        The matched df column name, or None if no match found.
    """
    if claimed is None:
        claimed = set()
    col_set = set(df_columns)

    # 1. Config primary
    primary = config_cols.get(key, "")
    if primary and primary in col_set and primary not in claimed:
        return primary

    aliases = HCO_COL_ALIASES.get(key, [])

    # 2. Exact alias match
    for alias in aliases:
        if alias in col_set and alias not in claimed:
            return alias

    # 3. Case-insensitive alias match
    lower_map = {c.lower(): c for c in df_columns if c not in claimed}
    for alias in aliases:
        matched = lower_map.get(alias.lower())
        if matched:
            return matched

    # 4. Keyword-based fuzzy match
    keyword_sets = _HCO_COL_KEYWORDS.get(key, [])
    for kw_set in keyword_sets:
        for col in df_columns:
            if col in claimed:
                continue
            col_lower = col.lower()
            if all(kw in col_lower for kw in kw_set):
                logger.info("HCO keyword match: '%s' → '%s' (keywords: %s)", key, col, kw_set)
                return col

    return None


def remap_hco_columns(
    df: pd.DataFrame,
    cfg: dict[str, Any],
) -> tuple[pd.DataFrame, list[str]]:
    """Remap HCO DataFrame columns to standard names expected by the pipeline.

    Uses the centralized alias + keyword resolution. Returns a copy of the
    DataFrame with columns renamed, plus a list of human-readable mapping
    descriptions (for UI display).

    Skips remapping for columns that already have the standard name.
    """
    cols = cfg.get("columns", {})
    rename_map: dict[str, str] = {}
    descriptions: list[str] = []
    claimed: set[str] = set()

    # Map each logical field to its standard config column name
    _FIELD_TO_STANDARD = {
        "hco_entity_vid": cols.get("hco_entity_vid", "hco.vid__v (VID)"),
        "hco_name":       cols.get("hco_name", "hco.primary_name__v (NAME)"),
        "hco_type":       cols.get("hco_type", "hco.hco_type__v (TYPE)"),
        "hco_phone":      cols.get("hco_phone", "hco.phone__v (PHONE)"),
        "hco_fax":        cols.get("hco_fax", "hco.fax__v (FAX)"),
        "hco_city":       cols.get("hco_city", "hco.city__v (CITY)"),
        "hco_state":      cols.get("hco_state", "hco.state__v (STATE)"),
        "hco_postal":     cols.get("hco_postal", "hco.postal_code__v (POSTAL CODE)"),
        "hco_addr_line1": cols.get("hco_addr_line1", "hco.address_line_1__v (ADDRESS LINE 1)"),
        "hco_addr_line2": cols.get("hco_addr_line2", "hco.address_line_2__v (ADDRESS LINE 2)"),
        "hco_status":     cols.get("hco_status", "hco.hco_status__v (STATUS)"),
    }

    for field_key, standard_name in _FIELD_TO_STANDARD.items():
        # If the standard name is already in the df, no remapping needed
        if standard_name in df.columns:
            claimed.add(standard_name)
            continue

        matched = resolve_hco_column(
            list(df.columns), field_key, cols, claimed,
        )
        if matched and matched != standard_name:
            rename_map[matched] = standard_name
            claimed.add(matched)
            descriptions.append(f"`{matched}` → `{standard_name}`")

    if rename_map:
        df = df.rename(columns=rename_map)
        logger.info("HCO column remap: %s", rename_map)

    return df, descriptions
