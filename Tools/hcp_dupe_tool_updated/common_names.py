"""
common_names.py — Indian Common Name Detector for HCP Duplicate Detection.

Provides a curated dictionary of common Indian first names and surnames,
plus a classifier to determine whether an HCP name is "common" (high risk
of false-positive duplicate match) or "uncommon" (safe to auto-merge when
other signals like specialty + city align).

Usage:
    detector = IndianCommonNameDetector(cfg)
    result   = detector.classify("rajesh", "kumar")
    # → {"is_common": True, "first_common": True, "last_common": True,
    #    "label": "common", "reason": "Both 'rajesh' and 'kumar' are common Indian names."}
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("dupe_tool.common_names")

# ═══════════════════════════════════════════════════════════════════════
#  CURATED DICTIONARIES — sourced from Indian census frequency data
# ═══════════════════════════════════════════════════════════════════════

# ~200 common Indian first names (male + female), normalized lowercase
COMMON_FIRST_NAMES: frozenset[str] = frozenset({
    # ── Male (top frequency) ────────────────────────────────────────
    "rajesh", "suresh", "ramesh", "amit", "anil", "sanjay", "vijay",
    "ravi", "rakesh", "manoj", "ashok", "ajay", "raj", "rahul", "rohit",
    "sandeep", "deepak", "mukesh", "pankaj", "pradeep", "pramod", "vinod",
    "dinesh", "naresh", "mahesh", "ganesh", "satish", "girish", "harish",
    "umesh", "sunil", "arun", "varun", "kiran", "mohan", "rohan", "nitin",
    "sachin", "tushar", "gaurav", "vivek", "alok", "anand", "arvind",
    "ashish", "atul", "bharat", "dilip", "gopal", "hari", "hemant",
    "jagdish", "jitendra", "kamal", "kishore", "krishna", "lalit",
    "manish", "neeraj", "nikhil", "pawan", "prakash", "prasad",
    "rajendra", "rajiv", "rajan", "ravindra", "sagar", "sameer",
    "shankar", "shyam", "srinivas", "sudhir", "sushil", "tarun", "uday",
    "venkatesh", "vikram", "vinay", "yogesh", "ankur", "kapil", "harsh",
    "vishal", "kunal", "sumit", "mohit", "hitesh", "paresh", "bhavesh",
    "jignesh", "alpesh", "chirag", "darshan", "devendra", "dharmesh",
    "gautam", "govind", "jayesh", "lokesh", "naveen", "prem", "ramakrishna",
    "sanjiv", "shailesh", "shashank", "subhash", "surendra", "vikas",
    "balaji", "chandra", "dev", "dhananjay", "jatin", "karthik",
    "madhav", "nagesh", "narayan", "raghav", "siddharth", "srikanth",
    "venkat", "arjun", "pranav", "aditya", "abhishek", "akash",
    "mayank", "shubham", "vaibhav", "yash", "om", "sohan", "krishnan",
    "bhaskar", "murali", "sundar", "raghunath", "ramakant", "santosh",
    # ── Female (top frequency) ──────────────────────────────────────
    "priya", "anita", "sunita", "kavita", "rita", "neeta", "pooja",
    "divya", "swati", "anjali", "deepa", "geeta", "rekha", "seema",
    "neha", "shruti", "pallavi", "rashmi", "smita", "aarti", "archana",
    "jyoti", "lata", "meena", "nandini", "padma", "preeti", "radha",
    "renu", "shobha", "sita", "suman", "uma", "usha", "vandana",
    "vaishali", "priyanka", "payal", "nisha", "nikita", "namrata",
    "poonam", "ritu", "sapna", "manisha", "komal", "kajal", "madhuri",
    "kalpana", "kamala", "lakshmi", "sarita", "shanti", "pushpa",
    "savita", "saroj", "sushma", "asha", "bhavna", "chitra", "heena",
    "hema", "indira", "maya", "meenakshi", "manju", "mamta", "nirmala",
    "parvati", "sangeeta", "shilpa", "sneha", "sonia", "sudha", "vijaya",
    "yamuna", "aparna", "bhagyashree", "chhaya", "durga", "gita",
    "jaya", "kusum", "leela", "mohini", "naina", "prabha", "rajani",
    "rani", "rohini", "rukmini", "shakuntala", "sharda", "tara", "veena",
})

# ~120 common Indian last names / surnames, normalized lowercase
COMMON_LAST_NAMES: frozenset[str] = frozenset({
    # ── Pan-Indian / Hindi belt ─────────────────────────────────────
    "kumar", "singh", "sharma", "verma", "gupta", "jain", "yadav",
    "thakur", "chauhan", "agarwal", "agrawal", "saxena", "rastogi",
    "srivastava", "shukla", "tiwari", "trivedi", "pandey", "mishra",
    "dubey", "dwivedi", "bajpai", "khanna", "kapoor", "malhotra",
    "bhatia", "arora", "sethi", "chopra", "dhawan", "gill", "bajaj",
    "goyal", "mittal", "singhal", "garg", "nagpal", "tandon", "kaushik",
    "rawat", "negi", "bisht", "pathak", "upadhyay", "chaturvedi",
    "dixit", "awasthi", "nigam", "bhatnagar", "mathur",
    # ── South Indian ────────────────────────────────────────────────
    "rao", "reddy", "nair", "menon", "pillai", "iyer", "iyengar",
    "naidu", "raju", "shetty", "hegde", "patil", "kamat", "bhat",
    "kulkarni", "deshpande", "joshi", "phadke", "gokhale", "kelkar",
    "sathe", "pawar", "shinde", "jadhav", "deshmukh", "chavan",
    "more", "wagh", "gaikwad", "bhosale", "kamble",
    # ── East Indian ─────────────────────────────────────────────────
    "banerjee", "chatterjee", "mukherjee", "ghosh", "das", "sen",
    "bose", "roy", "dey", "sarkar", "majumdar", "dutta", "ganguly",
    "chakraborty", "bhattacharya", "kar", "mitra", "nag", "pal",
    "saha", "barua",
    # ── West Indian ─────────────────────────────────────────────────
    "patel", "shah", "mehta", "desai", "modi", "dave", "parikh",
    "bhatt", "vyas", "trivedi", "pandya", "raval", "sheth",
    # ── Sikh / Punjabi ──────────────────────────────────────────────
    "kaur", "sandhu", "sidhu", "dhillon", "grewal", "brar", "bajwa",
    "mann", "virk", "johal", "randhawa",
    # ── Other common ────────────────────────────────────────────────
    "choudhary", "chowdhury", "prasad", "rathore", "solanki",
    "rajput", "mehra", "vohra", "ahuja", "walia", "oberoi",
})


# ═══════════════════════════════════════════════════════════════════════
#  DETECTOR CLASS
# ═══════════════════════════════════════════════════════════════════════

class IndianCommonNameDetector:
    """Classify Indian HCP names as common or uncommon."""

    def __init__(self, cfg: dict[str, Any] | None = None):
        cfg = cfg or {}
        self.require_both = cfg.get("require_both_parts_common", True)
        self.first_names = COMMON_FIRST_NAMES
        self.last_names = COMMON_LAST_NAMES
        logger.info(
            "IndianCommonNameDetector ready — %d first names, %d last names, "
            "require_both=%s",
            len(self.first_names), len(self.last_names), self.require_both,
        )

    # ── Public API ──────────────────────────────────────────────────

    def is_common(self, first_name_norm: str, last_name_norm: str) -> bool:
        """
        Quick check: is this name combination 'common' in India?

        Args:
            first_name_norm: Normalized (lowercase, stripped) first name.
            last_name_norm:  Normalized (lowercase, stripped) last name.

        Returns:
            True if the name is common (risky for auto-merge), False otherwise.
        """
        fn = first_name_norm.strip().lower() if first_name_norm else ""
        ln = last_name_norm.strip().lower() if last_name_norm else ""

        first_hit = fn in self.first_names
        last_hit = ln in self.last_names

        if self.require_both:
            return first_hit and last_hit
        else:
            return first_hit or last_hit

    def classify(
        self, first_name_norm: str, last_name_norm: str
    ) -> dict[str, Any]:
        """
        Detailed classification with explanation.

        Returns:
            {
                "is_common": bool,
                "first_common": bool,
                "last_common": bool,
                "label": "common" | "uncommon",
                "reason": str   # human-readable sentence
            }
        """
        fn = first_name_norm.strip().lower() if first_name_norm else ""
        ln = last_name_norm.strip().lower() if last_name_norm else ""

        first_hit = fn in self.first_names
        last_hit = ln in self.last_names

        if self.require_both:
            is_common = first_hit and last_hit
        else:
            is_common = first_hit or last_hit

        # Build explanation
        if is_common:
            parts = []
            if first_hit:
                parts.append(f"'{fn}'")
            if last_hit:
                parts.append(f"'{ln}'")
            reason = (
                f"{' and '.join(parts)} {'are' if len(parts) > 1 else 'is'} "
                f"common Indian name{'s' if len(parts) > 1 else ''}."
            )
        else:
            uncommon_parts = []
            if not first_hit and fn:
                uncommon_parts.append(f"'{fn}'")
            if not last_hit and ln:
                uncommon_parts.append(f"'{ln}'")
            reason = (
                f"{' and '.join(uncommon_parts) or 'name'} "
                f"{'are' if len(uncommon_parts) > 1 else 'is'} uncommon, "
                f"reducing false-match risk."
            )

        return {
            "is_common": is_common,
            "first_common": first_hit,
            "last_common": last_hit,
            "label": "common" if is_common else "uncommon",
            "reason": reason,
        }
