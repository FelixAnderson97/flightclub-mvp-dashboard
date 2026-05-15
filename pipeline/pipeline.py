"""
HGEM MVP Dashboard — Data Pipeline

SUMMARY
-------
Reads every Hub_Data_Export*.csv in the data folder (and at project root,
where new drops land), parses guest feedback, extracts employee mentions,
scores sentiment, applies all merging/splitting/override rules from
name_config.py, and emits:

  Outputs/dashboard.html         — single-file interactive dashboard
  Outputs/dashboard_data.json    — processed data (used for debug + anomaly diff)
  Outputs/snapshot.json          — same as above, kept as "previous" snapshot
  Outputs/changelog.md           — running history of each pipeline run
  Outputs/anomalies.md           — anomalies surfaced this run (new venues etc.)

Usage:
  python pipeline.py [--data-dir PATH] [--out-dir PATH]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict, Counter
from datetime import datetime, date
from pathlib import Path

import pandas as pd

from name_config import (
    NAME_GROUPS, NEVER_SPLIT, FORCE_SPLIT, PRIMARY_VENUE_OVERRIDE,
    MISATTRIBUTED_MENTIONS, NON_NAMES, POSITIVE_WORDS, NEGATIVE_WORDS,
    COMPLAINT_SIGNALS, NEGATION_WORDS,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# 4-4-5 hospitality calendar
PERIODS = [
    {"id": "P1", "start": date(2025, 12, 29), "end": date(2026, 1, 25)},
    {"id": "P2", "start": date(2026, 1, 26),  "end": date(2026, 2, 22)},
    {"id": "P3", "start": date(2026, 2, 23),  "end": date(2026, 3, 29)},
    {"id": "P4", "start": date(2026, 3, 30),  "end": date(2026, 4, 26)},
    {"id": "P5", "start": date(2026, 4, 27),  "end": date(2026, 5, 24)},
    {"id": "P6", "start": date(2026, 5, 25),  "end": date(2026, 6, 21)},  # placeholder for next period
]

QUARTERS = {
    "Q1": ["P1", "P2", "P3"],
    "Q2": ["P4", "P5", "P6"],
}

# Auto-split threshold: if a name has >= this many mentions at each of 2+ venues,
# treat as different people and split by venue.
AUTO_SPLIT_THRESHOLD = 3

# Name extraction: words starting with capital, 2+ chars, after a non-letter or
# at sentence start. We then filter via NON_NAMES.
NAME_RE = re.compile(r"\b([A-Z][a-z]{1,15})\b")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def canonicalise(name: str) -> str:
    """Apply NAME_GROUPS merge: 'jaz' -> 'Jas', 'Holly' -> 'Hollie', etc."""
    return NAME_GROUPS.get(name.lower(), name)


def parse_date(s: str) -> date | None:
    if pd.isna(s):
        return None
    try:
        return datetime.strptime(str(s).split(" ")[0], "%Y-%m-%d").date()
    except ValueError:
        try:
            return datetime.fromisoformat(str(s)).date()
        except ValueError:
            return None


def assign_period(d: date | None) -> str | None:
    if d is None:
        return None
    for p in PERIODS:
        if p["start"] <= d <= p["end"]:
            return p["id"]
    return None


def split_sentences(text: str) -> list[str]:
    """Lightweight sentence splitter — splits on .!?; plus newlines."""
    if not text:
        return []
    parts = re.split(r"(?<=[.!?])\s+|[\n\r]+", text)
    return [p.strip() for p in parts if p and p.strip()]


def score_sentence_sentiment(sentence: str) -> str:
    """
    Returns 'positive', 'neutral', or 'negative'.

    Logic:
      - Tokenize, look for positive/negative words
      - Check for negation immediately before positive words
      - If complaint signal present and no strong positive, downgrade to neutral
      - Default to positive (most named mentions are praise)
    """
    if not sentence:
        return "neutral"
    s = sentence.lower()
    tokens = re.findall(r"[a-z']+", s)

    # Negation-aware positive count
    pos = 0
    for i, t in enumerate(tokens):
        if t in POSITIVE_WORDS:
            # check window of 3 tokens back for negation
            window = tokens[max(0, i - 3):i]
            if any(w in NEGATION_WORDS for w in window):
                pos -= 1
            else:
                pos += 1

    neg = sum(1 for t in tokens if t in NEGATIVE_WORDS)
    has_complaint_signal = any(sig in s for sig in COMPLAINT_SIGNALS)

    if neg > pos:
        return "negative"
    if pos > 0:
        return "positive"
    if has_complaint_signal:
        return "neutral"
    return "positive"  # conservative default: bare names get treated as praise


# ---------------------------------------------------------------------------
# 1. Load and dedupe CSV data
# ---------------------------------------------------------------------------

def load_all_csvs(data_dir: Path, root_dir: Path) -> pd.DataFrame:
    """Load every Hub_Data_Export*.csv from data_dir and root_dir, concat, dedupe."""
    files = sorted(set(
        list(data_dir.glob("Hub_Data_Export*.csv"))
        + list(root_dir.glob("Hub_Data_Export*.csv"))
    ))
    if not files:
        raise SystemExit(f"No Hub_Data_Export*.csv found in {data_dir} or {root_dir}")

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, low_memory=False)
        except Exception as e:
            print(f"  WARN: could not read {f.name}: {e}")
            continue
        df["_source_file"] = f.name
        frames.append(df)
        print(f"  loaded {f.name}: {len(df)} rows, {df['Visit Id'].nunique()} visits")

    all_rows = pd.concat(frames, ignore_index=True)

    # Standardise — some files don't have Area/Visit Summary
    for col in ("Area", "Visit Summary", "Visit Type", "Visit Source",
                "Feedback Category", "Region", "Answer"):
        if col not in all_rows.columns:
            all_rows[col] = ""

    # Dedupe: same Visit Id + same Comments + same Answer text is a duplicate row
    before = len(all_rows)
    all_rows["Comments"] = all_rows["Comments"].fillna("")
    all_rows["Answer"] = all_rows["Answer"].fillna("")
    all_rows = all_rows.drop_duplicates(
        subset=["Visit Id", "Comments", "Answer"]
    ).reset_index(drop=True)
    print(f"  dedupe: {before} -> {len(all_rows)} rows")

    return all_rows


# ---------------------------------------------------------------------------
# 2. Build per-visit corpus
# ---------------------------------------------------------------------------

def build_visit_corpus(rows: pd.DataFrame) -> pd.DataFrame:
    """One row per Visit Id, with all comments concatenated into a single corpus."""
    def join_text(series):
        seen = set()
        out = []
        for t in series:
            if pd.isna(t):
                continue
            t = str(t).strip()
            if t and t not in seen:
                seen.add(t)
                out.append(t)
        return " ¶ ".join(out)

    grouped = rows.groupby("Visit Id", as_index=False).agg({
        "Branch": "first",
        "Visit Date Time": "first",
        "Visit Score": "first",
        "Recommend Score": "first",
        "Visit Summary": "first",
        "Comments": join_text,
    })
    grouped["VisitDate"] = grouped["Visit Date Time"].apply(parse_date)
    grouped["Period"] = grouped["VisitDate"].apply(assign_period)
    grouped["Corpus"] = (
        grouped["Visit Summary"].fillna("").astype(str) + " ¶ " +
        grouped["Comments"].fillna("").astype(str)
    )
    return grouped


# ---------------------------------------------------------------------------
# 3. Extract employee mentions per visit
# ---------------------------------------------------------------------------

# Suffixes that are almost never present in first names — rule-based filter
_NAME_SUFFIX_BLOCKLIST = (
    "ly",       # adverbs (Amazingly, Approachable -> -le)
    "ed",       # past-tense verbs (Accepted, Asked, Walked)
    "ing",      # -ing forms (Asking, Allowing, Amazing)
    "tion",     # nouns (Attention, Description)
    "ness",     # nouns (Kindness, Happiness)
    "ment",     # nouns (Movement, Settlement)
    "able",     # adjectives (Approachable, Reliable)
    "ible",     # adjectives (Possible)
    "ous",      # adjectives (Generous, Famous)
    "ive",      # adjectives (Attentive, Active) — note: a few names end -ive though
    "est",      # superlatives (Greatest, Easiest)
)

# Specific short names that DO end in blocked suffixes — whitelist
_SUFFIX_WHITELIST = {
    "Ed", "Ned", "Ted", "Fred", "Jed",                    # -ed names
    "Greg", "Reg", "Meg", "Peg",                          # short -eg
    "Eve", "Liv", "Olive",                                # -ive names
    "May", "Kay", "Ray", "Fay",                           # -ay (not blocked but FYI)
    "Tess", "Bess", "Jess",                               # -ess
    "Wes",
}

def _looks_like_non_name(word: str) -> bool:
    """Heuristic: words ending in common English derivational suffixes."""
    if word in _SUFFIX_WHITELIST:
        return False
    if len(word) < 5:
        return False  # short words don't get suffix-blocked
    lw = word.lower()
    return any(lw.endswith(suf) for suf in _NAME_SUFFIX_BLOCKLIST)


def extract_candidate_names(text: str, lc_ratio_map: dict | None = None,
                            lc_ratio_threshold: float = 0.5) -> list[str]:
    """Pull capitalized words that survive NON_NAMES, suffix, and lc-ratio filters."""
    candidates = NAME_RE.findall(text or "")
    out = []
    for c in candidates:
        if c in NON_NAMES:
            continue
        if _looks_like_non_name(c):
            continue
        if lc_ratio_map and lc_ratio_map.get(c, 0.0) >= lc_ratio_threshold:
            # Word appears in lowercase form too often to be a real name
            continue
        out.append(c)
    return out


def build_lc_ratio_map(rows: pd.DataFrame) -> dict:
    """
    Pre-compute lowercase-vs-capitalized ratio for every candidate word in the
    corpus. Real names appear almost always capitalized (lc_ratio < 0.2). Common
    English words appear mostly lowercase (lc_ratio > 0.5).
    """
    from collections import Counter
    counts = Counter()
    text_cols = [c for c in ("Comments", "Visit Summary", "Answer") if c in rows.columns]
    for col in text_cols:
        for txt in rows[col].dropna().astype(str):
            for m in re.finditer(r"\b[A-Za-z]{2,15}\b", txt):
                counts[m.group(0)] += 1

    ratio = {}
    for w, c in counts.items():
        if w[0].isupper() and w[1:].islower():
            lc = counts.get(w.lower(), 0)
            total = c + lc
            if total >= 3:
                ratio[w] = lc / total
    return ratio


def extract_mentions(visits: pd.DataFrame, lc_ratio_map: dict | None = None) -> pd.DataFrame:
    """
    For each visit, find every employee mentioned, score the sentence(s)
    containing the name, and emit one row per (visit, employee) mention.
    """
    records = []
    for _, v in visits.iterrows():
        text = v["Corpus"]
        if not text:
            continue
        candidates = set(extract_candidate_names(text, lc_ratio_map))
        if not candidates:
            continue

        sentences = split_sentences(text)

        for raw_name in candidates:
            canonical = canonicalise(raw_name)
            # skip if NON_NAMES kicks in post-canonicalisation
            if canonical in NON_NAMES:
                continue

            # Skip misattributed mentions (e.g. customer named same as employee)
            if (str(v["Visit Id"]), canonical) in MISATTRIBUTED_MENTIONS:
                continue

            # Find sentences containing the name (case-insensitive)
            name_sentences = [
                s for s in sentences
                if re.search(rf"\b{re.escape(raw_name)}\b", s, re.IGNORECASE)
            ]
            if not name_sentences:
                continue

            # Score the worst sentence — one bad signal beats neutral background
            worst = "positive"
            for s in name_sentences:
                sent = score_sentence_sentiment(s)
                if sent == "negative":
                    worst = "negative"
                    break
                if sent == "neutral" and worst == "positive":
                    worst = "neutral"

            # Choose the longest/richest sentence as the quote
            quote = max(name_sentences, key=len) if name_sentences else ""

            records.append({
                "visit_id":   str(v["Visit Id"]),
                "branch":     v["Branch"],
                "date":       v["VisitDate"].isoformat() if v["VisitDate"] else None,
                "period":     v["Period"],
                "name":       canonical,
                "sentiment":  worst,
                "quote":      quote.strip(),
            })

    df = pd.DataFrame(records)
    # Dedupe (visit_id, name): each visit contributes max one mention per employee.
    # Prefer richer (longer) quote on collisions.
    if not df.empty:
        df["_qlen"] = df["quote"].str.len().fillna(0)
        # If we ever get multiple sentiments for the same (visit, name), keep the worst
        sentiment_order = {"negative": 0, "neutral": 1, "positive": 2}
        df["_sord"] = df["sentiment"].map(sentiment_order)
        df = df.sort_values(["_sord", "_qlen"], ascending=[True, False])
        df = df.drop_duplicates(subset=["visit_id", "name"]).reset_index(drop=True)
        df = df.drop(columns=["_sord", "_qlen"])
    return df


# ---------------------------------------------------------------------------
# 4. Apply venue-based auto-splitter (with NEVER_SPLIT / FORCE_SPLIT overrides)
# ---------------------------------------------------------------------------

def apply_venue_splits(mentions: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    For each name, decide whether it represents one person or several people
    by venue. Mutates mention `name` to 'Name · Venue' for split names.
    Returns (mentions_with_split_names, split_info_dict_for_logging).
    """
    if mentions.empty:
        return mentions, {}

    # Count positive mentions per (name, venue)
    pos = mentions[mentions["sentiment"] == "positive"]
    venue_counts = pos.groupby(["name", "branch"]).size().unstack(fill_value=0)

    split_info = {}  # name -> list of venues to split into
    for name, row in venue_counts.iterrows():
        if name in NEVER_SPLIT:
            continue
        venues_with_threshold = [v for v, c in row.items() if c >= AUTO_SPLIT_THRESHOLD]
        if name in FORCE_SPLIT:
            # Force split — use ALL venues where the name appears, not just threshold
            venues_with_threshold = sorted(
                [v for v, c in row.items() if c > 0],
                key=lambda v: -row[v],
            )
        if len(venues_with_threshold) >= 2:
            split_info[name] = venues_with_threshold

    # Apply: rename `name` to 'Name · Venue' for mentions of split names
    if split_info:
        def maybe_split(row):
            n = row["name"]
            if n in split_info:
                # Only rewrite if this venue is one of the split venues
                if row["branch"] in split_info[n]:
                    return f"{n} · {row['branch']}"
                else:
                    # Edge case: a stray mention at a non-threshold venue —
                    # attribute to the closest split (highest-count venue)
                    return f"{n} · {split_info[n][0]}"
            return n
        mentions = mentions.copy()
        mentions["name"] = mentions.apply(maybe_split, axis=1)

    return mentions, split_info


# ---------------------------------------------------------------------------
# 5. Build employee aggregates for the dashboard
# ---------------------------------------------------------------------------

def build_employee_profiles(mentions: pd.DataFrame) -> list[dict]:
    """Aggregate per-employee stats, primary venue, period breakdown, quotes."""
    if mentions.empty:
        return []

    employees = []
    for name, group in mentions.groupby("name"):
        positives = group[group["sentiment"] == "positive"]
        neutrals  = group[group["sentiment"] == "neutral"]
        negatives = group[group["sentiment"] == "negative"]

        # Primary venue: most-positive-mentions venue, with manual override
        if name in PRIMARY_VENUE_OVERRIDE:
            primary_venue = PRIMARY_VENUE_OVERRIDE[name]
        else:
            venue_pos = positives["branch"].value_counts()
            primary_venue = venue_pos.index[0] if len(venue_pos) > 0 else group["branch"].iloc[0]

        # Period breakdown
        period_counts = positives.groupby("period").size().to_dict()
        # Venue breakdown
        venue_counts = positives.groupby("branch").size().to_dict()

        # Quotes: only descriptive ones (>= 12 chars beyond the name itself)
        # Sort by length desc, keep all
        quotes = []
        seen_quotes = set()
        for _, row in positives.sort_values(by="quote", key=lambda s: s.str.len(),
                                              ascending=False).iterrows():
            q = (row["quote"] or "").strip()
            if not q or q.lower() in seen_quotes:
                continue
            seen_quotes.add(q.lower())
            # Skip bare name-only mentions
            stripped = re.sub(rf"\b{re.escape(name.split(' · ')[0])}\b", "",
                              q, flags=re.IGNORECASE).strip(" .,!?;:-")
            if len(stripped) < 8:
                continue
            quotes.append({
                "text": q,
                "date": row["date"],
                "venue": row["branch"],
                "period": row["period"],
            })

        bare_count = len(positives) - len(quotes)

        employees.append({
            "name": name,
            "primary_venue": primary_venue,
            "positive": int(len(positives)),
            "neutral": int(len(neutrals)),
            "negative": int(len(negatives)),
            "period_counts": {p: int(c) for p, c in period_counts.items()},
            "venue_counts": {v: int(c) for v, c in venue_counts.items()},
            "quotes": quotes,
            "bare_mentions": int(bare_count),
        })

    employees.sort(key=lambda e: -e["positive"])
    return employees


# ---------------------------------------------------------------------------
# 6. Anomaly detection (compare current run to previous snapshot)
# ---------------------------------------------------------------------------

def detect_anomalies(current: dict, previous: dict | None) -> list[dict]:
    """
    Flag changes that should pause the publish for review:
      - New venues appearing in the data
      - New top-30 names that look suspicious (single capitalized word, common
        non-name patterns)
      - Existing employees whose primary venue has shifted
    """
    anomalies = []

    # New venues
    cur_venues = set(current["venues"])
    prev_venues = set(previous["venues"]) if previous else cur_venues
    new_venues = cur_venues - prev_venues
    for v in new_venues:
        anomalies.append({
            "type": "new_venue",
            "severity": "high",
            "detail": f"New venue appeared in data: {v}. Confirm it's a real Flight Club site, not a typo or location name in reviews.",
        })

    # New employees in top 30
    cur_top = [e["name"] for e in current["employees"][:30]]
    prev_top = set(e["name"] for e in previous["employees"][:50]) if previous else set()
    for name in cur_top:
        if name not in prev_top and previous is not None:
            anomalies.append({
                "type": "new_top_employee",
                "severity": "medium",
                "detail": f"'{name}' entered the top 30 this run. Sanity-check it's a real employee name and not a missed false-positive.",
            })

    # Venue moves
    if previous:
        prev_venue_by_name = {e["name"]: e["primary_venue"] for e in previous["employees"]}
        for e in current["employees"][:60]:
            old = prev_venue_by_name.get(e["name"])
            if old and old != e["primary_venue"]:
                anomalies.append({
                    "type": "venue_move",
                    "severity": "low",
                    "detail": f"{e['name']} primary venue shifted: {old} -> {e['primary_venue']}. May indicate a staff move (consider PRIMARY_VENUE_OVERRIDE).",
                })

    return anomalies


# ---------------------------------------------------------------------------
# 7. Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-dir", default=str(Path(__file__).resolve().parent.parent))
    args = ap.parse_args()

    project_dir = Path(args.project_dir)
    data_dir    = project_dir / "data"
    out_dir     = project_dir / "Outputs"
    out_dir.mkdir(exist_ok=True)
    data_dir.mkdir(exist_ok=True)

    print(f"[1/6] Loading CSVs from {data_dir} and project root ({project_dir})")
    rows = load_all_csvs(data_dir, project_dir)

    print(f"[2/6] Building per-visit corpus")
    visits = build_visit_corpus(rows)
    print(f"      {len(visits)} unique visits")
    print(f"      date range: {visits['VisitDate'].min()} -> {visits['VisitDate'].max()}")
    print(f"      branches: {sorted(visits['Branch'].dropna().unique())}")
    print(f"      period split: {visits['Period'].value_counts().to_dict()}")

    print(f"[3/6] Building lc-ratio map + extracting employee mentions")
    lc_ratio_map = build_lc_ratio_map(rows)
    print(f"      lc-ratio map covers {len(lc_ratio_map)} candidate words")
    mentions = extract_mentions(visits, lc_ratio_map)
    print(f"      {len(mentions)} mentions before splitting")
    print(f"      sentiment split: {mentions['sentiment'].value_counts().to_dict()}")

    print(f"[4/6] Applying venue splits")
    mentions, split_info = apply_venue_splits(mentions)
    print(f"      {len(split_info)} names split by venue:")
    for n, vs in split_info.items():
        print(f"        {n} -> {vs}")

    print(f"[5/6] Building employee profiles")
    employees = build_employee_profiles(mentions)
    print(f"      {len(employees)} unique employees")
    print(f"      total positive mentions: {sum(e['positive'] for e in employees)}")

    # Build the summary blob the dashboard consumes
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "visit_count": int(len(visits)),
        "date_range": [
            visits["VisitDate"].min().isoformat() if visits["VisitDate"].min() else None,
            visits["VisitDate"].max().isoformat() if visits["VisitDate"].max() else None,
        ],
        "periods": [
            {"id": p["id"], "start": p["start"].isoformat(), "end": p["end"].isoformat()}
            for p in PERIODS
        ],
        "quarters": QUARTERS,
        "venues": sorted(visits["Branch"].dropna().unique().tolist()),
        "employees": employees,
        "totals": {
            "positive": sum(e["positive"] for e in employees),
            "neutral":  sum(e["neutral"]  for e in employees),
            "negative": sum(e["negative"] for e in employees),
            "by_period": {
                p: int(((mentions["period"] == p) & (mentions["sentiment"] == "positive")).sum())
                for p in [pp["id"] for pp in PERIODS]
            },
            "by_venue": {
                v: int(((mentions["branch"] == v) & (mentions["sentiment"] == "positive")).sum())
                for v in sorted(visits["Branch"].dropna().unique())
            },
        },
        "split_info": split_info,
    }

    # Compact mentions array (for client-side cross-period+venue filtering)
    summary["mentions"] = [
        {"e": m["name"], "b": m["branch"], "p": m["period"], "s": m["sentiment"][0]}
        for _, m in mentions.iterrows()
    ]

    print(f"[6/6] Writing outputs to {out_dir}")
    # Load previous snapshot before writing the new one
    snapshot_path = out_dir / "snapshot.json"
    previous = None
    if snapshot_path.exists():
        try:
            previous = json.loads(snapshot_path.read_text(encoding="utf-8"))
        except Exception:
            previous = None

    anomalies = detect_anomalies(summary, previous)
    summary["anomalies"] = anomalies
    if anomalies:
        print(f"      ! {len(anomalies)} anomalies detected:")
        for a in anomalies:
            print(f"        [{a['severity']}] {a['type']}: {a['detail']}")
    else:
        print(f"      no anomalies vs prior run")

    # Write JSON data
    (out_dir / "dashboard_data.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )
    snapshot_path.write_text(json.dumps(summary, default=str), encoding="utf-8")

    # Write changelog entry
    changelog = out_dir / "changelog.md"
    top5_str = ", ".join("{0} ({1})".format(e["name"], e["positive"]) for e in employees[:5])
    entry = (
        f"## {summary['generated_at']}\n"
        f"- visits: {summary['visit_count']}\n"
        f"- positive mentions: {summary['totals']['positive']}\n"
        f"- employees: {len(employees)}\n"
        f"- top 5: {top5_str}\n"
        f"- anomalies: {len(anomalies)}\n\n"
    )
    if changelog.exists():
        existing = changelog.read_text(encoding="utf-8")
        changelog.write_text(entry + existing, encoding="utf-8")
    else:
        changelog.write_text(entry, encoding="utf-8")

    # Write anomaly report
    if anomalies:
        anom_md = "# Anomalies \u2014 needs review\n\n"
        for a in anomalies:
            anom_md += f"- **[{a['severity']}] {a['type']}** \u2014 {a['detail']}\n"
        (out_dir / "anomalies.md").write_text(anom_md, encoding="utf-8")

    # Render the HTML dashboard
    from build_dashboard import build_dashboard
    html = build_dashboard(summary)
    (out_dir / "dashboard.html").write_text(html, encoding="utf-8")
    print(f"      done. dashboard at: {out_dir / 'dashboard.html'}")


if __name__ == "__main__":
    main()
