"""Detect event types from journal metadata and message content."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterable


def strip_splunk_prefix(value: str) -> str:
    """Remove Splunk metadata prefixes like host::, source::, sourcetype::."""
    for prefix in ("host::", "source::", "sourcetype::"):
        if value.startswith(prefix):
            return value[len(prefix) :]
    return value


class EventKind(str, Enum):
    ACCESS_LOG = "access_log"
    SYSLOG = "syslog"
    GENERIC = "generic"


ACCESS_SOURCETYPE_RE = re.compile(
    r"(access|apache|nginx|iis|httpd|lb)",
    re.IGNORECASE,
)
SYSLOG_SOURCETYPE_RE = re.compile(r"syslog", re.IGNORECASE)
ACCESS_SOURCE_RE = re.compile(
    r"(/var/log/(nginx|apache2?|httpd)|access\.log)",
    re.IGNORECASE,
)
SYSLOG_SOURCE_RE = re.compile(r"/var/log/syslog", re.IGNORECASE)

# Combined / nginx-plus access log line
ACCESS_MESSAGE_RE = re.compile(
    r"^\S+\s+\S+\s+\S+\s+\[[^\]]+\]\s+\""
    r"(?:GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH|CONNECT|TRACE)\s",
    re.IGNORECASE,
)
# RFC3164 syslog with priority prefix
SYSLOG_MESSAGE_RE = re.compile(
    r"^<\d+>[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\S+",
)


@dataclass(frozen=True)
class ClassifiedEvent:
    kind: EventKind
    dataset: str
    pipeline_name: str
    reason: str


@dataclass
class EventProfile:
    """Summary of event kinds discovered across one or more journals."""

    by_kind: Counter[EventKind] = field(default_factory=Counter)
    by_sourcetype: Counter[str] = field(default_factory=Counter)
    kinds_seen: set[EventKind] = field(default_factory=set)

    def merge(self, other: EventProfile) -> None:
        self.by_kind.update(other.by_kind)
        self.by_sourcetype.update(other.by_sourcetype)
        self.kinds_seen.update(other.kinds_seen)


def parser_pipeline_name(kind: EventKind) -> str:
    return f"frosty-parse-{kind.value.replace('_', '-')}"


def router_pipeline_name(index_name: str | None = None) -> str:
    if index_name:
        return f"frosty-pipeline-{index_name}"
    return "frosty-pipeline"


def classify_event(
    *,
    sourcetype: str = "",
    source: str = "",
    message: str = "",
    splunk_index: str = "",
) -> ClassifiedEvent:
    """Determine event kind and target ingest pipeline for a journal event."""
    st = strip_splunk_prefix(sourcetype).lower()
    src = strip_splunk_prefix(source).lower()
    msg = message.strip()

    if st and ACCESS_SOURCETYPE_RE.search(st):
        kind = EventKind.ACCESS_LOG
        reason = f"sourcetype={st!r}"
    elif st and SYSLOG_SOURCETYPE_RE.search(st):
        kind = EventKind.SYSLOG
        reason = f"sourcetype={st!r}"
    elif src and ACCESS_SOURCE_RE.search(src):
        kind = EventKind.ACCESS_LOG
        reason = f"source={src!r}"
    elif src and SYSLOG_SOURCE_RE.search(src):
        kind = EventKind.SYSLOG
        reason = f"source={src!r}"
    elif ACCESS_MESSAGE_RE.match(msg):
        kind = EventKind.ACCESS_LOG
        reason = "message=access_log_pattern"
    elif SYSLOG_MESSAGE_RE.match(msg):
        kind = EventKind.SYSLOG
        reason = "message=syslog_pattern"
    else:
        kind = EventKind.GENERIC
        reason = "fallback=generic"

    dataset = f"{splunk_index}.{kind.value}" if splunk_index else kind.value
    return ClassifiedEvent(
        kind=kind,
        dataset=dataset,
        pipeline_name=parser_pipeline_name(kind),
        reason=reason,
    )


from frosty.buckets import FrozenBucket


def profile_journal(journal_path: Path, *, splunk_index: str = "") -> EventProfile:
    """Scan a journal file and tally event kinds without loading all events into memory."""
    from frosty.journal import iter_journal_events

    profile = EventProfile()
    for event in iter_journal_events(journal_path):
        st = strip_splunk_prefix(str(event.get("sourcetype", "")))
        classified = classify_event(
            sourcetype=st,
            source=str(event.get("source", "")),
            message=str(event.get("event", "")),
            splunk_index=splunk_index,
        )
        profile.by_kind[classified.kind] += 1
        if st:
            profile.by_sourcetype[st] += 1
        profile.kinds_seen.add(classified.kind)
    return profile


def profile_buckets(buckets: Iterable[FrozenBucket]) -> EventProfile:
    combined = EventProfile()
    for bucket in buckets:
        combined.merge(profile_journal(bucket.journal_path, splunk_index=bucket.index_name))
    return combined


def required_parser_pipelines(profile: EventProfile) -> list[EventKind]:
    """Return parser pipeline kinds needed for the discovered profile."""
    kinds = sorted(profile.kinds_seen, key=lambda k: k.value)
    return kinds if kinds else [EventKind.GENERIC]
