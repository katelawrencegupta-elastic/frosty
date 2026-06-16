"""Elasticsearch ingest pipeline definitions, keyed by detected event kind."""

from __future__ import annotations

from frosty.event_types import EventKind, parser_pipeline_name, router_pipeline_name

ACCESS_LOG_GROK = (
    '%{IPORHOST:client.ip} (?:%{DATA:http.ident}|-) (?:%{DATA:http.auth}|-) '
    '\\[%{HTTPDATE:http.request.timestamp}\\] "%{WORD:http.request.method} '
    '%{DATA:url.original} HTTP/%{NUMBER:http.version}" '
    '%{NUMBER:http.response.status_code} %{NUMBER:http.response.body.bytes} '
    '"%{DATA:http.request.referrer}" "%{DATA:user_agent.original}"'
)

SYSLOG_GROK = (
    '<%{POSINT:log.syslog.priority}>%{SYSLOGTIMESTAMP:syslog.timestamp} '
    '%{NOTSPACE:syslog.hostname} %{DATA:syslog.program}'
    '(?:\\[%{POSINT:process.pid}\\])?: %{GREEDYDATA:syslog.message}'
)

SSHD_FAILED_GROK = (
    'Failed password for (?:invalid user )?%{USERNAME:user.name} '
    'from %{IP:source.ip} port %{NUMBER:source.port} %{GREEDYDATA}'
)

SUDO_GROK = (
    '%{USERNAME:user.name} : TTY=%{DATA:tty} ; PWD=%{DATA:pwd} ; '
    'USER=%{USERNAME:sudo.user} ; COMMAND=%{GREEDYDATA:sudo.command}'
)

ON_FAILURE = [
    {
        "set": {
            "field": "error.message",
            "value": "{{ _ingest.on_failure_message }}",
        }
    }
]


def _set_original() -> dict:
    return {
        "set": {
            "field": "event.original",
            "copy_from": "message",
            "ignore_empty_value": True,
        }
    }


def _access_log_processors() -> list[dict]:
    return [
        _set_original(),
        {
            "grok": {
                "field": "message",
                "patterns": [ACCESS_LOG_GROK],
                "ignore_missing": True,
                "ignore_failure": True,
            }
        },
        {
            "date": {
                "field": "http.request.timestamp",
                "target_field": "@timestamp",
                "formats": ["dd/MMM/yyyy:HH:mm:ss Z"],
                "ignore_failure": True,
            }
        },
        {
            "convert": {
                "field": "http.response.status_code",
                "type": "integer",
                "ignore_missing": True,
                "ignore_failure": True,
            }
        },
        {
            "convert": {
                "field": "http.response.body.bytes",
                "type": "long",
                "ignore_missing": True,
                "ignore_failure": True,
            }
        },
        {
            "convert": {
                "field": "http.version",
                "type": "float",
                "ignore_missing": True,
                "ignore_failure": True,
            }
        },
        {"set": {"field": "event.category", "value": "web"}},
    ]


def _syslog_processors() -> list[dict]:
    return [
        _set_original(),
        {
            "grok": {
                "field": "message",
                "patterns": [SYSLOG_GROK],
                "ignore_missing": True,
                "ignore_failure": True,
            }
        },
        {
            "date": {
                "field": "syslog.timestamp",
                "target_field": "@timestamp",
                "formats": ["MMM  d HH:mm:ss", "MMM dd HH:mm:ss"],
                "ignore_failure": True,
            }
        },
        {
            "convert": {
                "field": "log.syslog.priority",
                "type": "integer",
                "ignore_missing": True,
                "ignore_failure": True,
            }
        },
        {
            "convert": {
                "field": "process.pid",
                "type": "integer",
                "ignore_missing": True,
                "ignore_failure": True,
            }
        },
        {
            "grok": {
                "field": "syslog.message",
                "patterns": [SSHD_FAILED_GROK],
                "ignore_missing": True,
                "ignore_failure": True,
            }
        },
        {
            "grok": {
                "field": "syslog.message",
                "patterns": [SUDO_GROK],
                "ignore_missing": True,
                "ignore_failure": True,
            }
        },
        {
            "convert": {
                "field": "source.port",
                "type": "integer",
                "ignore_missing": True,
                "ignore_failure": True,
            }
        },
        {"set": {"field": "event.category", "value": "host"}},
    ]


def _generic_processors() -> list[dict]:
    return [
        _set_original(),
        {"set": {"field": "event.category", "value": "unknown"}},
    ]


PARSER_BUILDERS: dict[EventKind, tuple[str, list[dict]]] = {
    EventKind.ACCESS_LOG: (
        "GROK parser for HTTP access / combined log lines",
        _access_log_processors(),
    ),
    EventKind.SYSLOG: (
        "GROK parser for RFC3164 syslog messages",
        _syslog_processors(),
    ),
    EventKind.GENERIC: (
        "Passthrough parser for unrecognized log lines",
        _generic_processors(),
    ),
}


def build_parser_pipeline(kind: EventKind) -> dict:
    description, processors = PARSER_BUILDERS[kind]
    return {
        "description": description,
        "processors": processors + [{"set": {"field": "event.kind", "value": kind.value}}],
        "on_failure": ON_FAILURE,
    }


def _router_preprocessors() -> list[dict]:
    """Classify event.kind at ingest time when not already set (e.g. during reindex)."""
    return [
        {
            "set": {
                "field": "event.kind",
                "value": EventKind.ACCESS_LOG.value,
                "if": (
                    "ctx.event?.kind == null && ctx.sourcetype != null && "
                    "/(?i).*(access|nginx|apache|httpd|iis).*/.matcher(ctx.sourcetype).find()"
                ),
            }
        },
        {
            "set": {
                "field": "event.kind",
                "value": EventKind.SYSLOG.value,
                "if": (
                    "ctx.event?.kind == null && ctx.sourcetype != null && "
                    "/(?i).*syslog.*/.matcher(ctx.sourcetype).find()"
                ),
            }
        },
        {
            "set": {
                "field": "event.kind",
                "value": EventKind.GENERIC.value,
                "if": "ctx.event?.kind == null",
            }
        },
    ]


def build_router_pipeline(kinds: list[EventKind], index_name: str) -> dict:
    """Build a router pipeline that delegates to per-kind parsers via event.kind."""
    processors: list[dict] = _router_preprocessors()
    for kind in kinds:
        if kind == EventKind.GENERIC:
            continue
        processors.append(
            {
                "pipeline": {
                    "name": parser_pipeline_name(kind),
                    "if": f"ctx.event?.kind == '{kind.value}'",
                }
            }
        )

    # Generic is the fallback when no specialized parser matched at ingest time
    if EventKind.GENERIC in kinds:
        processors.append(
            {
                "pipeline": {
                    "name": parser_pipeline_name(EventKind.GENERIC),
                    "if": "ctx.event?.kind == 'generic'",
                }
            }
        )

    return {
        "description": f"Route frosty-{index_name} events to parsers by detected event.kind",
        "processors": processors,
        "on_failure": ON_FAILURE,
    }


def pipelines_for_index(kinds: list[EventKind], index_name: str) -> dict[str, dict]:
    """Return all pipeline definitions needed for an index given discovered event kinds."""
    pipelines: dict[str, dict] = {}
    for kind in kinds:
        pipelines[parser_pipeline_name(kind)] = build_parser_pipeline(kind)
    pipelines[router_pipeline_name(index_name)] = build_router_pipeline(kinds, index_name)
    return pipelines
