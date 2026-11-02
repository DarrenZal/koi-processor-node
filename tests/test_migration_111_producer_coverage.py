"""Every application INSERT into entity_registry must stamp migration 111."""

from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_every_entity_registry_insert_names_resolution_tier():
    offenders = []
    pattern = re.compile(
        r"INSERT\s+INTO\s+entity_registry\b(?P<body>.*?)(?:\"\"\"|''')",
        re.IGNORECASE | re.DOTALL,
    )
    for path in sorted((REPO_ROOT / "api").rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        for match in pattern.finditer(source):
            if "resolution_tier" not in match.group("body"):
                line = source.count("\n", 0, match.start()) + 1
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{line}")
    assert offenders == [], (
        "entity_registry producer(s) bypass migration-111 instrumentation: "
        + ", ".join(offenders)
    )
