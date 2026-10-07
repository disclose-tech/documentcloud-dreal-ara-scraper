"""Event data helpers, shared by the UploadPipeline and the archive refresh script.

Event data is a dict {"documents": {key: {...}}, "zips": {url: {...}}} of the files
already uploaded, where key is the file URL (or zip URL + "/" + path in the zip), and
"zips" lists the zip files whose supported files have all been uploaded.

It is split in two:
- the archive (event_data/archive.json), committed in the repo and loaded from disk
- the live event data, stored on DocumentCloud, holding only the entries that are
  not archived yet (keeps the PATCH requests to DocumentCloud small)
"""

import json
from pathlib import Path

ARCHIVE_PATH = Path(__file__).parent.parent / "event_data" / "archive.json"

SECTIONS = ("documents", "zips")


def load_archive(path=ARCHIVE_PATH):
    """Loads the archived event data, or empty sections if there is no archive."""

    if not path.exists():
        return {section: {} for section in SECTIONS}

    with open(path, "r") as file:
        return json.load(file)


def merge_event_data(archive, event_data):
    """Adds the entries of event_data to archive (in place).

    Live event data is more recent: it wins over archived entries.
    """

    for section in SECTIONS:
        archive.setdefault(section, {}).update(event_data.get(section) or {})

    return archive


def write_archive(event_data, path=ARCHIVE_PATH):
    """Writes the archived event data, entries sorted by key, one entry per line."""

    path.parent.mkdir(exist_ok=True)

    sections = []
    for section in SECTIONS:
        entries = event_data.get(section) or {}
        lines = [f"{json.dumps(key)}: {json.dumps(entries[key])}" for key in sorted(entries)]
        body = "{\n" + ",\n".join(lines) + "\n}" if lines else "{}"
        sections.append(f"{json.dumps(section)}: {body}")

    with open(path, "w") as file:
        file.write("{\n" + ",\n".join(sections) + "\n}\n")
