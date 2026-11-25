"""Print a Lightning Queries reply as a table: `curl ... | uv run lightning-table`.

Reads the reply's JSON on stdin. A row too wide for the terminal (the agent's reasoning, say) prints one
field per line instead, so nothing is cut off. Anything that isn't a reply with rows (an error) prints as is.
"""

import json
import shutil
import sys


def _cell(value) -> str:
    return "" if value is None else value if isinstance(value, str) else json.dumps(value)


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # Windows consoles default to a code page
    text = sys.stdin.read().lstrip("﻿")  # PowerShell can start piped text with a byte order mark
    try:
        result = json.loads(text)["result"]
        columns = [str(c["name"]).lower() for c in result["schema"]["columns"]]
        rows = [[_cell(v) for v in row] for row in result.get("data") or []]
    except (ValueError, KeyError, TypeError):
        print(text, end="" if text.endswith("\n") else "\n")
        return

    widths = [max([len(name), *(len(row[i]) for row in rows if i < len(row))]) for i, name in enumerate(columns)]
    if sum(widths) + 2 * (len(widths) - 1) <= shutil.get_terminal_size((120, 24))[0]:
        print("  ".join(name.ljust(w) for name, w in zip(columns, widths, strict=False)).rstrip())
        print("  ".join("-" * w for w in widths))
        for row in rows:
            print("  ".join(v.ljust(w) for v, w in zip(row, widths, strict=False)).rstrip())
    else:
        label = max(map(len, columns), default=0)
        for n, row in enumerate(rows):
            if n:
                print()
            for name, value in zip(columns, row, strict=False):
                print(f"{name.ljust(label)} : {value}")
    print(f"{len(rows)} row(s)")


if __name__ == "__main__":
    main()
