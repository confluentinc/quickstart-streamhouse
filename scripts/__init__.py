"""Deploy, data-generation, serving, and app tooling for the Streamhouse flight recovery demo."""

import sys


def _utf8_when_redirected() -> None:
    """On Windows, write UTF-8 when stdout or stderr is a file or pipe instead of a console.

    Python writes a Windows console in Unicode, but a redirected stream uses the ANSI code page (cp1252), which
    has no ✓ or →: the first status line would raise UnicodeEncodeError. That killed deploy's background stream,
    whose output goes to tmp/datagen.log, and `uv run deploy > deploy.log`. Every entry point imports this package.
    """
    if sys.platform != "win32":
        return
    for stream in (sys.stdout, sys.stderr):
        try:
            if stream is not None and not stream.isatty() and stream.encoding.lower().replace("-", "") != "utf8":
                stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, OSError, ValueError):
            pass  # a replaced stream (pytest's capture, say) that can't be reconfigured


_utf8_when_redirected()
