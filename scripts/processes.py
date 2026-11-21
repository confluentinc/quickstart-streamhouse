"""The local processes the labs start, check, and stop, on macOS, Linux, and Windows.

Windows has no `ps` or `lsof`, its os.kill(pid, 0) sends a console Ctrl+C event rather than checking that the
process exists, start_new_session does nothing there, and a `.cmd` shim (npx, or claude installed with npm) can't
be started by name. Everything that would reach for those goes through here instead. Stdlib only.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
from pathlib import Path

WINDOWS = sys.platform == "win32"


def exe(name: str) -> str:
    """The program to put first in a subprocess argv for a tool found on PATH.

    Windows' CreateProcess finds `name.exe` on PATH but not `name.cmd` or `name.bat`, so a shim gets its full path
    (from shutil.which). A real .exe, and every tool elsewhere, keeps its bare name, which reads well in messages.
    """
    if WINDOWS:
        found = shutil.which(name)
        if found and found.lower().endswith((".cmd", ".bat")):
            return found
    return name


def detached() -> dict:
    """Popen keyword arguments that let a background process outlive the terminal that started it.

    On Windows that means its own hidden console and process group: closing the terminal window (which sends
    CTRL_CLOSE_EVENT to every process attached to its console) and Ctrl+C in it no longer reach the process.
    DETACHED_PROCESS would leave it without a console, and the console programs it starts (`uv run` starts
    several) would each open a visible window.
    """
    if WINDOWS:
        return {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW}
    return {"start_new_session": True}


def pid_alive(pid: int) -> bool:
    """True while a process with this ID is running."""
    if pid <= 0:
        return False
    if WINDOWS:
        return _windows_pid_alive(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # it exists; it belongs to another user
    except OSError:
        return False
    return True


def command_line(pid: int) -> str | None:
    """The process's command line: "" when it isn't running, None when this computer can't tell."""
    if WINDOWS:
        rows = _windows_processes(f"ProcessId={int(pid)}")
        return None if rows is None else rows.get(pid, "")
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip()


def all_command_lines() -> dict[int, str] | None:
    """Every process's command line by PID, or None when this computer can't tell."""
    if WINDOWS:
        return _windows_processes()
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid=,args="], capture_output=True, encoding="utf-8", errors="replace", timeout=15
        )
    except (OSError, subprocess.SubprocessError):
        return None
    found = {}
    for line in result.stdout.splitlines():
        pid, _, args = line.strip().partition(" ")
        if pid.isdigit():
            found[int(pid)] = args.strip()
    return found


def program_names(command: str) -> set[str]:
    """The program-like words of a command line, without folders or .exe, lower-cased.

    `"C:\\repo\\.venv\\Scripts\\python.exe" "C:\\repo\\.venv\\Scripts\\f1-race.exe" --laps 5` gives
    {"python", "f1-race", "--laps", "5"}, the same set as `/repo/.venv/bin/python /repo/.venv/bin/f1-race --laps 5`.
    """
    names = set()
    for word in command.replace('"', " ").split():
        name = word.replace("\\", "/").rsplit("/", 1)[-1].lower()
        names.add(name.removesuffix(".exe"))
    return names


def stop(pid: int, *markers: str) -> bool:
    """Stop the process if it is running and its command line names one of `markers`. True when it was stopped.

    The check guards against a PID that a finished process left in a file and the OS has since given to another
    program. When the command line can't be read (no `ps`; PowerShell blocked by policy on Windows), only a Python
    process is stopped, since every lab command runs in one.
    """
    if pid == os.getpid() or not pid_alive(pid):
        return False
    command = command_line(pid)
    if command is None:
        if not _is_python(pid):
            return False
    elif not any(marker in command for marker in markers):
        return False
    try:
        os.kill(pid, signal.SIGTERM)  # TerminateProcess on Windows
    except OSError:
        return False
    return True


def listening_pids(port: int) -> list[int] | None:
    """PIDs of the processes listening on this TCP port, or None when this computer can't tell."""
    if WINDOWS:
        return _windows_listening_pids(port)
    try:
        result = subprocess.run(
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return [int(pid) for pid in dict.fromkeys(result.stdout.split()) if pid.isdigit()]


def stop_command(pids: list[int]) -> str:
    """The command a person runs to stop these processes in this computer's usual shell."""
    if WINDOWS:  # taskkill works the same in PowerShell and Command Prompt
        return "taskkill /F " + " ".join(f"/PID {pid}" for pid in pids)
    return "kill " + " ".join(str(pid) for pid in pids)


# --- Windows ---------------------------------------------------------------------------------------------------


def _kernel32():
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.QueryFullProcessImageNameW.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPWSTR,
        ctypes.POINTER(wintypes.DWORD),
    ]
    kernel32.QueryFullProcessImageNameW.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    return ctypes, wintypes, kernel32


_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_STILL_ACTIVE = 259
_ERROR_ACCESS_DENIED = 5


def _windows_pid_alive(pid: int) -> bool:
    ctypes, wintypes, kernel32 = _kernel32()
    handle = kernel32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return ctypes.get_last_error() == _ERROR_ACCESS_DENIED  # it exists; it belongs to another user
    try:
        code = wintypes.DWORD()
        return bool(kernel32.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == _STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def _is_python(pid: int) -> bool:
    if not WINDOWS:
        return False
    ctypes, wintypes, kernel32 = _kernel32()
    handle = kernel32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return False
    try:
        size = wintypes.DWORD(32768)
        buffer = ctypes.create_unicode_buffer(size.value)
        if not kernel32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(size)):
            return False
        return Path(buffer.value).name.lower().startswith("python")
    finally:
        kernel32.CloseHandle(handle)


def _powershell(script: str) -> str | None:
    """Run a Windows PowerShell script and return what it prints (UTF-8), or None if it can't run."""
    powershell = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32" / "WindowsPowerShell" / "v1.0"
    try:
        result = subprocess.run(
            [
                str(powershell / "powershell.exe"),
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[Console]::OutputEncoding = [Text.Encoding]::UTF8; " + script,
            ],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout if result.returncode == 0 else None


def _windows_processes(cim_filter: str = "") -> dict[int, str] | None:
    where = f" -Filter '{cim_filter}'" if cim_filter else ""
    out = _powershell(
        f"Get-CimInstance Win32_Process{where} | ForEach-Object {{ '{{0}} {{1}}' -f $_.ProcessId, $_.CommandLine }}"
    )
    if out is None:
        return None
    found = {}
    for line in out.splitlines():
        pid, _, command = line.strip().partition(" ")
        if pid.isdigit():
            found[int(pid)] = command.strip()
    return found


def _windows_listening_pids(port: int) -> list[int] | None:
    """From netstat, whose columns don't depend on the Windows display language (its state names do)."""
    try:
        result = subprocess.run(
            ["netstat", "-ano"], capture_output=True, encoding="utf-8", errors="replace", timeout=15
        )
    except (OSError, subprocess.SubprocessError):
        return None
    pids = []
    for line in result.stdout.splitlines():
        fields = line.split()
        # TCP  127.0.0.1:8000  0.0.0.0:0  LISTENING  4242: a listener has no remote end.
        if len(fields) == 5 and fields[0].upper() == "TCP" and fields[2] in ("0.0.0.0:0", "[::]:0"):
            if fields[1].rsplit(":", 1)[-1] == str(port) and fields[4].isdigit():
                pids.append(int(fields[4]))
    return list(dict.fromkeys(pids))
