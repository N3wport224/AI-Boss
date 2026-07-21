"""Desktop entry point for the packaged AI-Boss executable.

Boots the same FastAPI app the dashboard normally runs via
`uvicorn webapp.main:app`, but as a plain Python script PyInstaller can
freeze into a single .exe -- no shell, no `uvicorn` command, and no Python
install required on the end user's machine. Opens the dashboard in the
default browser automatically once the server is up, since a double-clicked
.exe has no terminal to type a URL into.
"""
import threading
import webbrowser

import uvicorn

HOST = "127.0.0.1"
PORT = 8000


def _open_browser() -> None:
    webbrowser.open(f"http://{HOST}:{PORT}")


def main() -> None:
    # Persistent-storage env vars (AIBOSS_DB_PATH and friends) get set here,
    # BEFORE this import -- webapp.main reads them at import time, so this
    # is the one place that ordering matters. Wired in a later step.
    import webapp.main as app_module

    threading.Timer(1.5, _open_browser).start()
    uvicorn.run(app_module.app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
