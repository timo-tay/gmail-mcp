#!/usr/bin/env python3
"""Targeted re-auth for the four non–Startup TNT accounts, forcing Chrome.

Targets: gmail, ualberta, iconii, yegaf.

Run with the project venv:
    /Users/tim/mcps/gmail-mcp/.venv/bin/python reauth_chrome.py
"""
import subprocess
import sys
import webbrowser

from config import (
    get_accounts,
    get_client_secret_path,
    get_credentials_dir,
    load_config,
)
from auth import AuthManager

TARGETS = ["gmail", "ualberta", "iconii", "yegaf"]


# ---- force Google Chrome regardless of the system default browser ----------
class ChromeBrowser(webbrowser.BaseBrowser):
    def open(self, url, new=0, autoraise=True):
        try:
            subprocess.Popen(["open", "-a", "Google Chrome", url])
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"  (could not launch Chrome: {exc})", flush=True)
            return False


webbrowser.register("chrome", None, ChromeBrowser(), preferred=True)


def main() -> None:
    config = load_config()
    accounts = get_accounts(config)
    auth = AuthManager(get_credentials_dir(config), get_client_secret_path(config))

    for name in TARGETS:
        info = accounts.get(name)
        if info is None:
            print(f"SKIP {name}: not in config.json", flush=True)
            continue
        email = info.get("email", name)
        print(f"\n=== Authenticating '{name}' ({email}) — sign in as {email} ===", flush=True)
        try:
            auth.authenticate(name, email=email)
            ok = auth.is_authenticated(name)
            print(f"{'OK' if ok else 'UNVERIFIED'} {name}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL {name}: {exc}", flush=True)

    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
