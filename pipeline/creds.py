"""
HGEM MVP Dashboard — Credential Storage

SUMMARY
-------
Simple symmetric-encryption credential store for the GitHub PAT and HGEM
username/password. Uses Fernet (AES-128 in CBC mode + HMAC) from the
`cryptography` library.

Where things live:
  Master key:  ~/.hgem_mvp_key                 (auto-generated on first use)
  Secrets:     <project>/Outputs/secrets.enc   (Fernet-encrypted JSON dict)

Permissions:
  Both files are written with mode 0600 (owner read/write only).

Usage:
  # Save a credential
  python creds.py set github_pat
  # (you'll be prompted to paste the value; not echoed in the shell)

  # Or programmatically
  from creds import set_credential, get_credential
  set_credential("github_pat", "github_pat_AB...")
  token = get_credential("github_pat")
"""
from __future__ import annotations
import getpass
import json
import os
import sys
from pathlib import Path

from cryptography.fernet import Fernet

# Master key lives outside the project folder so it doesn't end up in a sync
HOME_KEY_PATH = Path.home() / ".hgem_mvp_key"

# Encrypted secrets live in the project Outputs folder
SECRETS_PATH = Path(__file__).resolve().parent.parent / "Outputs" / "secrets.enc"


def _load_or_create_key() -> bytes:
    """Read the master key, or generate one on first use."""
    if HOME_KEY_PATH.exists():
        return HOME_KEY_PATH.read_bytes()
    key = Fernet.generate_key()
    HOME_KEY_PATH.write_bytes(key)
    try:
        HOME_KEY_PATH.chmod(0o600)
    except Exception:
        pass
    print(f"  generated new master key at {HOME_KEY_PATH}", file=sys.stderr)
    return key


def _fernet() -> Fernet:
    return Fernet(_load_or_create_key())


def _load_secrets() -> dict:
    if not SECRETS_PATH.exists():
        return {}
    try:
        raw = SECRETS_PATH.read_bytes()
        decrypted = _fernet().decrypt(raw)
        return json.loads(decrypted.decode("utf-8"))
    except Exception as e:
        raise SystemExit(f"Could not decrypt {SECRETS_PATH}: {e}")


def _save_secrets(secrets: dict) -> None:
    SECRETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    encrypted = _fernet().encrypt(json.dumps(secrets).encode("utf-8"))
    SECRETS_PATH.write_bytes(encrypted)
    try:
        SECRETS_PATH.chmod(0o600)
    except Exception:
        pass


def set_credential(name: str, value: str) -> None:
    """Save a credential. Overwrites any existing value."""
    secrets = _load_secrets()
    secrets[name] = value
    _save_secrets(secrets)
    print(f"  saved credential '{name}' to {SECRETS_PATH}", file=sys.stderr)


def get_credential(name: str) -> str | None:
    """Read a credential. Returns None if not set."""
    return _load_secrets().get(name)


def list_credentials() -> list[str]:
    """List the names of stored credentials (not the values)."""
    return list(_load_secrets().keys())


def delete_credential(name: str) -> None:
    """Remove a credential."""
    secrets = _load_secrets()
    if name in secrets:
        del secrets[name]
        _save_secrets(secrets)
        print(f"  removed '{name}'", file=sys.stderr)


def main():
    if len(sys.argv) < 2:
        print("Usage: python creds.py <set|get|list|delete> [name]")
        print(f"  Key:     {HOME_KEY_PATH}")
        print(f"  Secrets: {SECRETS_PATH}")
        return

    cmd = sys.argv[1]
    if cmd == "list":
        for n in list_credentials():
            print(f"  {n}")
    elif cmd == "set":
        if len(sys.argv) < 3:
            raise SystemExit("Usage: python creds.py set <name>")
        name = sys.argv[2]
        value = getpass.getpass(f"Enter value for {name}: ")
        set_credential(name, value)
    elif cmd == "get":
        if len(sys.argv) < 3:
            raise SystemExit("Usage: python creds.py get <name>")
        v = get_credential(sys.argv[2])
        print(v if v else "(not set)")
    elif cmd == "delete":
        if len(sys.argv) < 3:
            raise SystemExit("Usage: python creds.py delete <name>")
        delete_credential(sys.argv[2])
    else:
        raise SystemExit(f"Unknown command: {cmd}")


if __name__ == "__main__":
    main()
