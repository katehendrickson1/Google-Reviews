import os
from pathlib import Path

from google.oauth2.service_account import Credentials


def creds_path() -> str:
    if path := os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return path
    if Path("service_account.json").exists():
        return "service_account.json"
    parent = Path(__file__).parent.parent / "service_account.json"
    if parent.exists():
        return str(parent)
    raise FileNotFoundError(
        "service_account.json not found. Set GOOGLE_APPLICATION_CREDENTIALS or place "
        "service_account.json at the repo root."
    )


def sheets_credentials() -> Credentials:
    return Credentials.from_service_account_file(
        creds_path(),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )


def vertex_credentials() -> Credentials:
    return Credentials.from_service_account_file(
        creds_path(),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
