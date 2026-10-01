"""Connexion à MinIO : les identifiants sont lus hors du code source.

Ordre de priorité :
1. variables d'environnement MINIO_URL / MINIO_ACCESS_KEY / MINIO_SECRET_KEY
2. fichier credentials.json à la racine du projet (jamais inclus dans le rendu)
"""
import json
import os
from pathlib import Path

import boto3

BUCKET = "tp2"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CREDENTIALS_FILE = PROJECT_ROOT / "credentials.json"


def get_credentials():
    if os.getenv("MINIO_ACCESS_KEY") and os.getenv("MINIO_SECRET_KEY"):
        return {
            "url": os.getenv("MINIO_URL", "http://127.0.0.1:9000"),
            "accessKey": os.environ["MINIO_ACCESS_KEY"],
            "secretKey": os.environ["MINIO_SECRET_KEY"],
        }
    with open(CREDENTIALS_FILE, encoding="utf-8") as f:
        return json.load(f)


def get_s3_client():
    cred = get_credentials()
    return boto3.client(
        "s3",
        endpoint_url=cred["url"],
        aws_access_key_id=cred["accessKey"],
        aws_secret_access_key=cred["secretKey"],
    )
