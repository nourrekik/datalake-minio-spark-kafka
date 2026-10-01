"""Configuration commune : paramètres du projet et connexion MinIO par identité.

Deux identités techniques (mission 3) :
  - "ingestion" -> ingestion-user : alimente RAW
  - "pipeline"  -> pipeline-user  : transforme RAW -> PROCESSED -> CURATED

Les secrets ne sont JAMAIS dans le code. Pour une identité <profil>, ils sont lus :
  1. dans les variables d'environnement MINIO_<PROFIL>_ACCESS_KEY / MINIO_<PROFIL>_SECRET_KEY
  2. sinon dans config/credentials_<profil>.json (fichier exclu du rendu)
"""
import json
import os
from pathlib import Path

import boto3
from botocore.config import Config

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"

with open(CONFIG_DIR / "settings.json", encoding="utf-8") as f:
    SETTINGS = json.load(f)

BUCKET = SETTINGS["bucket"]
MINIO_URL = SETTINGS["minio_url"]


def get_credentials(profil):
    env = profil.upper()
    if os.getenv(f"MINIO_{env}_ACCESS_KEY") and os.getenv(f"MINIO_{env}_SECRET_KEY"):
        return {"url": MINIO_URL,
                "accessKey": os.environ[f"MINIO_{env}_ACCESS_KEY"],
                "secretKey": os.environ[f"MINIO_{env}_SECRET_KEY"]}
    fichier = CONFIG_DIR / f"credentials_{profil}.json"
    if not fichier.exists():
        raise FileNotFoundError(f"Credentials introuvables pour '{profil}' : créez {fichier} "
                                f"(modèle : {fichier.name.replace('.json', '.example.json')})")
    with open(fichier, encoding="utf-8") as f:
        cred = json.load(f)
    cred.setdefault("url", MINIO_URL)
    return cred


def get_s3_client(profil, access_key=None, secret_key=None):
    """Client S3 pour une identité. access_key/secret_key permettent de forcer des credentials (tests)."""
    cred = get_credentials(profil) if access_key is None else {"url": MINIO_URL}
    return boto3.client(
        "s3",
        endpoint_url=cred["url"],
        aws_access_key_id=access_key or cred["accessKey"],
        aws_secret_access_key=secret_key or cred["secretKey"],
        # échec rapide si MinIO est injoignable (au lieu de longues tentatives)
        config=Config(connect_timeout=3, read_timeout=10, retries={"max_attempts": 2}),
    )
