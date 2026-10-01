"""Étape 5 — Tester réellement les permissions de l'utilisateur du pipeline."""
from botocore.exceptions import ClientError

from config import BUCKET, get_s3_client

s3 = get_s3_client()


def essai(description, action, attendu):
    try:
        action()
        resultat = "AUTORISÉE"
    except ClientError as e:
        code = e.response["Error"]["Code"]
        resultat = "REFUSÉE" if code in ("AccessDenied", "403") else f"ERREUR ({code})"
    statut = "OK" if resultat == attendu else "!! INATTENDU"
    print(f"{description:<32} -> {resultat:<10} (attendu : {attendu}) {statut}")


print(f"--- Test des permissions sur le bucket {BUCKET} ---\n")

essai("Lister le bucket",
      lambda: s3.list_objects_v2(Bucket=BUCKET, MaxKeys=1), "AUTORISÉE")
essai("Lecture dans RAW",
      lambda: s3.get_object(Bucket=BUCKET, Key="raw/ventes/ventes.csv")["Body"].read(), "AUTORISÉE")
essai("Écriture dans RAW",
      lambda: s3.put_object(Bucket=BUCKET, Key="raw/_tests/test.txt", Body=b"test"), "REFUSÉE")
essai("Écriture dans PROCESSED",
      lambda: s3.put_object(Bucket=BUCKET, Key="processed/_tests/test.txt", Body=b"test"), "AUTORISÉE")
essai("Écriture dans CURATED",
      lambda: s3.put_object(Bucket=BUCKET, Key="curated/_tests/test.txt", Body=b"test"), "AUTORISÉE")
essai("Suppression dans RAW",
      lambda: s3.delete_object(Bucket=BUCKET, Key="raw/ventes/ventes.csv"), "REFUSÉE")
essai("Suppression dans PROCESSED",
      lambda: s3.delete_object(Bucket=BUCKET, Key="processed/_tests/test.txt"), "REFUSÉE")
