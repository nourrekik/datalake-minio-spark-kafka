"""Mission 3 — Vérifier la séparation ingestion / traitement.

Chaque identité tente les mêmes opérations ; on compare au résultat attendu.
On teste aussi volontairement des credentials inadaptés ou faux.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from botocore.exceptions import ClientError, EndpointConnectionError  # noqa: E402
from common.config import BUCKET, get_s3_client  # noqa: E402


def essai(s3, operation):
    try:
        operation(s3)
        return "AUTORISÉ", ""
    except ClientError as e:
        err = e.response["Error"]
        return "REFUSÉ", f'{err.get("Code")} — {err.get("Message", "")}'
    except EndpointConnectionError as e:
        return "INJOIGNABLE", str(e)


OPERATIONS = {
    "Écrire dans RAW":       lambda s3: s3.put_object(Bucket=BUCKET, Key="raw/_tests/identite.txt", Body=b"test"),
    "Lire dans RAW":         lambda s3: s3.get_object(Bucket=BUCKET, Key="raw/ventes/ventes.csv")["Body"].read(),
    "Écrire dans PROCESSED": lambda s3: s3.put_object(Bucket=BUCKET, Key="processed/_tests/identite.txt", Body=b"test"),
    "Écrire dans CURATED":   lambda s3: s3.put_object(Bucket=BUCKET, Key="curated/_tests/identite.txt", Body=b"test"),
    "Supprimer dans RAW":    lambda s3: s3.delete_object(Bucket=BUCKET, Key="raw/ventes/ventes.csv"),
}

ATTENDU = {
    "ingestion": {"Écrire dans RAW": "AUTORISÉ", "Lire dans RAW": "AUTORISÉ", "Écrire dans PROCESSED": "REFUSÉ",
                  "Écrire dans CURATED": "REFUSÉ", "Supprimer dans RAW": "REFUSÉ"},
    "pipeline":  {"Écrire dans RAW": "REFUSÉ", "Lire dans RAW": "AUTORISÉ", "Écrire dans PROCESSED": "AUTORISÉ",
                  "Écrire dans CURATED": "AUTORISÉ", "Supprimer dans RAW": "REFUSÉ"},
}

total_ok = 0
for profil in ("ingestion", "pipeline"):
    print(f"\n=== Identité : {profil}-user ===")
    s3 = get_s3_client(profil)
    for nom, op in OPERATIONS.items():
        res, detail = essai(s3, op)
        ok = res == ATTENDU[profil][nom]
        total_ok += ok
        print(f"  {nom:<24} -> {res:<10} (attendu {ATTENDU[profil][nom]:<9}) {'OK' if ok else '!! INATTENDU'}")
        if res == "REFUSÉ":
            print(f"  {'':<24}    motif : {detail}")

print("\n=== Credentials inadaptés : la clé d'ingestion utilisée pour le pipeline ===")
res, detail = essai(get_s3_client("ingestion"),
                    lambda s3: s3.put_object(Bucket=BUCKET, Key="curated/_tests/mauvaise_identite.txt", Body=b"x"))
print(f"  ingestion-user écrit dans CURATED -> {res}\n    motif : {detail}")

print("\n=== Credentials faux : bonne Access Key, mauvaise Secret Key ===")
from common.config import get_credentials  # noqa: E402
vraie = get_credentials("ingestion")["accessKey"]
res, detail = essai(get_s3_client("ingestion", access_key=vraie, secret_key="mauvais-secret"),
                    lambda s3: s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/", MaxKeys=1))
print(f"  -> {res}\n    motif : {detail}")

print("\n=== Credentials faux : Access Key inexistante ===")
res, detail = essai(get_s3_client("ingestion", access_key="CLE_INEXISTANTE", secret_key="x"),
                    lambda s3: s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/", MaxKeys=1))
print(f"  -> {res}\n    motif : {detail}")

print(f"\nBilan : {total_ok}/10 tests conformes à la matrice des droits.")
