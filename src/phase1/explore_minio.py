"""Étape 6 — Explorer le Data Lake MinIO avec Python (API S3, aucun chemin local)."""
from collections import defaultdict

from config import BUCKET, get_s3_client

s3 = get_s3_client()

# 1. Lister tous les objets du bucket (avec pagination)
objets = []
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
    objets.extend(page.get("Contents", []))

print(f"=== Bucket '{BUCKET}' : {len(objets)} objet(s) ===\n")

# 2. Regrouper par zone (premier niveau de préfixe) puis par source
zones = defaultdict(lambda: defaultdict(list))
for obj in objets:
    parties = obj["Key"].split("/")
    zone = parties[0]
    source = parties[1] if len(parties) > 2 else "(racine)"
    zones[zone][source].append(obj)

for zone in ("raw", "processed", "curated"):
    sources = zones.get(zone, {})
    total = sum(len(v) for v in sources.values())
    print(f"[{zone.upper()}] {total} objet(s)")
    for source, liste in sorted(sources.items()):
        taille = sum(o["Size"] for o in liste)
        print(f"   {source:<20} {len(liste):>3} objet(s)  {taille / 1024:8.1f} Ko")
    print()

# 3. Informations détaillées sur quelques objets (métadonnées sans télécharger)
print("=== Métadonnées de quelques objets RAW ===")
for key in ["raw/ventes/ventes.csv", "raw/produits/produits.xml",
            "raw/magasins/magasins.xlsx", "raw/images_produits/P001.jpg"]:
    head = s3.head_object(Bucket=BUCKET, Key=key)
    print(f"- {key}")
    print(f"    taille        : {head['ContentLength']} octets")
    print(f"    type          : {head.get('ContentType')}")
    print(f"    modifié le    : {head['LastModified']}")
    print(f"    ETag          : {head['ETag']}")

# 4. Vérifier l'accès en lecture aux données RAW
print("\n=== Aperçu de raw/ventes/ventes.csv ===")
contenu = s3.get_object(Bucket=BUCKET, Key="raw/ventes/ventes.csv")["Body"].read().decode("utf-8")
for ligne in contenu.splitlines()[:5]:
    print("  " + ligne)
print(f"  ... ({len(contenu.splitlines()) - 1} lignes de données)")
