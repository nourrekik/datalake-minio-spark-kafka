"""Mission 12 — Simulateur d'application de caisse.

Génère progressivement des événements de vente. Chaque événement est AUTONOME : il contient tout ce
qu'il faut pour être exploité seul (identifiants, horodatage, quantité, prix, ville, catégorie).

    python src/streaming/caisse.py --rythme 1 --nombre 5      # affiche 5 ventes, une par seconde
    python src/streaming/caisse.py --taux-invalides 0.2        # 20 % d'événements hors schéma (mission 21)
"""
import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import CONFIG_DIR  # noqa: E402

with open(CONFIG_DIR / "referentiel_caisse.json", encoding="utf-8") as f:
    REF = json.load(f)

SCHEMA_VERSION = 1


def generer_vente(taux_invalides=0.0):
    """Construit un événement de vente (dict). Avec une probabilité taux_invalides, l'événement
    est volontairement non conforme au schéma, pour tester la robustesse du traitement."""
    id_magasin = random.choice(list(REF["magasins"]))
    id_produit = random.choice(list(REF["produits"]))
    nom_produit, categorie, prix = REF["produits"][id_produit]
    maintenant = datetime.now()
    vente = {
        "schema_version": SCHEMA_VERSION,
        "id_vente": f"S-{uuid.uuid4().hex[:10].upper()}",
        "horodatage": maintenant.isoformat(timespec="milliseconds"),
        "id_caisse": f"{id_magasin}-K{random.randint(1, 3)}",
        "id_magasin": id_magasin,
        "ville": REF["magasins"][id_magasin],
        "id_client": random.choice(REF["clients"]),
        "id_produit": id_produit,
        "nom_produit": nom_produit,
        "categorie": categorie,
        "quantite": random.choices([1, 2, 3, 4, 5], weights=[45, 25, 15, 10, 5])[0],
        "prix_unitaire": prix,
    }
    if random.random() < taux_invalides:
        defaut = random.choice(["quantite_texte", "champ_manquant", "prix_negatif"])
        if defaut == "quantite_texte":
            vente["quantite"] = "trois"
        elif defaut == "champ_manquant":
            del vente["id_magasin"], vente["ville"]
        else:
            vente["prix_unitaire"] = -10
    return vente


def flux(rythme=1.0, nombre=None, taux_invalides=0.0):
    """Générateur infini (ou limité à `nombre`) d'événements, un toutes les `rythme` secondes."""
    i = 0
    while nombre is None or i < nombre:
        yield generer_vente(taux_invalides)
        i += 1
        time.sleep(rythme)


def arguments():
    p = argparse.ArgumentParser(description="Simulateur de caisse")
    p.add_argument("--rythme", type=float, default=1.0, help="secondes entre deux ventes (défaut 1)")
    p.add_argument("--nombre", type=int, default=None, help="nombre de ventes (défaut : infini)")
    p.add_argument("--taux-invalides", type=float, default=0.0, help="part d'événements hors schéma (0 à 1)")
    return p.parse_args()


if __name__ == "__main__":
    a = arguments()
    try:
        for v in flux(a.rythme, a.nombre, a.taux_invalides):
            print(json.dumps(v, ensure_ascii=False))
    except KeyboardInterrupt:
        print("\nCaisse arrêtée.")
