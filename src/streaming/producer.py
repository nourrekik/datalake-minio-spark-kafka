"""Mission 13 — Producer Kafka : publie les ventes de la caisse dans le topic « ventes ».

    python src/streaming/producer.py --rythme 1              # une vente par seconde, sans fin (Ctrl+C)
    python src/streaming/producer.py --rythme 0.5 --nombre 20
    python src/streaming/producer.py --taux-invalides 0.2    # injecte 20 % d'événements hors schéma

Clé du message = id_magasin : toutes les ventes d'un même magasin vont dans la même partition,
ce qui garantit leur ordre d'arrivée par magasin.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kafka import KafkaProducer  # noqa: E402
from kafka.errors import KafkaError  # noqa: E402

from common.config import SETTINGS  # noqa: E402
from streaming.caisse import arguments, flux  # noqa: E402

KAFKA = SETTINGS["kafka"]


def creer_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA["bootstrap_servers"],
        acks="all",            # le broker confirme l'écriture avant de considérer l'envoi réussi
        retries=5,             # nouvelles tentatives automatiques en cas d'erreur passagère
        linger_ms=10,
        max_block_ms=10000,    # n'attend pas indéfiniment si le broker est injoignable
    )


def main():
    a = arguments()
    try:
        producer = creer_producer()
    except Exception:  # selon la version de kafka-python : NoBrokersAvailable, KafkaConnectionError...
        print(f"!! Kafka injoignable sur {KAFKA['bootstrap_servers']} : le broker est-il démarré ?")
        return 2

    print(f"Envoi vers le topic '{KAFKA['topic']}' (rythme {a.rythme}s, Ctrl+C pour arrêter)")
    envoyes = erreurs = 0
    try:
        for vente in flux(a.rythme, a.nombre, a.taux_invalides):
            try:
                cle = (vente.get("id_magasin") or "").encode("utf-8") or None
                valeur = json.dumps(vente, ensure_ascii=False).encode("utf-8")   # message = JSON en UTF-8
                meta = producer.send(KAFKA["topic"], key=cle, value=valeur).get(timeout=10)
                envoyes += 1
                print(f"  #{envoyes:<4} {vente['id_vente']}  {vente.get('ville', '?'):<9} "
                      f"{vente.get('categorie', '?'):<12} x{vente.get('quantite')}  "
                      f"-> partition {meta.partition}, offset {meta.offset}")
            except KafkaError as e:
                erreurs += 1
                print(f"  !! échec d'envoi de {vente['id_vente']} : {type(e).__name__} {e}")
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print(f"\nProducer arrêté : {envoyes} message(s) envoyé(s), {erreurs} échec(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
