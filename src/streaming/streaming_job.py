"""Missions 15 à 19 — Traitement streaming des ventes : Kafka -> RAW -> PROCESSED -> CURATED.

Spark Structured Streaming lit le topic « ventes » par MICRO-BATCHS (un toutes les N secondes).
Pour chaque micro-batch (fonction traiter_micro_batch) :
  1. RAW       : les messages bruts, tels que reçus, + métadonnées Kafka (partition, offset)
                 -> raw/ventes_stream/date=AAAA-MM-JJ/heure=HH/batch_000123.jsonl   (identité ingestion-user)
  2. contrôle  : application du SCHÉMA attendu ; les événements non conformes sont isolés
                 -> raw/_rejets_stream/date=.../batch_000123.jsonl                   (identité ingestion-user)
  3. PROCESSED : ventes valides, typées, montant calculé, ville standardisée (règles du batch réutilisées)
                 -> processed/ventes_stream/date=.../batch=000123/                   (identité pipeline-user)
  4. CURATED   : indicateurs recalculés sur tout l'historique streaming
                 -> curated/streaming/ca_par_ville/ et curated/streaming/ca_par_categorie/

Reprise : le CHECKPOINT (dossier local checkpoints/) mémorise les offsets Kafka déjà traités.
Après un arrêt, Spark repart exactement après le dernier micro-batch validé. Les fichiers sont
nommés par numéro de micro-batch : si un batch est rejoué, il réécrit les mêmes clés (pas de doublon).

    python src/streaming/streaming_job.py                 # micro-batch toutes les 20 s
    python src/streaming/streaming_job.py --intervalle 10
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

SRC = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SRC)); sys.path.insert(0, str(SRC / "batch"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,  # noqa: E402
                               StructType)

from common.config import BUCKET, PROJECT_ROOT, SETTINGS, get_credentials, get_s3_client  # noqa: E402
import spark_utils  # noqa: E402  (pipeline batch : écriture Parquet sans suppression)
from pipeline import agreger, ville_standard  # noqa: E402  (règles métier du batch réutilisées)

KAFKA = SETTINGS["kafka"]
CHECKPOINT = PROJECT_ROOT / "checkpoints" / "ventes_stream"

# Schéma attendu d'un événement de vente (contrat entre la caisse et la plateforme)
SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("id_vente", StringType()),
    StructField("horodatage", StringType()),
    StructField("id_caisse", StringType()),
    StructField("id_magasin", StringType()),
    StructField("ville", StringType()),
    StructField("id_client", StringType()),
    StructField("id_produit", StringType()),
    StructField("nom_produit", StringType()),
    StructField("categorie", StringType()),
    StructField("quantite", IntegerType()),
    StructField("prix_unitaire", DoubleType()),
])
OBLIGATOIRES = ["id_vente", "horodatage", "id_magasin", "ville", "id_produit", "categorie",
                "quantite", "prix_unitaire"]


def create_spark():
    cred = get_credentials("pipeline")
    hadoop = spark_utils._hadoop_version()
    import pyspark
    spark_version = pyspark.__version__
    spark = (SparkSession.builder.appName("tp2-streaming")
             .config("spark.jars.packages",
                     f"org.apache.hadoop:hadoop-aws:{hadoop},"
                     f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}")
             .config("spark.hadoop.fs.s3a.endpoint", cred["url"])
             .config("spark.hadoop.fs.s3a.access.key", cred["accessKey"])
             .config("spark.hadoop.fs.s3a.secret.key", cred["secretKey"])
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.sql.session.timeZone", "Europe/Paris")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def put_jsonl(s3, key, rows):
    corps = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in rows).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=corps, ContentType="application/x-ndjson")


def traiter_micro_batch(df, batch_id):
    """Appelée par Spark pour chaque micro-batch (foreachBatch). df = messages Kafka du batch."""
    nb = df.count()
    if nb == 0:
        return
    spark = df.sparkSession
    maintenant = datetime.now()
    date, heure = f"{maintenant:%Y-%m-%d}", f"{maintenant:%H}"
    s3_raw = get_s3_client("ingestion")     # alimente RAW
    s3_pipe = get_s3_client("pipeline")     # transforme

    # ---- 1. RAW : message brut tel que reçu + traçabilité Kafka
    brut = (df.select(F.col("value").cast("string").alias("message"), "partition", "offset",
                      F.col("timestamp").cast("string").alias("kafka_timestamp")))
    lignes_brutes = [r.asDict() for r in brut.collect()]
    cle_raw = f"raw/ventes_stream/date={date}/heure={heure}/batch_{batch_id:06d}.jsonl"
    put_jsonl(s3_raw, cle_raw, lignes_brutes)

    # ---- 2. Contrôle du schéma
    parse = brut.withColumn("v", F.from_json("message", SCHEMA)).select("message", "partition", "offset", "v.*")
    conforme = F.lit(True)
    for c in OBLIGATOIRES:
        conforme = conforme & F.col(c).isNotNull()
    conforme = conforme & (F.col("quantite") > 0) & (F.col("prix_unitaire") > 0)
    parse = parse.withColumn("conforme", F.coalesce(conforme, F.lit(False)))
    rejets = parse.filter(~F.col("conforme"))
    nb_rejets = rejets.count()
    if nb_rejets:
        put_jsonl(s3_raw, f"raw/_rejets_stream/date={date}/batch_{batch_id:06d}.jsonl",
                  [r.asDict() for r in rejets.select("message", "partition", "offset").collect()])

    # ---- 3. PROCESSED : règles métier du pipeline batch
    valides = (parse.filter("conforme")
               .withColumn("ville", ville_standard("ville"))
               .withColumn("horodatage", F.to_timestamp("horodatage"))
               .withColumn("date_vente", F.to_date("horodatage"))
               .withColumn("montant", F.round(F.col("quantite") * F.col("prix_unitaire"), 2))
               .withColumn("micro_batch", F.lit(batch_id))
               .drop("message", "conforme"))
    nb_valides = nb - nb_rejets
    if nb_valides:
        spark_utils.write_parquet(valides, f"processed/ventes_stream/date={date}/batch={batch_id:06d}")

    # ---- 4. CURATED : indicateurs sur tout l'historique streaming (repart de PROCESSED)
    historique = spark_utils.read_parquet(spark, "processed/ventes_stream")
    ca_ville = agreger(historique, "ville")
    ca_cat = agreger(historique, "categorie")
    spark_utils.write_parquet(ca_ville, "curated/streaming/ca_par_ville")
    spark_utils.write_parquet(ca_cat, "curated/streaming/ca_par_categorie")

    print(f"\n===== Micro-batch {batch_id} — {maintenant:%H:%M:%S} — {nb} message(s) : "
          f"{nb_valides} valide(s), {nb_rejets} rejeté(s) =====")
    offs = df.groupBy("partition").agg(F.min("offset").alias("min"), F.max("offset").alias("max")).orderBy("partition")
    print("Offsets traités : " + ", ".join(f"p{r['partition']}:{r['min']}→{r['max']}" for r in offs.collect()))
    print(f"RAW : {cle_raw}")
    print(f"CA par ville (cumul streaming, {historique.count()} ventes) :")
    ca_ville.show(truncate=False)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--intervalle", type=int, default=20, help="secondes entre deux micro-batchs")
    a = p.parse_args()

    spark = create_spark()
    flux = (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA["bootstrap_servers"])
            .option("subscribe", KAFKA["topic"])
            .option("startingOffsets", "earliest")   # 1er démarrage seulement ; ensuite : le checkpoint
            .option("failOnDataLoss", "false")
            .load())

    CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    requete = (flux.writeStream
               .foreachBatch(traiter_micro_batch)
               .option("checkpointLocation", str(CHECKPOINT))
               .trigger(processingTime=f"{a.intervalle} seconds")
               .start())
    print(f"Streaming démarré : topic '{KAFKA['topic']}', micro-batch toutes les {a.intervalle} s, "
          f"checkpoint {CHECKPOINT}. Ctrl+C pour arrêter.")
    try:
        requete.awaitTermination()
    except KeyboardInterrupt:
        print("\nArrêt demandé : fin du micro-batch en cours puis arrêt propre...")
        requete.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
