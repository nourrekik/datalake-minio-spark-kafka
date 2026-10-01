"""Outils Spark du pipeline batch (repris de la phase 1, adaptés à l'ingestion automatisée).

Changements par rapport à la phase 1 :
  - connexion avec l'identité technique pipeline-user (profil "pipeline") ;
  - lecture de TOUS les fichiers d'une source dans RAW (dépôt initial + lots ingérés
    dans raw/<source>/ingest_date=AAAA-MM-JJ/), et non plus d'un seul fichier ;
  - chaque ligne garde la trace de son fichier d'origine et de sa date d'ingestion.
"""
import glob
import io
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import BUCKET, get_credentials, get_s3_client  # noqa: E402

PROFIL = "pipeline"
RAW = f"s3a://{BUCKET}/raw"
DATE_INITIALE = "1970-01-01"  # fichiers déposés manuellement en phase 1 (sans ingest_date)


def _hadoop_version():
    jars = glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars", "hadoop-client-api-*.jar"))
    return re.search(r"hadoop-client-api-(.+)\.jar", jars[0]).group(1)


def create_spark(app_name="tp2-batch"):
    cred = get_credentials(PROFIL)
    spark = (SparkSession.builder.appName(app_name)
             .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{_hadoop_version()}")
             .config("spark.hadoop.fs.s3a.endpoint", cred["url"])
             .config("spark.hadoop.fs.s3a.access.key", cred["accessKey"])
             .config("spark.hadoop.fs.s3a.secret.key", cred["secretKey"])
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.sql.session.timeZone", "Europe/Paris")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _avec_origine(df):
    """Ajoute le fichier d'origine et la date d'ingestion (extraite du préfixe ingest_date=...)."""
    df = df.withColumn("_fichier_source", F.input_file_name())
    date = F.regexp_extract("_fichier_source", r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)
    return df.withColumn("_ingest_date", F.when(date == "", F.lit(DATE_INITIALE)).otherwise(date))


# ---------------------------------------------------------------------------
# Lecture de RAW : une méthode adaptée à chaque format, sur tous les fichiers de la source.
# ---------------------------------------------------------------------------

def read_ventes(spark):
    df = (spark.read.option("header", True).option("inferSchema", False)
          .option("recursiveFileLookup", True).option("pathGlobFilter", "*.csv")
          .csv(f"{RAW}/ventes/"))
    df = df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])  # BOM UTF-8
    return _avec_origine(df)


def read_clients(spark):
    return _avec_origine(spark.read.option("multiLine", True).option("recursiveFileLookup", True)
                         .option("pathGlobFilter", "*.json").json(f"{RAW}/clients/"))


def read_produits(spark):
    return _avec_origine(spark.read.format("xml").option("rowTag", "produit").option("inferSchema", False)
                         .option("recursiveFileLookup", True).option("pathGlobFilter", "*.xml")
                         .load(f"{RAW}/produits/"))


def read_magasins(spark):
    # Excel : non lu par Spark -> chaque objet .xlsx est lu via l'API S3 puis par pandas
    s3 = get_s3_client(PROFIL)
    frames = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="raw/magasins/"):
        for obj in page.get("Contents", []):
            if not obj["Key"].lower().endswith(".xlsx"):
                continue
            contenu = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
            pdf = pd.read_excel(io.BytesIO(contenu), dtype=str)
            pdf["_fichier_source"] = f"s3a://{BUCKET}/{obj['Key']}"
            m = re.search(r"ingest_date=(\d{4}-\d{2}-\d{2})", obj["Key"])
            pdf["_ingest_date"] = m.group(1) if m else DATE_INITIALE
            frames.append(pdf)
    pdf = pd.concat(frames, ignore_index=True)
    return spark.createDataFrame(pdf.astype(object).where(pdf.notna(), None))


def read_all_raw(spark):
    return {"ventes": read_ventes(spark), "clients": read_clients(spark),
            "produits": read_produits(spark), "magasins": read_magasins(spark)}


# ---------------------------------------------------------------------------
# Écriture Parquet dans MinIO SANS permission de suppression (inchangé depuis la phase 1) :
# écriture locale par Spark, puis PutObject sous des clés fixes -> idempotent.
# ---------------------------------------------------------------------------

def write_parquet(df, key_prefix, nb_fichiers=1):
    s3 = get_s3_client(PROFIL)
    dossier = tempfile.mkdtemp(prefix="tp2_")
    try:
        chemin_local = os.path.join(dossier, "out")
        df.coalesce(nb_fichiers).write.mode("overwrite").parquet("file://" + chemin_local)
        parts = sorted(f for f in os.listdir(chemin_local) if f.startswith("part-") and f.endswith(".parquet"))
        for i, nom in enumerate(parts):
            with open(os.path.join(chemin_local, nom), "rb") as f:
                s3.put_object(Bucket=BUCKET, Key=f"{key_prefix}/part-{i:05d}.snappy.parquet", Body=f.read())
        s3.put_object(Bucket=BUCKET, Key=f"{key_prefix}/_SUCCESS", Body=b"")
        print(f"   -> s3a://{BUCKET}/{key_prefix}/  ({len(parts)} fichier(s) part + _SUCCESS)")
    finally:
        shutil.rmtree(dossier, ignore_errors=True)


def read_parquet(spark, key_prefix):
    return spark.read.parquet(f"s3a://{BUCKET}/{key_prefix}")
