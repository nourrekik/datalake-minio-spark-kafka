"""Outils partagés : session Spark connectée à MinIO et lecture des sources RAW."""
import glob
import io
import os
import re

import pandas as pd
import pyspark
from pyspark.sql import SparkSession

from config import BUCKET, get_credentials, get_s3_client

RAW = f"s3a://{BUCKET}/raw"


def _hadoop_version():
    """Version de Hadoop embarquée dans PySpark -> version du connecteur hadoop-aws."""
    jars = glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars", "hadoop-client-api-*.jar"))
    return re.search(r"hadoop-client-api-(.+)\.jar", jars[0]).group(1)


def create_spark(app_name="tp2"):
    cred = get_credentials()
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


# ---------------------------------------------------------------------------
# Lecture des sources RAW : une méthode adaptée à chaque format.
# Toutes les colonnes CSV sont lues en texte pour contrôler nous-mêmes le typage.
# ---------------------------------------------------------------------------

def read_ventes(spark):
    # CSV : lecteur natif Spark, tout en string (pas d'inférence) pour détecter les valeurs invalides
    df = spark.read.option("header", True).option("inferSchema", False).csv(f"{RAW}/ventes/ventes.csv")
    # Le fichier contient un BOM UTF-8 : on nettoie les noms de colonnes par sécurité
    return df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])


def read_clients(spark):
    # JSON : lecteur natif Spark ; le fichier est un tableau sur plusieurs lignes -> multiLine
    return spark.read.option("multiLine", True).json(f"{RAW}/clients/clients.json")


def read_produits(spark):
    # XML : lecteur natif depuis Spark 4 ; chaque <produit> devient une ligne
    return (spark.read.format("xml")
            .option("rowTag", "produit")
            .option("inferSchema", False)
            .load(f"{RAW}/produits/produits.xml"))


def read_magasins(spark):
    # Excel : non supporté nativement par Spark -> lecture de l'objet via l'API S3,
    # parsing avec pandas/openpyxl, puis conversion en DataFrame Spark
    s3 = get_s3_client()
    contenu = s3.get_object(Bucket=BUCKET, Key="raw/magasins/magasins.xlsx")["Body"].read()
    pdf = pd.read_excel(io.BytesIO(contenu), dtype=str)
    return spark.createDataFrame(pdf.where(pdf.notna(), None))


def read_all_raw(spark):
    return {
        "ventes": read_ventes(spark),
        "clients": read_clients(spark),
        "produits": read_produits(spark),
        "magasins": read_magasins(spark),
    }


# ---------------------------------------------------------------------------
# Écriture Parquet dans MinIO SANS permission de suppression.
#
# L'écriture Spark classique directement en s3a:// (FileOutputCommitter) crée un
# dossier _temporary/ puis le SUPPRIME, et mode("overwrite") supprime l'ancien
# résultat : impossible ici, car la policy interdit s3:DeleteObject.
#
# Stratégie : Spark écrit le jeu de données dans un dossier local temporaire,
# puis chaque fichier est envoyé dans MinIO avec PutObject sous un NOM FIXE
# (part-00000.snappy.parquet + _SUCCESS). À la réexécution, les mêmes clés sont
# simplement écrasées : le résultat est remplacé, sans doublon et sans suppression.
# ---------------------------------------------------------------------------
import shutil
import tempfile


def write_parquet(df, key_prefix, nb_fichiers=1):
    s3 = get_s3_client()
    dossier = tempfile.mkdtemp(prefix="tp2_")
    try:
        chemin_local = os.path.join(dossier, "out")
        df.coalesce(nb_fichiers).write.mode("overwrite").parquet("file://" + chemin_local)

        parts = sorted(f for f in os.listdir(chemin_local) if f.startswith("part-") and f.endswith(".parquet"))
        for i, nom in enumerate(parts):
            cle = f"{key_prefix}/part-{i:05d}.snappy.parquet"
            with open(os.path.join(chemin_local, nom), "rb") as f:
                s3.put_object(Bucket=BUCKET, Key=cle, Body=f.read())
        # _SUCCESS est écrit en dernier : il signale que le jeu de données est complet
        s3.put_object(Bucket=BUCKET, Key=f"{key_prefix}/_SUCCESS", Body=b"")
        print(f"   -> s3a://{BUCKET}/{key_prefix}/  ({len(parts)} fichier(s) part + _SUCCESS)")
    finally:
        shutil.rmtree(dossier, ignore_errors=True)


def read_parquet(spark, key_prefix):
    return spark.read.parquet(f"s3a://{BUCKET}/{key_prefix}")
