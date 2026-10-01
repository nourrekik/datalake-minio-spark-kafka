"""Étape 7 — Explorer les sources RAW avec PySpark, SANS rien corriger.

Pour chaque source : format, schéma, nombre d'enregistrements, valeurs manquantes,
doublons, et valeurs incohérentes ou suspectes.
"""
from pyspark.sql import functions as F

from spark_utils import create_spark, read_all_raw

spark = create_spark("tp2-exploration")
sources = read_all_raw(spark)
FORMATS = {"ventes": "CSV", "clients": "JSON", "produits": "XML", "magasins": "Excel (XLSX)"}
CLES = {"ventes": "id_vente", "clients": "id_client", "produits": "id_produit", "magasins": "id_magasin"}


def titre(t):
    print("\n" + "=" * 70 + f"\n{t}\n" + "=" * 70)


def manquants(df):
    """Nombre de valeurs nulles OU vides (chaîne vide / espaces) par colonne."""
    exprs = []
    for c, t in df.dtypes:
        cond = F.col(c).isNull()
        if t == "string":
            cond = cond | (F.trim(F.col(c)) == "")
        exprs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(c))
    return df.agg(*exprs).collect()[0].asDict()


# --- 1. Profil général de chaque source --------------------------------------
for nom, df in sources.items():
    titre(f"{nom.upper()}  —  format {FORMATS[nom]}")
    df.printSchema()
    total = df.count()
    cle = CLES[nom]
    print(f"Enregistrements          : {total}")
    print(f"Lignes strictement identiques en double : {total - df.distinct().count()}")
    print(f"Doublons sur la clé {cle:<11}: {total - df.select(cle).distinct().count()}")
    print("Valeurs manquantes (null ou vide) :", {k: v for k, v in manquants(df).items() if v})
    df.show(5, truncate=False)

ventes, clients = sources["ventes"], sources["clients"]
produits, magasins = sources["produits"], sources["magasins"]

# --- 2. Valeurs incohérentes ou suspectes -------------------------------------
titre("ANOMALIES DÉTAILLÉES")

print("\n[Ventes] Dates absentes ou invalides (to_date renvoie null) :")
(ventes.withColumn("date_convertie", F.try_to_date("date_vente", "yyyy-MM-dd"))
       .filter(F.col("date_convertie").isNull())
       .select("id_vente", "date_vente").show(truncate=False))

print("[Ventes] Quantités absentes, nulles, négatives ou non entières :")
q = F.col("quantite").try_cast("double")
(ventes.filter(q.isNull() | (q <= 0) | (q != F.floor(q)))
       .select("id_vente", "quantite").show(truncate=False))

print("[Ventes] Lignes en double (même id_vente) :")
(ventes.groupBy("id_vente").count().filter("count > 1")
       .join(ventes, "id_vente").show(truncate=False))

print("[Ventes] Références inexistantes dans les tables de référence :")
for col, ref, nom in [("id_client", clients, "clients"),
                      ("id_produit", produits, "produits"),
                      ("id_magasin", magasins, "magasins")]:
    orphelins = ventes.join(ref.select(col).distinct(), col, "left_anti")
    print(f"  {col} absents de {nom} : {[r[col] for r in orphelins.select(col).distinct().collect()]}"
          f"  ({orphelins.count()} vente(s))")

print("\n[Clients / Magasins] Villes : différences de casse et espaces superflus")
for nom, df in [("clients", clients), ("magasins", magasins)]:
    (df.select(F.lit(nom).alias("source"), "ville",
               F.length("ville").alias("longueur"),
               (F.col("ville") != F.initcap(F.trim("ville"))).alias("a_standardiser"))
       .distinct().orderBy("ville").show(truncate=False))

print("[Clients] Segments rencontrés :")
clients.groupBy("segment").count().show()

print("[Produits] Catégories rencontrées et prix :")
produits.groupBy("categorie").count().show()
(produits.withColumn("prix", F.col("prix_unitaire").try_cast("double"))
         .filter(F.col("prix").isNull() | (F.col("prix") <= 0))
         .select("id_produit", "prix_unitaire").show())

spark.stop()
