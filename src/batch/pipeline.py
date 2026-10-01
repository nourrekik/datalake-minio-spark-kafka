"""Pipeline complet TP2 : RAW -> PROCESSED -> CURATED.

    RAW  : lecture des sources d'origine (jamais modifiées)
      -> contrôles, nettoyage, typage, standardisation
    PROCESSED : ventes, clients, produits, magasins (Parquet) + ventes_enrichies
      -> jointures déjà faites, agrégations
    CURATED : ca_par_ville, ca_par_categorie (Parquet)

    raw/images_produits/ : conservées dans RAW uniquement, non traitées.

Le pipeline est réexécutable (idempotent) : voir write_parquet() dans spark_utils.py.

Phase 2 : RAW peut désormais contenir plusieurs versions d'une même source (un fichier par lot
ingéré). Pour chaque identifiant, on conserve la version la plus récemment ingérée.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from spark_utils import create_spark, read_all_raw, read_parquet, write_parquet

NON_RENSEIGNE = "Non renseigné"


def derniere_version(df, cle):
    """Garde, pour chaque identifiant, la ligne issue du lot ingéré le plus récemment.
    Remplace le simple dropDuplicates : si un fichier corrige une donnée, la correction l'emporte."""
    w = Window.partitionBy(F.upper(F.trim(F.col(cle)))).orderBy(F.desc("_ingest_date"), F.desc("_fichier_source"))
    return df.withColumn("_rang", F.row_number().over(w)).filter("_rang = 1").drop("_rang")


def ville_standard(col):
    """'  PARIS ' -> 'Paris' ; chaîne vide -> null."""
    v = F.initcap(F.trim(F.col(col)))
    return F.when(v == "", None).otherwise(v)


# ============================================================================
# 1. RAW -> PROCESSED : nettoyage de chaque source
# ============================================================================

def nettoyer_ventes(df):
    q = F.col("quantite").try_cast("double")
    df = (df
          # doublons (V010 en double, ou même vente présente dans plusieurs lots) : dernière version
          .transform(lambda d: derniere_version(d, "id_vente"))
          .select(
              F.trim("id_vente").alias("id_vente"),
              F.try_to_date(F.trim("date_vente"), "yyyy-MM-dd").alias("date_vente"),
              F.col("date_vente").alias("date_vente_source"),
              F.upper(F.trim("id_client")).alias("id_client"),
              F.upper(F.trim("id_produit")).alias("id_produit"),
              F.upper(F.trim("id_magasin")).alias("id_magasin"),
              # "3.0" -> 3 ; valeur non numérique ou non entière -> null
              F.when(q == F.floor(q), q.cast("int")).alias("quantite"),
              F.col("_ingest_date").alias("ingest_date"),  # traçabilité : lot d'origine
          ))
    # On ne supprime pas : chaque anomalie est tracée dans une colonne dédiée
    anomalies = F.concat_ws(", ",
        F.when(F.col("date_vente_source").isNull(), F.lit("date absente")),
        F.when(F.col("date_vente").isNull() & F.col("date_vente_source").isNotNull(), F.lit("date invalide")),
        F.when(F.col("quantite").isNull(), F.lit("quantite absente")),
        F.when(F.col("quantite") == 0, F.lit("quantite nulle")),
        F.when(F.col("quantite") < 0, F.lit("quantite negative")),
    )
    return df.withColumn("anomalies", F.when(anomalies != "", anomalies))


def nettoyer_clients(df):
    return (derniere_version(df, "id_client")
            .select(
                F.upper(F.trim("id_client")).alias("id_client"),
                F.trim("nom").alias("nom_client"),
                ville_standard("ville").alias("ville_client"),
                F.coalesce(F.initcap(F.trim("segment")), F.lit(NON_RENSEIGNE)).alias("segment"),
            )
            .dropDuplicates(["id_client"]))


def nettoyer_produits(df):
    cat = F.trim("categorie")
    return (derniere_version(df, "id_produit")
            .select(
                F.upper(F.trim("id_produit")).alias("id_produit"),
                F.trim("nom_produit").alias("nom_produit"),
                F.when(cat.isNull() | (cat == ""), F.lit("Non renseignée")).otherwise(cat).alias("categorie"),
                F.col("prix_unitaire").try_cast("double").alias("prix_unitaire"),
            )
            .dropDuplicates(["id_produit"]))


def nettoyer_magasins(df):
    return (derniere_version(df, "id_magasin")
            .select(
                F.upper(F.trim("id_magasin")).alias("id_magasin"),
                F.trim("nom_magasin").alias("nom_magasin"),
                ville_standard("ville").alias("ville"),
                F.trim("region").alias("region"),
            )
            .dropDuplicates(["id_magasin"]))


# ============================================================================
# 2. Enrichissement des ventes (à partir de PROCESSED)
# ============================================================================

def enrichir(ventes, clients, produits, magasins):
    df = (ventes
          .join(clients, "id_client", "left")
          .join(produits, "id_produit", "left")
          .join(magasins, "id_magasin", "left"))

    # Références inexistantes : tracées dans la colonne anomalies
    ref = F.concat_ws(", ",
        F.col("anomalies"),
        F.when(F.col("nom_client").isNull(), F.lit("client inconnu")),
        F.when(F.col("nom_produit").isNull(), F.lit("produit inconnu")),
        F.when(F.col("nom_magasin").isNull(), F.lit("magasin inconnu")),
    )
    df = df.withColumn("anomalies", F.when(ref != "", ref))

    # Client ou magasin inconnu : on garde la vente (le CA est réel), on l'étiquette
    df = (df
          .withColumn("nom_client", F.coalesce("nom_client", F.lit("Client inconnu")))
          .withColumn("segment", F.coalesce("segment", F.lit("Inconnu")))
          .withColumn("ville", F.coalesce("ville", F.lit("Inconnue")))
          .withColumn("region", F.coalesce("region", F.lit("Inconnue")))
          .withColumn("nom_magasin", F.coalesce("nom_magasin", F.lit("Magasin inconnu")))
          .withColumn("montant", F.round(F.col("quantite") * F.col("prix_unitaire"), 2)))

    # Une vente n'est exploitable que si la quantité est > 0 et le prix connu
    valides = df.filter((F.col("quantite") > 0) & F.col("prix_unitaire").isNotNull())
    rejetees = df.subtract(valides)
    return valides.select(
        "id_vente", "date_vente", "id_client", "nom_client", "segment", "ville_client",
        "id_produit", "nom_produit", "categorie", "id_magasin", "nom_magasin", "ville", "region",
        "quantite", "prix_unitaire", "montant", "anomalies",
    ), rejetees


# ============================================================================
# 3. PROCESSED -> CURATED : agrégations métier
# ============================================================================

def agreger(df, cle):
    return (df.groupBy(cle)
            .agg(F.count("id_vente").alias("nb_ventes"),
                 F.sum("quantite").alias("quantite_totale"),
                 F.round(F.sum("montant"), 2).alias("chiffre_affaires"))
            .orderBy(F.desc("chiffre_affaires")))


def main():
    spark = create_spark("tp2-pipeline")

    print("\n[1/4] Lecture de RAW (tous les lots ingérés)")
    raw = read_all_raw(spark)
    for nom, df in raw.items():
        nb_fichiers = df.select("_fichier_source").distinct().count()
        print(f"   {nom:<9} : {df.count()} ligne(s) brutes issues de {nb_fichiers} fichier(s)")

    print("\n[2/4] RAW -> PROCESSED (nettoyage + Parquet)")
    propres = {
        "ventes": nettoyer_ventes(raw["ventes"]),
        "clients": nettoyer_clients(raw["clients"]),
        "produits": nettoyer_produits(raw["produits"]),
        "magasins": nettoyer_magasins(raw["magasins"]),
    }
    for nom, df in propres.items():
        print(f" {nom} : {df.count()} ligne(s) après nettoyage")
        write_parquet(df, f"processed/{nom}")

    print("\n[3/4] Enrichissement des ventes (lecture depuis PROCESSED)")
    p = {nom: read_parquet(spark, f"processed/{nom}") for nom in propres}
    enrichies, rejetees = enrichir(p["ventes"], p["clients"], p["produits"], p["magasins"])
    print(f" ventes exploitables : {enrichies.count()}  |  écartées : {rejetees.count()}")
    rejetees.select("id_vente", "id_produit", "quantite", "prix_unitaire", "anomalies").show(truncate=False)
    write_parquet(enrichies, "processed/ventes_enrichies")

    print("\n[4/4] PROCESSED -> CURATED (agrégations)")
    ventes_enrichies = read_parquet(spark, "processed/ventes_enrichies")  # on repart de PROCESSED
    ca_ville = agreger(ventes_enrichies, "ville")
    ca_categorie = agreger(ventes_enrichies, "categorie")
    write_parquet(ca_ville, "curated/ca_par_ville")
    write_parquet(ca_categorie, "curated/ca_par_categorie")

    print("\n=== Contrôle des résultats CURATED (relus depuis MinIO) ===")
    read_parquet(spark, "curated/ca_par_ville").orderBy(F.desc("chiffre_affaires")).show()
    read_parquet(spark, "curated/ca_par_categorie").orderBy(F.desc("chiffre_affaires")).show()

    print("Pipeline terminé avec succès. (raw/images_produits/ conservées dans RAW uniquement)")
    spark.stop()


if __name__ == "__main__":
    main()
