# Data Lake — Phases 2 et 3 : ingestion automatisée et streaming

**Binôme :** Nour REKIK — Bouchra TYAL

| Membre | Travail réalisé |
|---|---|
| Nour REKIK | Audit V1, zone incoming/, identités IAM, ingestion, anti-doublon, traçabilité ; Kafka, caisse, producer, observation de Kafka |
| Bouchra TYAL | Gestion des erreurs et reprise, reconnexion du pipeline, lot de test ; Structured Streaming, indicateur progressif, reprise, cohabitation, incidents |
| Commun | Architectures V1/V2/V3, choix techniques, analyse des tests, rapport et README |

Évolution du Data Lake de la phase 1 (MinIO, zones RAW / PROCESSED / CURATED, PySpark) :
- **Phase 2** : ingestion automatisée depuis une zone d'arrivée `incoming/`, avec contrôles, anti-doublon, traçabilité et gestion des erreurs ;
- **Phase 3** : flux de ventes en temps réel avec Kafka et Spark Structured Streaming, en cohabitation avec le batch.

Le rapport complet (architectures V1, V2, V3, tests commentés et incidents) est dans `RAPPORT.pdf`.

## Organisation du projet

```
├── src/
│   ├── common/config.py          # paramètres + connexion MinIO par identité (aucun secret dans le code)
│   ├── ingestion/
│   │   ├── ingest.py             # incoming/ -> RAW : contrôles, hash SHA-256, manifest, journal, reprise
│   │   └── test_identites.py     # test de la séparation ingestion-user / pipeline-user
│   ├── batch/
│   │   ├── spark_utils.py        # session Spark, lecture de tous les lots RAW, écriture Parquet sans suppression
│   │   └── pipeline.py           # pipeline de la phase 1 : RAW -> PROCESSED -> CURATED
│   ├── streaming/
│   │   ├── caisse.py             # simulateur de caisse (rythme configurable)
│   │   ├── producer.py           # producer Kafka -> topic « ventes »
│   │   └── streaming_job.py      # Spark Structured Streaming : Kafka -> RAW -> PROCESSED -> CURATED
│   └── run_batch.py              # chaîne batch complète : ingestion puis pipeline
├── config/
│   ├── settings.json                        # bucket, sources, extensions, Kafka
│   ├── referentiel_caisse.json              # magasins / produits / clients du simulateur
│   ├── credentials_ingestion.example.json   # modèles (les vrais fichiers sont exclus du rendu)
│   └── credentials_pipeline.example.json
├── policies/                     # policies IAM (JSON)
├── requirements.txt
├── README.md
└── RAPPORT.pdf
```

## Prérequis

- Python 3.13, Java 17
- MinIO (AIStor) et le client `mc` (alias `myaistor`)
- Apache Kafka 4.x en mode KRaft (`brew install kafka`)
- `pip install -r requirements.txt` (boto3, pyspark, pandas, openpyxl, pyarrow, kafka-python)

## Mise en place

### 1. Environnement

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Identités MinIO (mission 3)

```bash
mc admin policy create myaistor tp2-ingestion-raw     policies/tp2-ingestion-raw.json
mc admin policy create myaistor tp2-readonly          policies/tp2-readonly.json
mc admin policy create myaistor tp2-write-transformed policies/tp2-write-transformed.json
mc admin user add myaistor ingestion-user <mot_de_passe>
mc admin user add myaistor pipeline-user  <mot_de_passe>
mc admin policy attach myaistor tp2-ingestion-raw --user ingestion-user
mc admin policy attach myaistor tp2-readonly tp2-write-transformed --user pipeline-user
mc admin accesskey create myaistor ingestion-user
mc admin accesskey create myaistor pipeline-user
```

| Identité | RAW | PROCESSED / CURATED | Suppression |
|---|---|---|---|
| ingestion-user | lecture + écriture | aucun accès | non |
| pipeline-user | lecture | lecture + écriture | non |

### 3. Credentials (jamais dans le code ni dans le rendu)

Copier les modèles et y placer les clés de chaque identité :

```bash
cp config/credentials_ingestion.example.json config/credentials_ingestion.json
cp config/credentials_pipeline.example.json  config/credentials_pipeline.json
```

On peut aussi utiliser des variables d'environnement : `MINIO_INGESTION_ACCESS_KEY` / `MINIO_INGESTION_SECRET_KEY`, et `MINIO_PIPELINE_ACCESS_KEY` / `MINIO_PIPELINE_SECRET_KEY`.

### 4. Kafka

```bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
kafka-topics --bootstrap-server localhost:9092 --create --topic ventes --partitions 3 --replication-factor 1
```

## Exécution

### Batch (phase 2)

1. Déposer les fichiers dans `incoming/ventes`, `incoming/clients`, `incoming/produits` ou `incoming/magasins`.
2. Lancer la chaîne complète :

```bash
python src/run_batch.py              # ingestion puis pipeline
python src/ingestion/ingest.py       # ingestion seule
python src/ingestion/test_identites.py
```

- Fichiers ingérés : `raw/<source>/ingest_date=AAAA-MM-JJ/`, puis archivés dans `incoming/_archive/`
- Fichiers invalides : `incoming/_rejected/`
- Journal : `logs/ingestion.log` (lisible) et `logs/ingestion_journal.jsonl` (structuré)
- Anti-doublon : empreinte SHA-256 enregistrée dans `raw/_ingestion/manifest.json`
- Codes retour : 0 = succès, 1 = fichiers rejetés, 2 = ingestion impossible (MinIO ou credentials), avec les fichiers conservés pour reprise

### Streaming (phase 3)

```bash
python src/streaming/streaming_job.py --intervalle 20      # terminal 1 : traitement (micro-batch de 20 s)
python src/streaming/producer.py --rythme 1                # terminal 2 : caisse (1 vente/s)
python src/streaming/producer.py --taux-invalides 0.3      # test : 30 % d'événements hors schéma
```

| Sortie | Emplacement |
|---|---|
| Messages bruts + partition/offset | `raw/ventes_stream/date=…/heure=…/batch_NNNNNN.jsonl` |
| Événements hors schéma | `raw/_rejets_stream/date=…/` |
| Ventes valides (Parquet) | `processed/ventes_stream/date=…/batch=NNNNNN/` |
| Indicateurs | `curated/streaming/ca_par_ville/`, `curated/streaming/ca_par_categorie/` |
| Reprise | checkpoint local `checkpoints/ventes_stream/` |

## Choix techniques principaux

- **Anti-doublon par hash du contenu** : il résiste au renommage d'un fichier. Le manifest est stocké dans MinIO pour être partagé quelle que soit la machine.
- **Journal local en ajout seul** : il reste disponible même quand MinIO est en panne.
- **Pipeline de la phase 1 réutilisé** : il lit désormais tous les lots, et en cas de doublon la version la plus récemment ingérée l'emporte.
- **Micro-batch plutôt qu'« un événement = un fichier »** : cela évite le problème des petits fichiers.
- **Checkpoint et fichiers nommés par numéro de micro-batch** : à la reprise, il n'y a ni perte ni doublon.
- **Écriture Parquet locale, puis PutObject sous des clés fixes** : c'est idempotent et ne nécessite aucun droit de suppression.

## Éléments exclus du rendu

`.venv/`, `config/credentials_*.json` (seuls les `.example.json` sont fournis), `checkpoints/`, `logs/`, `incoming/`, `data/`, `__pycache__/`, et toute Access Key ou Secret Key.
