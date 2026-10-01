"""Chaîne batch complète (mission 8) : incoming/ -> ingestion -> RAW -> pipeline PySpark existant.

    python src/run_batch.py            # ingestion puis pipeline
    python src/run_batch.py --force    # relance le pipeline même sans nouveau fichier ingéré
"""
import subprocess
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent
PY = sys.executable

print("################ ÉTAPE 1/2 — INGESTION (ingestion-user) ################")
code = subprocess.call([PY, str(SRC / "ingestion" / "ingest.py")])

if code == 2:
    print("\n!! Ingestion impossible (MinIO ou credentials) : pipeline non lancé. Relancer plus tard.")
    sys.exit(2)

print("\n################ ÉTAPE 2/2 — PIPELINE PYSPARK (pipeline-user) ################")
code_pipeline = subprocess.call([PY, str(SRC / "batch" / "pipeline.py")])
sys.exit(code_pipeline or (1 if code == 1 else 0))
