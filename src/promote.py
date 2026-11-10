# Fichier : src/promote.py
"""Enregistre le modèle entraîné dans le Model Registry de MLflow."""

import argparse
import os
import mlflow

MODEL_NAME = "scoring-model"

def parse_args():
    parser = argparse.ArgumentParser(description="Enregistre le modèle dans MLflow Registry")
    # Supporte à la fois --run-id (utilisé par le pipeline) et --run_id
    parser.add_argument("--run-id", "--run_id", dest="run_id", required=True, help="ID du run MLflow")
    parser.add_argument("--mlflow_uri", "--mlflow-uri", default="http://localhost:5000", help="URI du serveur MLflow")
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Récupération de l'URI MLflow depuis l'environnement ou les arguments
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", args.mlflow_uri)
    mlflow.set_tracking_uri(tracking_uri)

    # URI de l'artefact model sauvegardé lors du train
    model_uri = f"runs:/{args.run_id}/model"

    print(f"Enregistrement du modèle depuis le run {args.run_id} sous le nom '{MODEL_NAME}'...")
    
    # Enregistrement du modèle dans le registre MLflow
    result = mlflow.register_model(model_uri=model_uri, name=MODEL_NAME)
    
    print(f"Modèle enregistré avec succès ! Nom : {MODEL_NAME}, Version : {result.version}")

if __name__ == "__main__":
    main()
