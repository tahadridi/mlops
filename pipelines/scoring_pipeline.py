# Fichier : pipelines/scoring_pipeline.py
"""Pipeline KFP « scoring-pipeline » (Labs 2 et 3).

ingest -> validate_data -> prepare -> detect_drift -> train -> evaluate
                                                            -> validate_model -> register
Trois gates (Lab 3) : contrat de données (Great Expectations), drift entre entraînement et
test, validation du modèle (Evidently + tests comportementaux). Un gate en échec arrête le run.
Compilation : python -m pipelines.scoring_pipeline   (écrit scoring_pipeline.yaml)
Les valeurs par défaut sont lues dans params.yaml AU MOMENT DE LA COMPILATION :
params.yaml reste la seule source des valeurs par défaut, comme pour dvc repro.
"""

from pathlib import Path

import yaml
from kfp import compiler, dsl, kubernetes

from pipelines.components import (
    MLFLOW_URI,
    detect_drift,
    evaluate_model,
    ingest_data,
    prepare_features,
    register_model,
    train_model,
    validate_data,
    validate_model,
)

PARAMS = yaml.safe_load(Path("params.yaml").read_text(encoding="utf-8"))
TRAIN, DRIFT = PARAMS["train"], PARAMS["drift"]


@dsl.pipeline(
    name="scoring-pipeline",
    description="Credit scoring : ingest -> validate -> prepare -> drift -> train -> evaluate"
    " -> validate_model -> register",
)
def scoring_pipeline(
    model_type: str = TRAIN["model"],
    C: float = TRAIN["C"],
    class_weight: str = str(TRAIN["class_weight"]),  # « none » accepté, comme au Lab 1
    n_estimators: int = TRAIN["n_estimators"],
    max_depth: str = str(TRAIN["max_depth"]),  # « none » accepté, comme au Lab 1
    learning_rate: float = TRAIN["learning_rate"],
    random_state: int = TRAIN["random_state"],
    mlflow_uri: str = MLFLOW_URI,
    ks_alpha: float = DRIFT["ks_alpha"],
    psi_threshold: float = DRIFT["psi_threshold"],
    drift_share: float = DRIFT["drift_share"],
):
    # Source externe (réseau) : 3 nouvelles tentatives, et jamais plus de 10 min
    ingest_task = ingest_data()
    ingest_task.set_retry(num_retries=3, backoff_duration="30s")
    ingest_task.set_cpu_limit("500m").set_memory_limit("512Mi")
    kubernetes.set_timeout(ingest_task, 600)

    # Gate 1 : le contrat de données (suite Great Expectations versionnée dans gx/)
    validate_task = validate_data(raw_data=ingest_task.outputs["raw_data"])
    validate_task.set_memory_limit("1Gi")

    prep_task = prepare_features(raw_data=ingest_task.outputs["raw_data"])
    prep_task.after(validate_task)  # gate : pas de préparation sans validation réussie
    prep_task.set_memory_limit("1Gi")

    # Gate 2 : le jeu de test ressemble-t-il à celui d'entraînement ?
    drift_task = detect_drift(
        reference_data=prep_task.outputs["train_data"],
        current_data=prep_task.outputs["test_data"],
        ks_alpha=ks_alpha,
        psi_threshold=psi_threshold,
        drift_share=drift_share,
        fail_on_drift=True,
    )
    drift_task.set_memory_limit("1Gi")

    train_task = train_model(
        raw_data=ingest_task.outputs["raw_data"],
        train_data=prep_task.outputs["train_data"],
        test_data=prep_task.outputs["test_data"],
        model_type=model_type,
        C=C,
        class_weight=class_weight,
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        random_state=random_state,
        mlflow_uri=mlflow_uri,
        kfp_run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER,  # remplacé par l'id du run KFP
    )
    train_task.after(drift_task)  # gate : pas d'entraînement sur des données qui dérivent
    # Une limit seule vaut aussi request : on demande peu, on autorise plus
    train_task.set_cpu_request("500m").set_cpu_limit("2")
    train_task.set_memory_request("512Mi").set_memory_limit("2Gi")
    kubernetes.set_timeout(train_task, 1800)
    # Toujours ré-entraîner : chaque run KFP doit produire un nouveau run MLflow
    train_task.set_caching_options(enable_caching=False)

    eval_task = evaluate_model(
        model=train_task.outputs["model"],
        test_data=prep_task.outputs["test_data"],
        run_id=train_task.outputs["Output"],
        mlflow_uri=mlflow_uri,
    )
    eval_task.set_memory_limit("1Gi")

    # Gate 3 : le modèle lui-même (Evidently + tests comportementaux)
    model_task = validate_model(
        model=train_task.outputs["model"],
        train_data=prep_task.outputs["train_data"],
        test_data=prep_task.outputs["test_data"],
    )
    model_task.set_memory_limit("1Gi")

    register_task = register_model(run_id=train_task.outputs["Output"], mlflow_uri=mlflow_uri)
    register_task.after(eval_task, model_task)  # on n'enregistre qu'un modèle validé
    register_task.set_memory_limit("1Gi")


if __name__ == "__main__":
    compiler.Compiler().compile(scoring_pipeline, package_path="scoring_pipeline.yaml")
train_task.after(drift_task) : pas d'entraînement si le test ne ressemble pas à l'entraînement ; register_task.after(eval_task, model_task) : on n'enregistre (même en @challenger) qu'un modèle évalué et validé.
Les seuils de drift deviennent des paramètres du pipeline, lus dans params.yaml à la compilation, modifiables au lancement d'un run.
Mémoire mesurée (composant + sous-processus) : environ 450 Mo pour detect-drift, 580 Mo pour validate-model, d'où les limites à 1 Gi.
python -m pipelines.scoring_pipeline
wc -l scoring_pipeline.yaml
python - <<'EOF'
from kfp import local

from pipelines.scoring_pipeline import scoring_pipeline

local.init(runner=local.SubprocessRunner(use_venv=False))
scoring_pipeline(mlflow_uri="http://localhost:5000")
EOF
Sortie attendue : 795 scoring_pipeline.yaml. En une minute environ, les 8 tâches passent en SUCCESS : validate-data (67 expectations), detect-drift (pas de drift), validate-model (Modèle VALIDÉ), puis register-model (@champion -> version …).

6.4 Image 1.2.0 et runs sur le cluster
sed -i 's/scoring-pipeline:1.1.0/scoring-pipeline:1.2.0/' \
    pipelines/components.py dags/scoring_dag.py
docker build -f pipelines/Dockerfile --build-arg VERSION=1.2.0 \
    -t localhost:5001/scoring-pipeline:1.2.0 .
docker push localhost:5001/scoring-pipeline:1.2.0
python -m pipelines.scoring_pipeline
python -m pipelines.drift_demo
grep -c "image: localhost:5001/scoring-pipeline:1.2.0" scoring_pipeline.yaml
