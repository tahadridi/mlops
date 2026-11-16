# Benchmark de latence — german-credit, modèle v3

1000 appels par backend, après 10 appels de chauffe · x86_64 · Python 3.12.14

| Backend | P50 (ms) | P95 (ms) | P99 (ms) |
|---|---|---|---|
| sklearn_pipeline | 2.686 | 3.031 | 3.378 |
| onnx_python | 0.052 | 0.062 | 0.070 |
| onnx_batch_100 | 0.222 | 0.253 | 0.278 |

Accélération ONNX vs scikit-learn (P50, 1 ligne) : x51.8
