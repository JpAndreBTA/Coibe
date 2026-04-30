# COIBE.IA Models

Esta pasta guarda o estado do monitor adaptativo.

Arquivos principais:

- `monitor_model_state.json`: estado adaptativo, termos aprendidos e metricas do ultimo ciclo.
- `monitor_training_history.jsonl`: historico incremental de ciclos de treinamento.
- `model_registry.json`: modelos disponiveis para selecao na UI local do backend.
- `coibe_adaptive_deep_model.joblib`: classificador local `scikit-learn` usado quando `coibe-deep-mlp` esta selecionado.
- `coibe_adaptive_deep_model.onnx.json`: manifesto de contrato para exportacao ONNX.
- `coibe_adaptive_deep_model.quant.json`: manifesto de compatibilidade de quantizacao.

- `monitor_model_state.json`: termos aprendidos, métricas do último ciclo e configuração de GPU usada.
- `monitor_training_history.jsonl`: histórico incremental de ciclos de treinamento/atualização.

Os arquivos são gerados pelo `local_monitor.py`. A análise é usada para priorizar busca e triagem em dados públicos; ela não conclui crime, culpa, parentesco ou irregularidade sem validação humana.
