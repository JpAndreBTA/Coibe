import pandas as pd
from sklearn.ensemble import IsolationForest


def create_mock_purchases() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"id": 1, "cidade": "São Paulo", "preco_unitario": 3450.00},
            {"id": 2, "cidade": "São Paulo", "preco_unitario": 3590.00},
            {"id": 3, "cidade": "Brasília", "preco_unitario": 3720.00},
            {"id": 4, "cidade": "Curitiba", "preco_unitario": 3380.00},
            {"id": 5, "cidade": "Recife", "preco_unitario": 3650.00},
            {"id": 6, "cidade": "Manaus", "preco_unitario": 3890.00},
            {"id": 7, "cidade": "Belo Horizonte", "preco_unitario": 3520.00},
            {"id": 8, "cidade": "Porto Alegre", "preco_unitario": 3410.00},
            {"id": 9, "cidade": "Salvador", "preco_unitario": 3770.00},
            {"id": 10, "cidade": "Fortaleza", "preco_unitario": 3480.00},
            {"id": 11, "cidade": "Goiânia", "preco_unitario": 11900.00},
            {"id": 12, "cidade": "Belém", "preco_unitario": 9800.00},
        ]
    )


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    model = IsolationForest(
        n_estimators=100,
        contamination=0.17,
        random_state=42,
    )

    features = df[["preco_unitario"]]
    predictions = model.fit_predict(features)

    result = df.copy()
    result["anomaly_score"] = model.decision_function(features)
    result["is_anomaly"] = predictions == -1
    return result.sort_values(["is_anomaly", "preco_unitario"], ascending=[False, False])


if __name__ == "__main__":
    purchases = create_mock_purchases()
    analyzed = detect_anomalies(purchases)

    print("\nCompras governamentais de Computadores analisadas:\n")
    print(analyzed.to_string(index=False))
