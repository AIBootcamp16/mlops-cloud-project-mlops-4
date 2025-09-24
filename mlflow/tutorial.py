# tutorial.py
import mlflow
import mlflow.sklearn
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix
from sklearn.model_selection import train_test_split
from mlflow.models import infer_signature


remote_server_uri = "http://127.0.0.1:5001"
mlflow.set_tracking_uri(remote_server_uri)
# 1. 실험 이름 지정
mlflow.set_experiment("MLFlow_Tutorial")

mlflow.autolog()
# 데이터 로딩
iris = load_iris()
X_train, X_test, y_train, y_test = train_test_split(
    iris.data, iris.target, test_size=0.2, random_state=42
)

# 2. 실험 시작
with mlflow.start_run(run_name="RF_100_3"):
    print("ACTIVE EXP:", mlflow.active_run().info.experiment_id)
    print("ARTIFACT URI:", mlflow.get_artifact_uri())
    # 하이퍼파라미터
    n_estimators = 100
    max_depth = 3

    # 모델 학습
    model = RandomForestClassifier(
        n_estimators=n_estimators, max_depth=max_depth, random_state=42
    )
    model.fit(X_train, y_train)

    # 예측 및 평가
    preds = model.predict(X_test)
    accuracy = accuracy_score(y_test, preds)
    signature = infer_signature(X_test, preds)

    # 3. 메트릭 및 파라미터 로깅
    mlflow.log_param("n_estimators", n_estimators)
    mlflow.log_param("max_depth", max_depth)
    mlflow.log_metric("accuracy", accuracy)

    cm = confusion_matrix(y_test, preds)
    plt.figure(figsize=(6, 4))
    sns.heatmap(
        cm,
        annot=True,
        fmt="d",
        cmap="Blues",
        xticklabels=iris.target_names,
        yticklabels=iris.target_names,
    )
    plt.title("Confusion Matrix")
    plt.xlabel("Predicted")
    plt.ylabel("True")
    plt.tight_layout()

    # 5. 파일로 저장 후 artifact로 로깅
    os.makedirs("outputs", exist_ok=True)
    fig_path = "outputs/conf_matrix.png"
    plt.savefig(fig_path)
    mlflow.log_artifact(fig_path)

    # 6. 태그 설정
    mlflow.set_tag("model_type", "RandomForest")
    mlflow.set_tag("version", "v2.0")
    mlflow.set_tag("developer", "conny")

    print(f"Accuracy: {accuracy:.4f}")

    mlflow.sklearn.log_model(
        sk_model=model,
        name="random_forest_model",
        signature=signature,
        registered_model_name="IrisRandomForestModel",
    )
