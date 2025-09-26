# mlflow_model_manager_alias_official.py

import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

class BestModelSelector:
    """
    MLflow 실험에서 최적 모델(run)을 선택하는 클래스
    """
    def __init__(self, experiment_name: str, metric_name: str = "RMSE"):
        self.metric_name = metric_name
        self.client = MlflowClient()
        exp = self.client.get_experiment_by_name(experiment_name)
        if exp is None:
            raise ValueError(f"Experiment '{experiment_name}' not found")
        self.experiment_id = exp.experiment_id

    def get_best_run(self):
        """
        experiment 내 모든 run 조회 후, metric 기준 최적 run 반환
        """
        runs = self.client.search_runs(
            experiment_ids=[self.experiment_id],
            filter_string="",
            order_by=[f"metrics.{self.metric_name} ASC"]
        )
        if not runs:
            raise ValueError(f"No runs found for experiment {self.experiment_id}")
        return runs[0]


class ModelRegistryManager:
    """
    최적 모델을 MLflow Registry에 등록하고 alias(champion)로 관리
    """
    def __init__(self, model_name: str, champion_alias: str = "champion"):
        self.model_name = model_name
        self.champion_alias = champion_alias
        self.client = MlflowClient()

    def register_model(self, run_id: str, artifact_path: str = "model", tags: dict = None):
        """
        모델 등록 후 태그 적용, 이전 champion alias 제거, 새 모델에 champion alias 지정
        """
        # 1️⃣ 모델 등록
        model_uri = f"runs:/{run_id}/{artifact_path}"
        registered_model = mlflow.register_model(model_uri=model_uri, name=self.model_name)
        version = registered_model.version
        print(f"모델 {self.model_name} v{version} 등록 완료")

        # 2️⃣ 태그 적용
        if tags:
            for key, value in tags.items():
                self.client.set_model_version_tag(
                    name=self.model_name,
                    version=version,
                    key=key,
                    value=str(value)
                )
            print(f"모델 {self.model_name} v{version} 태그 업데이트 완료: {tags}")

        # 3️⃣ 이전 champion alias 제거
        try:
            # 공식문서에 따르면 이전 alias가 있으면 덮어쓰기 가능하지만, 안전하게 삭제
            existing = self.client.get_model_version_by_alias(self.model_name, self.champion_alias)
            if existing:
                self.client.delete_registered_model_alias(self.model_name, self.champion_alias)
                print(f"이전 champion alias 제거 완료")
        except Exception:
            pass

        # 4️⃣ 새 모델에 champion alias 지정
        self.client.set_registered_model_alias(self.model_name, self.champion_alias, str(version))
        print(f"모델 {self.model_name} v{version} → alias '{self.champion_alias}' 지정 완료")

        return registered_model
