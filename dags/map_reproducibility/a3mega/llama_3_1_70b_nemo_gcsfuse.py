# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""DAGs to run hypercomputer recipes"""

import datetime

from airflow import models
from dags import composer_env
from dags.map_reproducibility.utils.common_utils import get_scheduled_time
from dags.map_reproducibility.utils.common_utils import run_nemo_workload


MODEL_ID = "llama3-1-70b"
METRICS_MODEL_ID = "llama3.1-70b"
PRECISION = "bf16"
HYPERCOMPUTER = "a3mega"
FRAMEWORK = "nemo"

SCHEDULED_TIME = (
    get_scheduled_time(HYPERCOMPUTER, MODEL_ID, FRAMEWORK)
    if composer_env.is_prod_env()
    else None
)

VALUE_YAML_PATH = (
    f"training/{HYPERCOMPUTER}/{MODEL_ID}/nemo-pretraining-gke/values.yaml"
)
SOFTWARE_ID = "pytorch_nemo"
IMAGE_VERSION = "nemo_workload:25.02"
KUEUE_NAME = "multislice-kueue"
NUM_GPUS = 256

default_dag_args = {
  "retries": 0,
}


with models.DAG(
    dag_id=f"{HYPERCOMPUTER}_recipes_{MODEL_ID}_{FRAMEWORK}_gcs_2",
    # schedule=SCHEDULED_TIME,
    tags=[
        # "reproducibility",
        # "experimental",
        # "xlml",
        # "regressiontests",
        # "a3mega",
        "storage-run"
    ],
    start_date=datetime.datetime(2024, 11, 15),
    catchup=False,
    default_args=default_dag_args,
) as dag:
  run_nemo_workload(
      hypercomputer=HYPERCOMPUTER,
      model_id=MODEL_ID,
      framework=FRAMEWORK,
      precision=PRECISION,
      kueue_name=KUEUE_NAME,
      metrics_model_id=METRICS_MODEL_ID,
      num_gpus=NUM_GPUS,
      config_model_name="llama3.1-70b-256gpus-bf16-gcsfuse-checkpointing.yaml",
      storage_run=True,
      storage_product="gcs",
      storage_next_branch=True,
      recipes_gob_patch_change="refs/changes/00/3800/3",
      bq_writer_repo_patch_change="",
      gcs_automation_repo_patch_change="refs/changes/23/2023/13",
      gcs_source_bucket="cmcs-checkpoint-sydney",
      gcs_metrics_bucket="cmcs-benchmark-raw-metrics",
      benchmark_type="checkpointing",
      gcsfuse_csi_driver="",
      logs_bucket="cmcs-benchmark-logs",
      num_steps=10,
      workload_type="system",
      workload_image="us-docker.pkg.dev/supercomputer-testing/dlsl-metadata/recipe-release-patched",
  )
