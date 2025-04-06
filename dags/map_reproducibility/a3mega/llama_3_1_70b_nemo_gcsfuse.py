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
import sys
import os
import tempfile

from airflow import models
from airflow.decorators import task
from airflow.hooks.subprocess import SubprocessHook
from dags import composer_env
from dags.map_reproducibility.utils.common_utils import get_nemo_metrics_cmds
from dags.map_reproducibility.utils.common_utils import configure_project_and_cluster
from dags.map_reproducibility.utils.common_utils import install_helm_cmds
from dags.map_reproducibility.utils.common_utils import namespace_cmds
from dags.map_reproducibility.utils.common_utils import wait_for_jobs_cmds
from dags.map_reproducibility.utils.common_utils import copy_bucket_cmds_nemo
from dags.map_reproducibility.utils.common_utils import cleanup_cmds
from dags.map_reproducibility.utils.common_utils import git_cookie_authdaemon
from dags.map_reproducibility.utils.common_utils import clone_recipes_gob
from dags.map_reproducibility.utils.common_utils import helm_apply_cmds
from dags.map_reproducibility.utils.common_utils import get_nemo_metrics
from dags.map_reproducibility.utils.common_utils import get_bq_writer_repo
# from dags.map_reproducibility.utils.common_utils import get_gcs_result_processing_repo
from dags.map_reproducibility.utils.benchmarkdb_utils import write_run
from dags.map_reproducibility.utils.common_utils import extract_run_details
from dags.map_reproducibility.utils.common_utils import extract_gpus
from dags.map_reproducibility.utils.common_utils import get_accelerator_type
from dags.map_reproducibility.utils.common_utils import get_pre_workload_cmds
from dags.map_reproducibility.utils.common_utils import get_gpu_recipe_cmd
from dags.map_reproducibility.utils.common_utils import get_bq_writer_path
from dags.map_reproducibility.utils.common_utils import get_recipe_repo_path
# from dags.map_reproducibility.utils.common_utils import get_run_results_generator_repo_path
from dags.map_reproducibility.utils.common_utils import get_scheduled_time
from dags.map_reproducibility.utils.common_utils import get_cluster
from dags.map_reproducibility.utils.common_utils import get_docker_image


MODEL_ID = "llama-3.1-70b"
METRICS_MODEL = "llama3.1-70b"
JOB_MODEL_NAME = "llama3-1-70b"
PRECISION = "bf16"
HYPERCOMPUTER = "a3mega"
FRAMEWORK = "nemo"

# SCHEDULED_TIME = (
#     get_scheduled_time(HYPERCOMPUTER, MODEL_ID, FRAMEWORK)
#     if composer_env.is_prod_env()
#     else None
# )

VALUE_YAML_PATH = (
    f"training/{HYPERCOMPUTER}/llama3.1-70b-gcsfuse/nemo-pretraining-gke/values.yaml"
)
CLUSTER, CLUSTER_REGION = get_cluster(HYPERCOMPUTER)
SOFTWARE_ID = "pytorch_nemo"
IMAGE_VERSION = "nemo_workload:24.07"
DOCKER_IMAGE = get_docker_image(HYPERCOMPUTER, FRAMEWORK)


@task
def run_aotc_workload():
  with tempfile.TemporaryDirectory() as tmpdir:
    hook = SubprocessHook()

    result = hook.run_command(
        [
            "bash",
            "-c",
            ";".join(
                git_cookie_authdaemon()
                + clone_recipes_gob()
                + get_bq_writer_repo()
            ),
        ],
        cwd=tmpdir,
    )


    recipe_repo_root = get_recipe_repo_path(tmpdir)
    bq_writer_repo_root = get_bq_writer_path(tmpdir)

    num_gpus = extract_gpus(recipe_repo_root, VALUE_YAML_PATH)
    print("get num gpus")
    print(f"recipe_repo_root: {recipe_repo_root}")
    print(f"bq_writer_repo_root: {bq_writer_repo_root}")
    print(VALUE_YAML_PATH)
    print(num_gpus)
    config_yaml_path = f"src/frameworks/{HYPERCOMPUTER}/nemo-configs/{METRICS_MODEL}-{num_gpus}gpus-{PRECISION}-gcsfuse-training.yaml"
    full_config_yaml_path = os.path.join(recipe_repo_root, config_yaml_path)
    print(full_config_yaml_path)

    (
        global_batch_size,
        optimizer,
        precision,
        seq_length,
        max_steps,
    ) = extract_run_details(recipe_repo_root, config_yaml_path)

    accelerator_type = get_accelerator_type(HYPERCOMPUTER)
    print(
        f"batch size: {global_batch_size}, num gpus: {num_gpus},  precision: {precision}, seq length: {seq_length}, max steps: {max_steps}"
    )

    helm_cmds = helm_apply_cmds(
                    FRAMEWORK,
                    HYPERCOMPUTER,
                    full_config_yaml_path,
                    recipe_repo_root,
                    DOCKER_IMAGE,
                )
    print(f"helm command: {helm_cmds}")

    result = hook.run_command(
        [
            "bash",
            "-c",
            ";".join(
                configure_project_and_cluster(CLUSTER, CLUSTER_REGION)
                + get_gpu_recipe_cmd(
                    HYPERCOMPUTER, MODEL_ID, FRAMEWORK, recipe_repo_root
                )
                + install_helm_cmds()
                + namespace_cmds()
                + get_pre_workload_cmds(JOB_MODEL_NAME, FRAMEWORK)
                + helm_apply_cmds(
                    FRAMEWORK,
                    HYPERCOMPUTER,
                    full_config_yaml_path,
                    recipe_repo_root,
                    DOCKER_IMAGE,
                )
                + wait_for_jobs_cmds()
                # + copy_bucket_cmds_nemo(recipe_repo_root)
                # + get_nemo_metrics_cmds(
                #     global_batch_size,
                #     num_gpus,
                #     PRECISION,
                #     METRICS_MODEL,
                #     accelerator_type,
                #     tmpdir,
                # )
                + cleanup_cmds()
            ),
        ],
        cwd=tmpdir,
    )
    assert result.exit_code == 0, f"Command failed with code {result.exit_code}"

with models.DAG(
    dag_id=f"{HYPERCOMPUTER}_recipes_{MODEL_ID}_{FRAMEWORK}_GCSFuse",
    # schedule=SCHEDULED_TIME,
    schedule=None,
    tags=[
        # "reproducibility",
        # "experimental",
        # "xlml",
        # "regressiontests",
        "tests"
        "a3mega",
    ],
    start_date=datetime.datetime(2024, 11, 15),
    catchup=False,
) as dag:
  run_aotc_workload()
