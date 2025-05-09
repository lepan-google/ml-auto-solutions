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

"""Utilities to construct configs for maxtext DAG on GKE."""

from dags.common import test_owner
from xlml.apis import gcp_config, metric_config, task, test_config
from xlml.apis.xpk_cluster_config import XpkClusterConfig
from dags import gcs_bucket
from dags.common.vm_resource import TpuVersion, Project, XpkClusters, GpuVersion, CpuVersion
from typing import Iterable
import datetime


def get_gke_config(
    time_out_in_min: int,
    test_name: str,
    docker_image: str,
    test_owner: str,
    run_model_cmds: Iterable[str],
    cluster: XpkClusterConfig = XpkClusters.TPU_V4_8_MAXTEXT_CLUSTER,
    num_slices: int = 1,
    dataset_name: metric_config.DatasetOption = metric_config.DatasetOption.XLML_DATASET,
    dataset_project: str = Project.CLOUD_ML_AUTO_SOLUTIONS.value,
    composer_project: str = Project.CLOUD_ML_AUTO_SOLUTIONS.value,
    base_output_directory: str = None,
    metric_aggregation_strategy: metric_config.AggregationStrategy = None,
    user_specified_job_metric_config: metric_config.MetricConfig = None,
) -> task.XpkTask:
  job_gcp_config = gcp_config.GCPConfig(
      project_name=cluster.project,
      zone=cluster.zone,
      dataset_name=dataset_name,
      dataset_project=dataset_project,
      composer_project=composer_project,
  )

  job_test_config = test_config.TpuGkeTest(
      test_config.Tpu(
          version=cluster.device_version,
          cores=cluster.core_count,
      ),
      test_name=test_name,
      run_model_cmds=run_model_cmds,
      set_up_cmds=None,
      timeout=datetime.timedelta(minutes=time_out_in_min),
      task_owner=test_owner,
      num_slices=num_slices,
      cluster_name=cluster.name,
      docker_image=docker_image,
  )
  job_metric_config = user_specified_job_metric_config
  if job_metric_config is None:
    job_metric_config = (
        metric_config.MetricConfig(
            tensorboard_summary=metric_config.SummaryConfig(
                file_location=base_output_directory,
                aggregation_strategy=metric_aggregation_strategy,
                use_regex_file_location=True,
            ),
        )
        if base_output_directory and metric_aggregation_strategy
        else None
    )

  return task.XpkTask(
      task_test_config=job_test_config,
      task_gcp_config=job_gcp_config,
      task_metric_config=job_metric_config,
  )


def get_gke_maxtext_nightly_config(
    time_out_in_min: int,
    test_name: str,
    docker_image: str,
    test_owner: str,
    cluster: XpkClusterConfig = XpkClusters.TPU_V4_8_MAXTEXT_CLUSTER,
    num_slices: int = 1,
    dataset_name: metric_config.DatasetOption = metric_config.DatasetOption.XLML_DATASET,
    dataset_project: str = Project.CLOUD_ML_AUTO_SOLUTIONS.value,
    composer_project: str = Project.CLOUD_ML_AUTO_SOLUTIONS.value,
) -> task.XpkTask:
  job_gcp_config = gcp_config.GCPConfig(
      project_name=cluster.project,
      zone=cluster.zone,
      dataset_name=dataset_name,
      dataset_project=dataset_project,
      composer_project=composer_project,
  )

  current_time = datetime.datetime.now()
  current_date = current_time.strftime("%Y-%m-%d")
  current_datetime = current_time.strftime("%Y-%m-%d-%H-%M-%S")
  base_output_directory = (
      f"{gcs_bucket.BASE_OUTPUT_DIR}/maxtext/nightly/automated/{current_date}"
  )
  run_name = f"{num_slices}slice-V{cluster.device_version.value}_{cluster.core_count}-maxtext-nightly-{current_datetime}"

  run_model_cmds = (
      "bash preflight.sh PLATFORM=GKE",
      (
          "JAX_PLATFORM_NAME=TPU XLA_FLAGS='--xla_dump_to=/tmp/xla_dump/'"
          " ENABLE_PJRT_COMPATIBILITY=true"
          f" python3 -m MaxText.train MaxText/configs/base.yml run_name={run_name}"
          f" base_output_directory={base_output_directory}"
          " dataset_path=gs://max-datasets-rogue dataset_type=synthetic"
          " model_name=llama3-8b per_device_batch_size=12 reuse_example_batch=1 metrics_file='metrics.txt'"
          " steps=50 enable_checkpointing=false profiler=xplane upload_all_profiler_results=true skip_first_n_steps_for_profiler=10 profiler_steps=10 gcs_metrics=true"
      ),
  )

  job_test_config = test_config.TpuGkeTest(
      test_config.Tpu(
          version=cluster.device_version,
          cores=cluster.core_count,
      ),
      test_name=test_name,
      run_model_cmds=run_model_cmds,
      set_up_cmds=None,
      timeout=datetime.timedelta(minutes=time_out_in_min),
      task_owner=test_owner,
      num_slices=num_slices,
      cluster_name=cluster.name,
      docker_image=docker_image,
  )

  return task.XpkTask(
      task_test_config=job_test_config,
      task_gcp_config=job_gcp_config,
  )


def get_maxtext_end_to_end_gpu_gke_test_config(
    time_out_in_min: int,
    test_name: str,
    run_model_cmds: str,
    cluster: XpkClusterConfig,
    test_owner: str,
    docker_image: str,
    num_slices: int = 1,
) -> task.GpuCreateResourceTask:
  job_gcp_config = gcp_config.GCPConfig(
      project_name=cluster.project,
      zone=cluster.zone,
      dataset_name=metric_config.DatasetOption.XLML_DATASET,
  )

  job_test_config = test_config.GpuXpkTest(
      test_config.Gpu(
          machine_type=None,
          image_family=None,
          count=None,
          accelerator_type=cluster.device_version.value,
          runtime_version=None,
      ),
      test_name=test_name,
      set_up_cmds=None,
      run_model_cmds=run_model_cmds,
      timeout=datetime.timedelta(minutes=time_out_in_min),
      task_owner=test_owner,
      cluster_name=cluster.name,
      docker_image=docker_image,
      num_slices=num_slices,
  )

  return task.XpkTask(
      task_test_config=job_test_config,
      task_gcp_config=job_gcp_config,
  )


def get_gke_gpt3_6b_nightly_config(
    time_out_in_min: int,
    test_name: str,
    docker_image: str,
    test_owner: str,
    cluster: XpkClusterConfig = XpkClusters.TPU_V4_8_MAXTEXT_CLUSTER,
    num_slices: int = 1,
    dataset_name: metric_config.DatasetOption = metric_config.DatasetOption.XLML_DATASET,
    dataset_project: str = Project.CLOUD_ML_AUTO_SOLUTIONS.value,
    composer_project: str = Project.CLOUD_ML_AUTO_SOLUTIONS.value,
) -> task.XpkTask:
  job_gcp_config = gcp_config.GCPConfig(
      project_name=cluster.project,
      zone=cluster.zone,
      dataset_name=dataset_name,
      dataset_project=dataset_project,
      composer_project=composer_project,
  )

  current_time = datetime.datetime.now()
  current_date = current_time.strftime("%Y-%m-%d")
  current_datetime = current_time.strftime("%Y-%m-%d-%H-%M-%S")
  base_output_directory = (
      f"{gcs_bucket.BASE_OUTPUT_DIR}/maxtext/nightly/automated/{current_date}"
  )
  run_name = f"{num_slices}slice-V{cluster.device_version.value}_{cluster.core_count}-gpt3-6b-nightly-{current_datetime}"

  run_model_cmds = (
      "bash preflight.sh PLATFORM=GKE",
      (
          "JAX_PLATFORM_NAME=TPU XLA_FLAGS='--xla_dump_to=/tmp/xla_dump/'"
          " ENABLE_PJRT_COMPATIBILITY=true"
          f" python3 -m MaxText.train MaxText/configs/base.yml run_name={run_name} model_name=gpt3-6b"
          f" base_output_directory={base_output_directory}"
          " dataset_path=gs://max-datasets-rogue dataset_type=synthetic"
          " per_device_batch_size=12 reuse_example_batch=1 global_parameter_scale=1 metrics_file='metrics.txt'"
          " steps=50 enable_checkpointing=false profiler=xplane upload_all_profiler_results=true skip_first_n_steps_for_profiler=10 profiler_steps=10 gcs_metrics=true"
      ),
  )

  job_test_config = test_config.TpuGkeTest(
      test_config.Tpu(
          version=cluster.device_version,
          cores=cluster.core_count,
      ),
      test_name=test_name,
      run_model_cmds=run_model_cmds,
      set_up_cmds=None,
      timeout=datetime.timedelta(minutes=time_out_in_min),
      task_owner=test_owner,
      num_slices=num_slices,
      cluster_name=cluster.name,
      docker_image=docker_image,
  )

  return task.XpkTask(
      task_test_config=job_test_config,
      task_gcp_config=job_gcp_config,
  )


def get_maxtext_cpu_end_to_end_gke_config(
    time_out_in_min: int,
    test_name: str,
    docker_image: str,
    test_owner: str,
    run_model_cmds: Iterable[str],
    cluster: XpkClusterConfig = XpkClusters.CPU_N2_STANDARD_64_CLUSTER,
    machine_count: int = 1,
    num_slices: int = 1,
    dataset_name: metric_config.DatasetOption = metric_config.DatasetOption.XLML_DATASET,
    dataset_project: str = Project.CLOUD_ML_AUTO_SOLUTIONS.value,
    composer_project: str = Project.CLOUD_ML_AUTO_SOLUTIONS.value,
    base_output_directory: str = None,
    metric_aggregation_strategy: metric_config.AggregationStrategy = None,
) -> task.XpkTask:
  job_gcp_config = gcp_config.GCPConfig(
      project_name=cluster.project,
      zone=cluster.zone,
      dataset_name=dataset_name,
      dataset_project=dataset_project,
      composer_project=composer_project,
  )

  job_test_config = test_config.CpuGkeTest(
      test_config.Cpu(
          device_type=cluster.device_version,
          machine_count=machine_count,
      ),
      test_name=test_name,
      run_model_cmds=run_model_cmds,
      set_up_cmds=None,
      timeout=datetime.timedelta(minutes=time_out_in_min),
      task_owner=test_owner,
      num_slices=num_slices,
      cluster_name=cluster.name,
      docker_image=docker_image,
  )

  job_metric_config = (
      metric_config.MetricConfig(
          tensorboard_summary=metric_config.SummaryConfig(
              file_location=base_output_directory,
              aggregation_strategy=metric_aggregation_strategy,
              use_regex_file_location=True,
          ),
      )
      if base_output_directory and metric_aggregation_strategy
      else None
  )

  return task.XpkTask(
      task_test_config=job_test_config,
      task_gcp_config=job_gcp_config,
      task_metric_config=job_metric_config,
  )
