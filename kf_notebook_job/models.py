import os
import shutil
import yaml

from datetime import datetime

from kfp import Client

import ipywidgets as widgets
from IPython.display import Markdown, display

from .notebook import get_parameters
from .kubernetes import get_mounts, VolumeSelector, ResourceSelector, get_namespace
from .pipeline import Pipeline, get_pipeline_func
from .conda import get_notebook_env_spec

class Experiment:
    def __init__(self, name):
        self.name = name
        
        self._client = Client()
        try:
            self.experiment = self._client.get_experiment(experiment_name=self.name)
        except ValueError:
            self.experiment = self._client.create_experiment(self.name)
            
        display(
            Markdown(
                "[Experiment details]"
                f"(/pipeline/#/experiments/details/{self.experiment.id}/)"
            )
        )

        self.pipeline_name = f"{get_namespace()}-{self.name}"
        self.pipeline_id = self._client.get_pipeline_id(self.pipeline_name)
        if self.pipeline_id is not None:
            self.pipeline = self._client.get_pipeline(self.pipeline_id)
            self.pipeline_version_id = self.pipeline.default_version.id

    def configure(self, notebook_path, resources = {}):
        self.new_notebook_path = os.path.abspath(notebook_path)
        
        for key in resources.keys():
            resources[key] = str(resources[key])
        
        mounts, volumes = get_mounts()
        self.volume_conf = VolumeSelector(mounts, volumes, self.new_notebook_path)
        self.resource_conf = ResourceSelector(defaults=resources)
        
        self.volume_conf.show()
        self.resource_conf.show()

    def _check_env(self, env_def, requirements):
        pkgs = {pkg.split('=')[0] for pkg in env_def['dependencies'] if type(pkg) == str}
        for req in requirements:
            if req not in pkgs:
                raise Exception(
                    f"Your job environment {envdef['name']} does not contain {req}. "
                    f"Please install by running `mamba install {req}`"
                )

    def save(self):
        if self.new_notebook_path is None:
            raise Exception("Please run .configure() first")

        base_path = os.path.dirname(self.new_notebook_path)
        archive_dir = os.path.join(base_path, 'job-archive')
        os.makedirs(archive_dir, exist_ok=True)
        archive_name = f"{self.name}-{datetime.now().isoformat()}.ipynb"
        archive_path = os.path.join(archive_dir, archive_name)
        shutil.copy(self.new_notebook_path, archive_path)
        
        self.notebook_path = archive_path
        
        env_def = get_notebook_env_spec(self.notebook_path)
        self._check_env(env_def, ['papermill', 'ipykernel'])
        conda_env_path = os.path.join(archive_dir, f"{archive_name}-environment.yml")
        
        with open(conda_env_path, 'w') as file:
            yaml.safe_dump(env_def, file)
        
        mounts, volumes = self.volume_conf.get_mounts()
        
        session_mounts, _ = get_mounts()
        for mount in mounts:
            if mount.mount_path == '/work':
                session_mount = next(smount for smount in session_mounts if smount.name == mount.name)
                out_path = os.path.join(session_mount.mount_path, self.name)
                os.makedirs(out_path, exist_ok=True)
                break
        
        pfunc = get_pipeline_func(
            self.notebook_path,
            experiment_name = self.name,
            mounts = mounts, volumes = volumes,
            pipeline_name = self.pipeline_name,
            conda_env_path = conda_env_path,
            **self.resource_conf.get_args()
        )
        
        pipeline = Pipeline(pfunc, self.notebook_path)
        with widgets.Output():
            pipeline.upload()
        self.pipeline = pipeline.pipeline
        self.pipeline_id = pipeline.pipeline.id
        self.pipeline_version_id = pipeline.version.id
        
        display(
            Markdown(
                "[Pipeline details]"
                f"(/pipeline/#/pipelines/details/{self.pipeline_id}/"
                f"version/{self.pipeline_version_id})")
        )
        
    def create_job(self, job_name, **kwargs):
        return self._client.run_pipeline(
            experiment_id=self.experiment.id,
            pipeline_id=self.pipeline.id,
            
            job_name=job_name,
            version_id=self.pipeline_version_id,
            params = kwargs
        )