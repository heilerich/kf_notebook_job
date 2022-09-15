import os
import warnings
import datetime

from tempfile import NamedTemporaryFile

import kfp.dsl
from kfp import Client
from kfp.compiler import Compiler
from kfp.components._structures import ComponentSpec, InputSpec, ContainerImplementation, ContainerSpec, \
    InputValuePlaceholder

from .notebook import get_parameters as get_nb_params
from .kubernetes import get_namespace

with warnings.catch_warnings():
    warnings.simplefilter(action='ignore', category=UserWarning)
    _compiler = Compiler()

_type_mappings = {
    'str': 'String',
    'int': 'Integer',
    'float': 'Float',
    'bool': 'Bool',
    'list': 'List',
    'dict': 'Dict'
}
_container_image = 'cr.nail.science/ml-containers/python-job:git-fbf6c64'


def _map_types(parameters):
    for name, param in parameters.items():
        inferred = param['inferred_type_name']
        if inferred in _type_mappings:
            yield (name, _type_mappings[inferred], param['default'])
        else:
            raise Exception(f"Parameter {name} has unsupported type hint '{inferred}', "
                            f"supported types are {', '.join(_type_mappings.keys())}")

def _gen_args(inputs):
    for inp in inputs:
        yield '-p'
        yield inp.name
        yield InputValuePlaceholder(inp.name)

def _get_parameters_def_str(nb_parameters):
    parameter_str = ""
    for name, param in nb_parameters.items():
        inferred = param['inferred_type_name']
        default = param['default']
        if default != '':
            parameter_str += f"{name}: {inferred} = {default}, "
        else:
            parameter_str = f"{name}: {inferred}, " + parameter_str
    return parameter_str

def _get_parameters_set_str(nb_parameters):
    parameter_str = ""
    for name, param in nb_parameters.items():
        parameter_str += f"{name} = {name}, "
    return parameter_str

def get_pipeline_func(notebook_path, **kwargs):
    nb_parameters = get_nb_params(notebook_path)
    component_inputs = [InputSpec(name=name, type=tp, default=default) for name, tp, default in _map_types(nb_parameters)]
    component_args = list(_gen_args(component_inputs))

    run_component = kfp.components._structures.ComponentSpec(
        name = 'Run notebook',
        description = 'Executes job notebook',
        inputs = component_inputs,
        implementation=ContainerImplementation(ContainerSpec(
            image=_container_image,
            command=[
                '/init', 'papermill',
                os.path.abspath(notebook_path),
                f"/work/{{{{inputs.parameters.run_id}}}}-{os.path.basename(notebook_path)}",
                '--kernel', 'job-kernel'
            ],
            args=component_args
        ))
    )
    
    pipeline_def = ("""
import math
import kfp.dsl
from kfp.dsl import RUN_ID_PLACEHOLDER
from kubernetes.utils.quantity import parse_quantity
from kfp.components._components import load_component_from_spec
from kubernetes.client.models.v1_toleration import V1Toleration
from kubernetes.client.models import V1EnvVar

@kfp.dsl.pipeline(name=pipeline_name)
def execute_notebook(
        PARAMETER_PLACEHOLDER_HINTS
        run_id: str = RUN_ID_PLACEHOLDER):
    op = (
        load_component_from_spec(run_component)(
            PARAMETER_PLACEHOLDER
        )
        .add_pod_label('shared-package-cache', 'true')
        .add_toleration(V1Toleration(key='nvidia.com/gpu', operator='Exists', effect='NoSchedule'))
        .set_cpu_request(str(cpu_request))
        .set_cpu_limit(str(math.ceil(parse_quantity(cpu_request))))
        .set_memory_request(str(memory_request))
        .set_memory_limit(str(parse_quantity(memory_request) + parse_quantity('200Mi')))
        .set_gpu_limit(str(gpu_request))
    )
    (op.container
        .add_env_variable(V1EnvVar(name='JOB_NAME', value=f"{experiment_name}-{run_id}"))
        .add_env_variable(V1EnvVar(name='CONDA_ENV_PATH', value=conda_env_path))
    )
    for mount in mounts:
        if mount.mount_path == '/work':
            mount.sub_path = f"{experiment_name}"
        op.container.add_volume_mount(mount)
    for volume in volumes:
        op.add_volume(volume)

pipeline = execute_notebook
    """.replace('PARAMETER_PLACEHOLDER_HINTS', _get_parameters_def_str(nb_parameters))
       .replace('PARAMETER_PLACEHOLDER', _get_parameters_set_str(nb_parameters))
    )

    kwargs.update({
        'notebook_path': notebook_path,
        'namespace': get_namespace(),
        'run_component': run_component
    })
    function_scope = kwargs

    exec(pipeline_def, function_scope)
    return function_scope['pipeline']

def get_pipeline_yaml(pfunc):
    tmp = NamedTemporaryFile(suffix='.yaml', delete=True)
    _compiler.compile(pfunc, tmp.name)
    yaml = tmp.read()
    tmp.close()
    return yaml

class Pipeline:
    def __init__(self, pfunc, notebook_path):
        self.pipeline_name = pfunc._component_human_name
        
        self.yaml = get_pipeline_yaml(pfunc).decode()
        self._client = Client()
        self.pipeline_id = self._client.get_pipeline_id(self.pipeline_name)
        
        nb_parameters = get_nb_params(notebook_path)
        args = _get_parameters_def_str(nb_parameters)
        
    def upload(self):
        with NamedTemporaryFile(suffix='.yaml', mode='w', delete=False) as tmp:
            tmp.write(self.yaml)

        try:
            if self.pipeline_id is not None:
                self.pipeline = self._client.get_pipeline(self.pipeline_id)
                self.version = self._client.upload_pipeline_version(
                    tmp.name, pipeline_id=self.pipeline_id,
                    pipeline_version_name=datetime.datetime.now().isoformat())
            else:
                self.pipeline = self._client.upload_pipeline(tmp.name, pipeline_name=self.pipeline_name)
                self.version = self.pipeline.default_version
        finally:
            os.remove(tmp.name)
        