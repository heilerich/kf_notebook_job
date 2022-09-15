import subprocess
import yaml

import nb_conda_kernels

from .notebook import load_notebook, get_kernel_name

def get_notebook_env_spec(nb_path):
    kernel_name = get_kernel_name(load_notebook(nb_path))
    mgr = nb_conda_kernels.CondaKernelSpecManager()
    kernel_spec = mgr.get_kernel_spec(kernel_name)
    env_name = kernel_spec.metadata['conda_env_name']
    mamba_out = subprocess.check_output(['mamba', 'env', 'export', '-n', env_name])
    return yaml.safe_load(mamba_out)