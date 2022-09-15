import os
from kubernetes import client, config
import platform
import ipywidgets as widgets
from functools import partial
from copy import deepcopy
from collections import OrderedDict

from kubernetes.client.models.v1_volume import V1Volume
from kubernetes.client.models.v1_volume_mount import V1VolumeMount

config.load_incluster_config()
k8s_v1 = client.CoreV1Api()

def _find_mount_point(path):
    path = os.path.abspath(path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)
    return path

def get_namespace():
    with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as ns_file:
        namespace = ns_file.read()
    return namespace

def get_mounts():
    namespace = get_namespace()
    pod = k8s_v1.read_namespaced_pod(platform.node(), namespace)
    volumes = {vol.name: vol for vol in pod.spec.volumes if vol.persistent_volume_claim is not None}
    container = next((cont for cont in pod.spec.containers if cont.name == pod.metadata.labels['statefulset']), None)
    mounts = [mount for mount in container.volume_mounts if mount.name in volumes]
    return mounts, volumes

class VolumeSelector:
    def __init__(self, mounts, volumes, notebook_path):
        self.volumes = volumes
        self.mounts = mounts
        
        self.output_volume = None
        self.code_volume = self._get_notebook_mount(mounts, notebook_path).name
        
        self.volume_grid = widgets.GridspecLayout(len(mounts), 2)
        self.volume_grid.visible = False

        self.volume_checkboxes = []
        self.volume_paths = []
        
        output_index = None
        
        for i, mount in enumerate(mounts):
            if 'output' in mount.name:
                self.output_volume = mount.name
                output_index = i
            
            checkbox = widgets.Checkbox(value=True, layout=widgets.Layout(width='30px'), indent=False)
            self.volume_checkboxes.append(checkbox)

            self.volume_grid[i, 0] = widgets.HBox(
                [checkbox,
                widgets.Label(mount.name, layout=widgets.Layout(flex='auto'))]
            )

            pathbox = widgets.Text(mount.mount_path, layout=widgets.Layout(width='auto'))
            self.volume_paths.append(pathbox)
            self.volume_grid[i, 1] = pathbox

        def on_change_output(msg, selector):
            if selector.code_volume != msg.new:
                selector.output_volume = msg.new
            else:
                msg.owner.value = msg.old
            selector._update_values()

        on_change_output_bound = partial(on_change_output, selector=self)

        self.volume_box = widgets.VBox([
            self._get_box_selector(on_change_output_bound, index=output_index, title='Output Volume'),
            self.volume_grid
        ])
        
        self._update_values()
        
    def _get_notebook_mount(self, mounts, notebook_path):
        mount_point = _find_mount_point(notebook_path)
        return next(mount for mount in mounts if mount.mount_path == mount_point)

    def _update_values(self):
        for i, mount in enumerate(self.mounts):
            if mount.name == self.output_volume:
                self.volume_checkboxes[i].value = True
                self.volume_checkboxes[i].disabled = True
                self.volume_paths[i].value = '/work'
                self.volume_paths[i].disabled = True
            elif mount.name == self.code_volume:
                self.volume_checkboxes[i].value = True
                self.volume_checkboxes[i].disabled = True
                self.volume_paths[i].value = self.mounts[i].mount_path
                self.volume_paths[i].disabled = True
            else:
                self.volume_checkboxes[i].disabled = False
                self.volume_paths[i].disabled = False
                self.volume_paths[i].value = self.mounts[i].mount_path

    def _get_box_selector(self, callback, index, title):
        output_volume_select = widgets.Dropdown(
            options=[mount.name for mount in self.mounts],
            index=index
        )
        output_volume_select.observe(callback, names='value')
        output_volume_box = widgets.HBox([widgets.Label(title), output_volume_select])
        return output_volume_box

    def show(self):
        from IPython.display import display
        display(self.volume_box)
        
    def get_mounts(self):
        if self.output_volume is None:
            raise Exception("Please select an output volume")
        ret_mounts = []
        ret_volumes = []
        for i, mount in enumerate(self.mounts):
            if self.volume_checkboxes[i].value is True:
                mnt = deepcopy(mount)
                mnt.read_only = self.output_volume != mnt.name
                mnt.mount_path = self.volume_paths[i].value
                ret_mounts.append(mnt)
                ret_volumes.append(self.volumes[mnt.name])
        
        ret_mounts += [V1VolumeMount(
                            mount_path=f"/home/jovyan/{folder}",
                            name='ipython-files',
                            sub_path=folder
                        )
                        for folder in ['.local', '.cache', '.ipython']]
        ret_volumes.append(V1Volume(name='ipython-files', empty_dir={}))
        return ret_mounts, ret_volumes

_resource_settings = OrderedDict(
    cpu_request = 'Number of CPU cores. Can also use floats and millis e.g. 500m = 0.5',
    memory_request = 'E.g. 500Mi, 16Gi',
    gpu_request = 'Number of GPUs'
)

class ResourceSelector:
    def __init__(self, defaults = {}):
        labels = widgets.VBox([widgets.Label(name) for name in _resource_settings.keys()])
        self.inputs = [widgets.Text(value=defaults.get(key, '')) for key in _resource_settings.keys()]

        input_boxes = widgets.VBox([
            widgets.HBox([
                self.inputs[i],
                widgets.Label(helptext)
            ])
            for i, helptext in enumerate(_resource_settings.values())
        ])

        self.app = widgets.AppLayout(
            header = widgets.Label('Resource Requests'),
            left_sidebar = labels,
            center = input_boxes,
        )
    
    def show(self):
        from IPython.display import display
        display(self.app)
    
    def get_args(self):
        args = {name: self.inputs[i].value for i, name in enumerate(_resource_settings.keys())}
        for key, val in args.items():
            if val == '':
                raise Exception(f"Please set {key}")
        return args