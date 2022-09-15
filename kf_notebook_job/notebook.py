import nbformat
import papermill

def load_notebook(nb_path):
    return nbformat.read(nb_path, as_version=4)

def get_kernel_name(notebook):
    return notebook.metadata.kernelspec.name

def get_parameters(nb_path, warn=True):
    param = papermill.inspect_notebook('train.ipynb')
    if param == dict() and warn:
        import logging
        logging.warn("No parameters cell found. Have you tagged a cell with the parameters keyword?")
    return param