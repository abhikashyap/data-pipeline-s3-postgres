FROM jupyter/all-spark-notebook:latest

USER root
RUN pip install --no-cache-dir -U \
    jupyterlab-lsp \
    python-lsp-server \
    python-lsp-black \
    python-lsp-ruff \
    pylsp-mypy \
    pylsp-rope
USER ${NB_UID}