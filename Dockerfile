FROM jupyter/pyspark-notebook

USER root

COPY requirements.txt /tmp/

RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER jovyan
