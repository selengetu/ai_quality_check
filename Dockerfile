FROM apache/airflow:2.9.1-python3.11

# Install as the airflow user (required by the official image)
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Bake project files into the image so tasks read from the container's
# overlay filesystem, not the host-mounted VirtioFS volume.
# VirtioFS on Apple Silicon causes OSError Errno 35 on certain read()
# patterns used by dbt and shutil — baking avoids this entirely.
COPY --chown=airflow:root dbt/             /opt/airflow/dbt/
COPY --chown=airflow:root ai_engine/       /opt/airflow/ai_engine/
COPY --chown=airflow:root great_expectations/ /opt/airflow/great_expectations/
