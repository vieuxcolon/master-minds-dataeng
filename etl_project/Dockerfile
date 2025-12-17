FROM apache/airflow:3.1.0

# Copy requirements
ADD requirements.txt .

# Install Airflow (same version as base image) + extra dependencies
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
