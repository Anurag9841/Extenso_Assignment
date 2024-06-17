FROM apache/airflow:2.9.1

ADD requirements.txt .
ADD env.bat .


# Install apache-airflow and other dependencies
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==2.1.3
RUN ./env.bat