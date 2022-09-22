FROM apache/airflow:2.3.4
# These are the new libraries that have to be installed
RUN pip3 install pandas numpy scipy matplotlib seaborn tqdm db-dtypes ipykernel ipywidgets ipython
ENV PYTHONPATH="$PYTHONPATH:/opt/airflow/py_scripts"
ENV GOOGLE_APPLICATION_CREDENTIALS="$GOOGLE_APPLICATION_CREDENTIALS:/opt/airflow/py_scripts/application_default_credentials.json"