FROM apache/airflow:2.3.4
# These are the new libraries that have to be installed
RUN pip3 install pandas numpy scipy matplotlib seaborn tqdm db-dtypes ipykernel ipywidgets ipython
ENV PYTHONPATH="$PYTHONPATH:/opt/airflow/py_scripts"