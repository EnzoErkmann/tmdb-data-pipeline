FROM apache/airflow:2.9.2

# Copy the requirements file into the image
COPY requirements.txt /requirements.txt

# Keep the default Airflow user to install dependencies
USER airflow    

RUN pip install --no-cache-dir -r /requirements.txt