FROM apache/airflow:2.9.2

# Copia o arquivo de requisitos para dentro da imagem
COPY requirements.txt /requirements.txt

# Mantém o usuário padrão do Airflow para instalar as dependências
USER airflow    

RUN pip install --no-cache-dir -r /requirements.txt