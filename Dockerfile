FROM ubuntu:20.04

ENV TZ=America/Sao_Paulo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update && apt-get upgrade -y

RUN apt-get install -y \
    openjdk-11-jdk \
    git \
    curl \
    wget \
    lsb-release \
    sudo \
    docker.io \
    tzdata \
    python3 \
    python3-pip \
    postgresql \
    postgresql-contrib \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /workspace/

RUN pip3 install --no-cache-dir -r /workspace/requirements.txt && rm -rf /root/.cache/pip

RUN wget https://downloads.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz && \
    tar -xvzf spark-3.4.4-bin-hadoop3.tgz && \
    mv spark-3.4.4-bin-hadoop3 /opt/spark && \
    rm spark-3.4.4-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

EXPOSE 4040 7077 5432

WORKDIR /workspace

COPY . /workspace/

RUN service postgresql start && \
    until pg_isready; do echo "Aguardando o PostgreSQL..."; sleep 2; done && \
    su - postgres -c "psql -c \"CREATE ROLE spark WITH LOGIN PASSWORD 'spark' SUPERUSER;\""

# Create tables in PostgreSQL
RUN service postgresql start && \
    su - postgres -c "psql -d sparkdb -f /workspace/collect_cnpj/utils/create_tables.sql"

CMD ["python3", "collect_cnpj/app.py"]