FROM python:3.8.10

ENV WORKSPACE=/opt/installs
RUN mkdir -p $WORKSPACE
WORKDIR $WORKSPACE
RUN apt-get update && apt-get install -y \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt requirements.txt
RUN python3.8 -m pip install -r requirements.txt
COPY collect_cnpj .
ENV SPARK_HOME=/usr/local/spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native
ENV SPARK_LOCAL_IP=10.0.0.58
CMD ["python3.8", "main.py"]
