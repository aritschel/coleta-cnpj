FROM python:3.8.10
ENV WORKSPACE = /opt/installs
RUN mkdir -p $WORKSPACE
WORKDIR $WORKSPACE
COPY requirements.txt requirements.txt
RUN python3.8 -m pip install -r requirements.txt
COPY collect_cnpj .
CMD ["python3.8", "main.py"]
