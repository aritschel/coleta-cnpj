# coleta-cnpj

## Descrição

Este projeto coleta, transforma e carrega dados de CNPJ utilizando Apache Spark e PostgreSQL.

## Pré-requisitos

- Docker
- Docker Compose

## Configuração

1. Clone o repositório:
    ```sh
    git clone https://github.com/seu-usuario/coleta-cnpj.git
    cd coleta-cnpj
    ```

2. Construa a imagem Docker:
    ```sh
    docker build -t coleta-cnpj .
    ```

3. Inicie os serviços com Docker Compose:
    ```sh
    docker-compose up -d
    ```

4. Ative o ambiente virtual Python:
    ```sh
    source .venv/bin/activate
    ```

## Execução

Para executar o pipeline de ETL, utilize o seguinte comando:
```sh
docker exec -it coleta-cnpj-spark-master python3 collect_cnpj/app.py
```

## Estrutura do Projeto

- `collect_cnpj/`: Contém os módulos principais do projeto.
  - `extract.py`: Responsável por baixar e extrair os dados.
  - `load.py`: Carrega os dados extraídos no PostgreSQL.
  - `transform.py`: Aplica transformações nos dados.
  - `utils/`: Contém funções utilitárias e configurações.
- `Dockerfile`: Define a imagem Docker para o projeto.
- `docker-compose.yml`: Configura os serviços Docker.
- `requirements.txt`: Lista de dependências Python.

## Próximos Passos

- Criar uma DAG para agendar a execução do pipeline a cada 3 meses.
