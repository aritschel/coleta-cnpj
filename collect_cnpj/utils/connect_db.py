import psycopg2


def connect_to_postgres():
    try:
        connection = psycopg2.connect(
            dbname="sparkdb",
            user="spark",
            password="spark",
            host="127.0.0.1",
            port="5432"
        )
        return connection
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


def close_connection(connection):
    if connection:
        connection.close()
        print("Conexão fechada.")
