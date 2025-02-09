from utils.connect_db import connect_to_postgres, close_connection
import os


def load_to_postgres(db_name: str, file_name: str):
    """Carrega os dados extraídos para o banco PostgreSQL."""
    # Conectar ao banco de dados
    connection = connect_to_postgres()
    if connection is None:
        return

    try:
        cursor = connection.cursor()

        table_name = db_name.lower()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100),
            email VARCHAR(100)
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        CSV_PATH = "bronze/csv"
        with open(os.path.join(CSV_PATH, file_name), 'r') as file:
            next(file)
            cursor.copy_from(file, table_name, sep=',')
            connection.commit()
        print(f"Dados carregados na tabela {table_name} com sucesso!")

    except Exception as e:
        print(f"Erro ao carregar dados no PostgreSQL: {e}")
    finally:
        close_connection(connection)
