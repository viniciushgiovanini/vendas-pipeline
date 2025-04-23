import psycopg2 as psq


def create_tables_if_not_exist(db_config):
    conn = psq.connect(**db_config)
    cursor = conn.cursor()

    create_cliente = """
    CREATE TABLE IF NOT EXISTS clientes (
        id_cliente SERIAL PRIMARY KEY,
        nome VARCHAR(100),
        email VARCHAR(150),
        documento VARCHAR(50),
        tipo_pessoa VARCHAR(50),
        tipo_contato VARCHAR(50)
    );
    """

    create_vendas = """
    CREATE TABLE IF NOT EXISTS vendas (
        id_venda SERIAL PRIMARY KEY,
        id_cliente INTEGER REFERENCES clientes(id_cliente),
        id_cliente_orfao BOOLEAN,
        valor NUMERIC(10, 2),
        data_venda DATE
    );
    """

    cursor.execute(create_cliente)
    cursor.execute(create_vendas)

    conn.commit()
    cursor.close()
    conn.close()
