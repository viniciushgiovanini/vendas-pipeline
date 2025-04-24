import pandas as pd
import psycopg2 as psq
import re
from datetime import datetime
from io import StringIO
pd.set_option('display.max_rows', None)

DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'nome_do_seu_banco',
    'user': 'postgres',
    'password': 'sua_senha'
}

################################
#        Create Table          #
################################


def create_tables_if_not_exist(db_config):
    """
    This function checks the existence of tables in the database and creates them if not.

    :param db_config: Configuration dictionary for connecting to PostgreSQL.
    :type db_config: dict
    """
    try:
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
    except:
        print('An error occurred while creating the tables in the database')


############################
#        Extração          #
############################


def extract(path="./data/data.xlsx", sheet_name="clientes"):
    """
    Extracts data from an Excel spreadsheet.

    :param path: Path to the Excel file.
    :type path: str

    :param sheet_name: Name of the sheet to extract.
    :type sheet_name: str

    :return: Data extracted from the specified sheet as a DataFrame.
    :rtype: pandas.DataFrame
    """
    return pd.read_excel(path, sheet_name=sheet_name)


######################################
#        Tratamento Cliente          #
######################################
def cleaning_name(word):
    """
    Cleans a name string by removing common honorifics and extra spaces.

    :param word: Name string to clean.
    :type word: str
    :return: Cleaned name string.
    :rtype: str
    """

    remove_words_list = ["Dra.", "Srta.", "Sr.", "Sra."]

    for prefix in remove_words_list:
        if word.startswith(prefix):
            word = word.replace(prefix, "")
            break

    word = word.strip()
    return word


def validate_email(word):
    """
    Validates email using regex.

    :param word: String to validate as an email.
    :type word: str
    :return:True if the string is a valid email, False otherwise.
    :rtype: bool
    """
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'

    if re.fullmatch(regex, word):
        return True
    else:
        return False


def cleaning_doc(word):
    """
    Cleans and validates a CPF or CNPJ document number.

    This function removes common formatting characters and checks if the resulting string has a valid length:
    - 11 digits for CPF
    - 14 digits for CNPJ

    :param word: Document string to clean.
    :type word: str
    :return: Cleaned document number if valid, otherwise 'desconhecido'.
    :rtype: str
    """

    word = str(word)

    word = re.sub(r'[.,-/]', '', word)

    if len(word) == 11 or len(word) == 14:
        return word.strip()
    else:
        return "desconhecido"


def cleaning_person_type(word, n_documento):
    """
    Cleans or infers the person type (Legal or Individual) based on the document number.

    If the provided type is invalid or not a string, the function infers it based on document length:
    - 14 digits → "Jurídica"
    - 11 digits → "Física"

    :param word: Raw person type.
    :type word: str
    :param n_documento: Cleaned document number, without punctuation.
    :type n_documento: str
    :return: Cleaned or inferred person type.
    :rtype: str
    """

    try:
        if n_documento == "desconhecido":
            return "desconhecido"
        elif (not isinstance(word, str)) and len(n_documento) == 14:
            return "Juridica"
        elif (not isinstance(word, str)) and len(n_documento) == 11:
            return "Fisica"
        else:
            return word.lower().title()
    except:
        return "desconhecido"


def cleaning_contact_type(word, email):
    """
    Cleans or infers the contact type based on the provided value.

    If the contact type (`word`) is a valid string, it will be formatted in title case.
    Otherwise, the function will try to infer the contact type based on the email field.

    :param word: Contact type.
    :type word: str
    :param email: Email address associated with the contact, used as fallback.
    :type email: str
    :return: Cleaned or inferred contact type.
    :rtype: str
    """

    if isinstance(word, str):
        return word.lower().title()
    else:
        if email != "desconhecido":
            return "Email"
        else:
            return "desconhecido"


def transform_cliente(df):
    """
    Transforms and cleans a client DataFrame by applying various formatting and validation functions
    to standardize fields such as name, document, person type, contact type, and email.

    :param df: Raw DataFrame containing client data.
    :type df: pandas.DataFrame
    :return: Transformed and cleaned DataFrame.
    :rtype: pandas.DataFrame
    """

    for index, row in df.iterrows():
        df.loc[index, 'nome'] = cleaning_name(row["nome"])
        df.loc[index, 'documento'] = cleaning_doc(row["documento"])
        df.loc[index, 'tipo_pessoa'] = cleaning_person_type(
            word=row["tipo_pessoa"], n_documento=cleaning_doc(row["documento"]))
        df.loc[index, 'tipo_contato'] = cleaning_contact_type(
            row["tipo_contato"], email=row["email"])

        if validate_email(word=row["email"]):
            df.loc[index, 'email'] = row["email"]
        else:
            df.loc[index, 'email'] = "email_invalido"

    for col in df:
        df[col] = df[col].fillna("desconhecido")

    return df


####################################
#        Tratamento Vendas         #
####################################

def string_to_date(date):
    """
    Converts a date from string or pandas Timestamp to a Python date object.

    :param date: Date to convert. Can be a string (format 'YYYY-MM-DD') or pd.Timestamp.
    :type date: str or pd.Timestamp
    :return: Converted Python date object.
    :rtype: datetime.date
    """

    if isinstance(date, str):
        return datetime.strptime(date, '%Y-%m-%d').date()
    elif type(date) == pd.Timestamp:
        return date.date()
    else:
        return datetime.strptime("1970-01-01", '%Y-%m-%d').date()


def verify_id_cliente(id, df_clientes):
    """
    Checks if the given client ID is not present in the clients DataFrame.

    :param id: Client ID to check.
    :type id: int
    :param df_clientes: DataFrame containing valid client IDs.
    :type df_clientes: pandas.DataFrame
    :return: True if ID is not found (orphan), False if ID exists.
    :rtype: bool
    """
    if id in df_clientes["id_cliente"].values:
        return False
    else:
        return True


def validate_values_numbers(value):
    """
    Validates and formats a numeric value. Ensures it's non-negative and formatted as float with 2 decimals.

    :param value: Numeric value to validate and format.
    :type value: float or int
    :return: Validated float as string with 2 decimal places.
    :rtype: float
    """

    if (not isinstance(value, float)) and (not isinstance(value, int)):
        return 0.0
    elif value < 0:
        return "{:.2f}".format(float(abs(value)))
    else:
        return "{:.2f}".format(float(value))


def transform_vendas(df, df_clientes):
    """
    Transforms and cleans the sales DataFrame by standardizing dates, validating client IDs,
    and formatting numeric values.

    :param df: Raw sales DataFrame.
    :type df: pandas.DataFrame
    :param df_clientes: DataFrame containing valid client IDs.
    :type df_clientes: pandas.DataFrame
    :return: Cleaned and transformed sales DataFrame.
    :rtype: pandas.DataFrame
    """

    for index, row in df.iterrows():
        df.loc[index, "data_venda"] = pd.to_datetime(
            string_to_date(row["data_venda"]))
        df.loc[index, "id_cliente_orfao"] = verify_id_cliente(
            id=row["id_cliente"], df_clientes=df_clientes)
        df.loc[index, "valor"] = float(validate_values_numbers(row["valor"]))

    return df
################################
#        Load Postgres         #
################################


def load_to_postgres(df, table_name, db_config):
    """
    Loads a DataFrame into a PostgreSQL table.

    :param df: DataFrame containing the data to be loaded into PostgreSQL.
    :type df: pandas.DataFrame
    :param table_name: Name of the table where the data will be inserted.
    :type table_name: str
    :param db_config: Configuration dictionary for connecting to PostgreSQL.
    :type db_config: dict
    """
    try:
        conn = psq.connect(**db_config)
        cursor = conn.cursor()

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cursor.copy_from(buffer, table_name, sep=',',
                         null='', columns=df.columns)

        conn.commit()
        cursor.close()
        conn.close()
        print("Data inserted successfully!")
    except:
        print('An error occurred while inserting data into the database !')


############
#   Main   #
############


def main():
    create_tables_if_not_exist(db_config=DB_CONFIG)

    df_clientes = extract(path="./data/data.xlsx", sheet_name="clientes")
    df_clientes_tratado = transform_cliente(df=df_clientes)
    load_to_postgres(df=df_clientes_tratado,
                     table_name="clientes", db_config=DB_CONFIG)

    df_vendas = extract(path="./data/data.xlsx", sheet_name="vendas")
    df_vendas_tratado = transform_vendas(
        df=df_vendas, df_clientes=df_clientes)
    load_to_postgres(df=df_vendas_tratado,
                     table_name="vendas", db_config=DB_CONFIG)


if __name__ == "__main__":
    main()
