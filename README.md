# Pipeline de Vendas

Projeto para criação de um pipeline de dados (ETL).

### Arquivos do Projeto

- O script principal contendo os métodos desenvolvidos é o **etl.py**
- Para visualizar o resultado do ETL inserido no PostgreSQL foi feito um dump das tabelas clientes e vendas.
  - cliente_dump.csv
  - vendas_dump.csv

### Explicações do Tratamento de dados

- Tabela Clientes

  - O nome do cliente foi padronizado, removendo espaços iniciais e finais e removendo prefixos.
  - Foi criado um método para validar o email, utilizando REGEX, para verificar se a string corresponde a um formato válido de email.
  - O dado de documento contém tanto CPF quanto CNPJ, e estava com pontuação. Foi realizada a normalização. Caso o documento informado seja inválido, o valor "desconhecido" é atribuído ao campo.
  - O tipo de pessoa possuía muitos valores ausentes. Foi feita uma inferência com base no tipo de documento. Caso o documento esteja com erro, o valor "desconhecido" é atribuído ao campo.
  - O tipo de contato também possuía valores ausentes. Foi feita a verificação, mantendo o email com valor padrão. Caso o email não seja informado, é atribuído o valor "desconhecido" ao campo.

- Tabela Vendas

  - A coluna data_venda foi convertida para o tipo datetime.

  - Foi feita a verificação do id_cliente para garantir que o id existe na tabela Clientes. Criou-se uma nova coluna chamada id_cliente_orfao.

  - A coluna id_cliente_orfao marca o registro como órfão (True) caso o id_cliente não exista mais na tabela Clientes, e False caso o id_cliente exista.

  - A coluna valor foi tratada para garantir que os números estejam no tipo float. Foi feita a verificação caso o número seja negativo ou nulo, e criado um padrão de 2 casas decimais para o valor do produto.

### Como rodar o Projeto

- Criar o ambiente virtual para instalar as dependências do Python:

```cmd
python -m venv env
```

- Entrar no ambiente virtual (Windows):

```cmd
env/Scripts/activate
```

- Entrar no ambiente virtual (Linux):

```cmd
source env/bin/activate
```

- Instalar as dependências do projeto:

```cmd
pip install -r requirements.txt
```

- Inserir as credenciais do banco de dados:
  - Para inserir as credenciais, preencha o dicionário no início do arquivo etl.py, denominado **DB_CONFIG**.
  - **OBS**: A porta padrão do banco PostgreSQL é 5432.

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'nome_do_seu_banco',
    'user': 'postgres',
    'password': 'sua_senha'
}
```

## Linguagens de Desenvolvimento

<img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" width="50px"/>&nbsp;
<img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/pandas/pandas-original.svg" width="50px"/>&nbsp;
<img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/postgresql/postgresql-original.svg" width="50px"/>

## Desenvolvimento ✏

**Feito por**: [Vinícius Henrique Giovanini](https://github.com/viniciushgiovanini)
