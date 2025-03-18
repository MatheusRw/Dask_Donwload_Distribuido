import os
import pandas as pd
from sqlalchemy import create_engine
from dask.distributed import Client

# Inicializa o cliente Dask
client = Client('tcp://*****:*****',
                timeout='60s',
                asynchronous=False,
                heartbeat_interval=10000)

# Função para processar e salvar os dados no SQL Server
def process_file(file_path, db_config, required_columns):
    try:
        # Cria o engine dentro da função para evitar problemas de serialização
        connection_string = (
            f"mssql+pyodbc://{db_config['username']}:{db_config['password']}@"
            f"{db_config['server']}/{db_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
        )
        engine = create_engine(connection_string)
        
        # Carrega o CSV com pandas
        df = pd.read_csv(file_path, sep=";", header=0, dtype=str)

        # Verifica se todas as colunas esperadas existem no DataFrame
        existing_columns = set(df.columns)
        missing_columns = required_columns - existing_columns

        if missing_columns:
            print(f"Erro: Colunas ausentes no arquivo {file_path}: {missing_columns}")
            return  # Pula o arquivo problemático

        # Seleciona apenas as colunas necessárias
        df = df[list(required_columns)]

        # Insere os dados no banco em batch
        df.to_sql('view_cdr_sms_mt_last_month_1024_arquivos_v3',
                  con=engine,
                  index=False,
                  if_exists='append',
                  chunksize=5000)

        print(f'Arquivo inserido com sucesso: {file_path}')

    except Exception as e:
        print(f"Ocorreu um erro ao processar o arquivo {file_path}: {e}")

# Função executada no worker para listar os arquivos CSV
def listar_arquivos(local_dir):
    return [os.path.join(local_dir, filename)
            for filename in os.listdir(local_dir) if filename.endswith(".csv")]

# Função principal para submeter as tarefas usando Dask
def process_files_and_save_to_db(local_dir, db_config):
    # Executa a função listar_arquivos em todos os workers
    arquivos_dict = client.run(listar_arquivos, local_dir)
    
    # Conjunto das colunas esperadas
    required_columns = {
        "idCDR", "brandID", "idSubscription", "subscriptionID", "msisdn",
        "originator", "destination", "startDate", "parseDay", "parseDate",
        "trafficUnits", "trafficUnitsRatedSession", "packageID", "ratingPackRef",
        "destinationPattern"
    }

    futures = []
    for worker, file_list in arquivos_dict.items():
        for file_path in file_list:
            # Submete a tarefa para o worker específico que possui o arquivo
            future = client.submit(process_file,
                                   file_path,
                                   db_config,
                                   required_columns,
                                   workers=[worker])
            futures.append(future)

    # Aguarda a conclusão de todas as tarefas
    client.gather(futures)

# Exemplo de chamada da função
local_dir = r"C:\Users\matheus.weinert\Desktop\CDR2"  # Esse caminho deve ser válido em cada worker
db_config = {
    'server': '******',
    'database': '******',
    'username': '******',
    'password': '******'
}

process_files_and_save_to_db(local_dir, db_config)
