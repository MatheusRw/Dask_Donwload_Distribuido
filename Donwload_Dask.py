import os
import paramiko
import stat
import posixpath
import zipfile
import io
import time
from dask.distributed import Client, as_completed

# Configuração do cliente Dask
client = Client('tcp://192.168.14.114:8786',
                timeout='60s',
                asynchronous=False,
                heartbeat_interval=10000)

def list_mvno_subfolders(hostname, port, username, password, remote_path):
    """
    Conecta ao SFTP, lista os subdiretórios do caminho base e retorna os caminhos completos.
    """
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname, port, username, password)
        
        sftp = ssh_client.open_sftp()
        sftp.chdir(remote_path)
        
        # Lista os itens que são diretórios (mvnos)
        mvno_list = [entry.filename for entry in sftp.listdir_attr() if stat.S_ISDIR(entry.st_mode)]
        
        sftp.close()
        ssh_client.close()
        
        # Garante que a concatenação use sempre "/"
        full_paths = [posixpath.join(remote_path, mvno) for mvno in mvno_list]
        return full_paths
    except Exception as e:
        print(f"Erro ao acessar o SFTP: {e}")
        return []

def should_download_zip(filename, date_range):
    """
    Verifica se o arquivo ZIP deve ser baixado com base no padrão do nome e no intervalo de datas.
    """
    try:
        if not filename.endswith(".zip") or not filename.startswith("cdr_"):
            return False
        # Extrai a data do nome do arquivo (exemplo: cdr_20241015_...)
        parts = filename.split('_')
        if len(parts) < 2:
            return False
        file_date = parts[1]
        return date_range[0] <= file_date <= date_range[1]
    except Exception as e:
        print(f"Erro ao filtrar o arquivo {filename}: {e}")
        return False

def should_extract_file(file_in_zip, file_types):
    """
    Verifica se um arquivo dentro do ZIP deve ser extraído com base nos tipos especificados.
    """
    for file_type in file_types:
        if file_type in file_in_zip:
            return True
    return False

def download_and_extract_zip(sftp, filename, file_types):
    """
    Baixa o arquivo ZIP de forma escalonada (em chunks), descompacta-o em memória e extrai os arquivos
    que atendem ao filtro para o diretório local.
    """
    try:
        local_dir = r"C:\Users\matheus.weinert\Desktop\CDR2"
        os.makedirs(local_dir, exist_ok=True)
        
        # Abre o arquivo remoto e lê em chunks para evitar carregar tudo de uma vez
        with sftp.open(filename, 'rb') as remote_file:
            zip_bytes = io.BytesIO()
            chunk_size = 1024 * 1024  # 1 MB por chunk
            while True:
                chunk = remote_file.read(chunk_size)
                if not chunk:
                    break
                zip_bytes.write(chunk)
            zip_bytes.seek(0)
            
            with zipfile.ZipFile(zip_bytes) as zip_file:
                # Itera pelos arquivos internos e extrai se atenderem aos critérios
                for file_in_zip in zip_file.namelist():
                    if should_extract_file(file_in_zip, file_types):
                        zip_file.extract(file_in_zip, local_dir)
                        print(f'Extraído: {file_in_zip} para {local_dir}')
    except Exception as e:
        print(f"Erro ao baixar e descompactar o arquivo {filename}: {e}")

def process_directory(hostname, port, username, password, directory_path, date_range, file_types):
    """
    Processa uma subpasta: conecta, lista os arquivos, filtra os ZIPs que atendem aos critérios e executa o download/extracção.
    A conexão SFTP é estabelecida uma única vez para todos os arquivos na pasta.
    """
    print(f"Processando diretório: {directory_path}")
    ssh_client = None
    sftp = None
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname, port, username, password)
        sftp = ssh_client.open_sftp()
        sftp.chdir(directory_path)
        
        for filename in sftp.listdir():
            if should_download_zip(filename, date_range):
                print(f"Baixando e descompactando: {filename} na pasta {directory_path}")
                download_and_extract_zip(sftp, filename, file_types)
                
    except Exception as e:
        print(f"Erro ao processar o diretório {directory_path}: {e}")
    finally:
        if sftp:
            sftp.close()
        if ssh_client:
            ssh_client.close()

if __name__ == "__main__":
    HOSTNAME = "*******"
    PORT = "*******"
    USERNAME = "*******"
    PASSWORD = ""*******""
    REMOTE_PATH = "/files/CDR"
    DATE_RANGE = ("20241001", "20241031")  # Intervalo de datas (YYYYMMDD)
    FILE_TYPES = ["CDR_SMS_MT"]  # Tipos de arquivos a filtrar dentro dos ZIPs
    
    start_time = time.time()
    
    # Lista os caminhos completos das subpastas
    subfolders = list_mvno_subfolders(HOSTNAME, PORT, USERNAME, PASSWORD, REMOTE_PATH)
    
    # Submete o processamento de cada subpasta como tarefa paralela no Dask
    tasks = []
    for folder in subfolders:
        task = client.submit(process_directory, HOSTNAME, PORT, USERNAME, PASSWORD,
                               folder, DATE_RANGE, FILE_TYPES)
        tasks.append(task)
    
    # Aguarda a conclusão de todas as tarefas
    for future in as_completed(tasks):
        try:
            future.result()
        except Exception as e:
            print(f"Erro na tarefa: {e}")
    
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Tempo total de execução: {execution_time:.2f} segundos")
