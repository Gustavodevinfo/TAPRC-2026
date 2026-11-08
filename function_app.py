
import logging
import os
import io
from datetime import datetime, timezone

import azure.functions as func
import pandas as pd
import pyodbc

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ==================================================
# CONFIGURAÇÕES
# ==================================================

# SQL SERVER DE ORIGEM - BRONZE
DB_SERVER = os.getenv("HOST")
DB_DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("USER")
DB_PASSWORD = os.getenv("PASSWORD")

# AZURE DATA LAKE
STORAGE_CONNECTION_STRING = os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING"
)
CONTAINER_NAME = "bronze"

# SQL SERVER DE DESTINO - PRATA
# Nomeei assim mas pode mudar para os nomes que quiser, desde que sejam consistentes com o .env
# E com as variáveis de ambiente que estao na Azure
PRATA_SERVER = os.getenv("PRATA_HOST")
PRATA_DATABASE = os.getenv("PRATA_DATABASE")
PRATA_USER = os.getenv("PRATA_USER")
PRATA_PASSWORD = os.getenv("PRATA_PASSWORD")

SCHEMA_PRATA = "itsm"

# Pastas da Bronze e tabelas da Prata
TABELAS = {
    "categoria": "categoria",
    "cliente_organizacao": "cliente_organizacao",
    "fila": "fila",
    "sla": "sla",
    "analista": "analista",
    "solicitante": "solicitante",
    "chamados": "chamado",
    "chamado_sla": "chamado_sla",
    "chamado_status_historico": "chamado_status_historico",
    "csat_avaliacao": "csat_avaliacao"
}

# IDs originais que são IDENTITY no destino
ID_COLUNAS = {
    "categoria": "id_categoria",
    "cliente_organizacao": "id_cliente_organizacao",
    "fila": "id_fila",
    "sla": "id_sla",
    "analista": "id_analista",
    "solicitante": "id_solicitante",
    "chamado": "id_chamado",
    "chamado_sla": "id_chamado_sla",
    "chamado_status_historico": "id_chamado_status_historico",
    "csat_avaliacao": "id_csat_avaliacao"
}

app = func.FunctionApp()


# ==================================================
# BRONZE - SALVAR NO DATA LAKE
# ==================================================

def salvar_no_datalake(df: pd.DataFrame, tabela: str):

    if df.empty:
        logging.warning(
            f"Tabela {tabela} está vazia. Nada será salvo."
        )
        return

    agora = datetime.now(timezone.utc)

    timestamp_str = agora.strftime("%Y%m%dT%H%M%SZ")

    ano = agora.strftime("%Y")
    mes = agora.strftime("%m")
    dia = agora.strftime("%d")

    pasta_destino = (
        f"itsm/{tabela}/ano={ano}/mes={mes}/dia={dia}"
    )

    nome_arquivo = f"{tabela}_{timestamp_str}.csv"

    caminho_completo = (
        f"{pasta_destino}/{nome_arquivo}"
    )

    try:

        service_client = (
            DataLakeServiceClient.from_connection_string(
                STORAGE_CONNECTION_STRING
            )
        )

        filesystem = service_client.get_file_system_client(
            CONTAINER_NAME
        )

        try:
            filesystem.create_directory(pasta_destino)
        except ResourceExistsError:
            pass

        csv_data = df.to_csv(index=False)

        file_client = filesystem.get_file_client(
            caminho_completo
        )

        file_client.upload_data(
            csv_data.encode("utf-8"),
            overwrite=True
        )

        logging.info(
            f"Arquivo salvo: {caminho_completo}"
        )

    except Exception:
        logging.exception(
            f"Erro ao salvar {tabela} no Data Lake."
        )
        raise


# ==================================================
# BRONZE - EXECUTAR EXTRAÇÕES
# ==================================================

def executar_bronze():

    logging.info("Iniciando extração da Bronze.")

    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_DATABASE};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        "Encrypt=yes;"
        "Connection Timeout=30;"
    )

    consultas = {
        "chamado_sla": (
            "SELECT * FROM [db-univille].itsm.chamado_sla"
        ),
        "chamado_status_historico": (
            "SELECT * FROM [db-univille].itsm.chamado_status_historico"
        ),
        "cliente_organizacao": (
            "SELECT * FROM [db-univille].itsm.cliente_organizacao"
        ),
        "csat_avaliacao": (
            "SELECT * FROM [db-univille].itsm.csat_avaliacao"
        ),
        "fila": (
            "SELECT * FROM [db-univille].itsm.fila"
        ),
        "sla": (
            "SELECT * FROM [db-univille].itsm.sla"
        ),
        "solicitante": (
            "SELECT * FROM [db-univille].itsm.solicitante"
        ),
        "categoria": (
            "SELECT * FROM [db-univille].itsm.categoria"
        ),
        "analista": (
            "SELECT * FROM [db-univille].itsm.analista"
        ),
        "chamados": (
            "SELECT * FROM [db-univille].itsm.chamado"
        )
    }

    try:

        for tabela, consulta in consultas.items():

            logging.info(
                f"Consultando tabela {tabela}..."
            )

            dados = pd.read_sql_query(
                consulta,
                conn
            )

            logging.info(
                f"{tabela}: {len(dados)} registros."
            )

            salvar_no_datalake(
                dados,
                tabela
            )

        logging.info("Extração Bronze concluída.")

    except Exception:
        logging.exception("Erro durante a execução da Bronze.")
        raise

    finally:
        conn.close()
        logging.info("Conexão com a origem encerrada.")


# ==================================================
# PRATA - CONEXÕES
# ==================================================

def conectar_bronze():

    service_client = (
        DataLakeServiceClient.from_connection_string(
            STORAGE_CONNECTION_STRING
        )
    )

    return service_client.get_file_system_client(
        file_system=CONTAINER_NAME
    )


def conectar_prata():

    conexao = URL.create(
        "mssql+pyodbc",
        username=PRATA_USER,
        password=PRATA_PASSWORD,
        host=PRATA_SERVER,
        database=PRATA_DATABASE,
        query={
            "driver": "ODBC Driver 18 for SQL Server",
            "Encrypt": "yes",
            "TrustServerCertificate": "no"
        }
    )

    return create_engine(
        conexao,
        fast_executemany=True
    )


# ==================================================
# PRATA - BUSCAR SNAPSHOT MAIS RECENTE
# ==================================================

# Lista e busca os csvs mais recentes
# O mais recente vira o df que vamos usar

def buscar_snapshot(filesystem, pasta):

    caminhos = filesystem.get_paths(
        path=f"itsm/{pasta}"
    )

    arquivos = [
        arquivo for arquivo in caminhos
        if not arquivo.is_directory
        and arquivo.name.lower().endswith(".csv")
    ]

    if not arquivos:

        logging.warning(
            f"Nenhum CSV encontrado para {pasta}."
        )

        return None

    arquivo_recente = max(
        arquivos,
        key=lambda arquivo: arquivo.last_modified
    )

    logging.info(
        f"Snapshot encontrado: {arquivo_recente.name}"
    )

    file_client = filesystem.get_file_client(
        arquivo_recente.name
    )

    conteudo = file_client.download_file().readall()

    return pd.read_csv(
        io.BytesIO(conteudo),
        dtype=str
    )



# ==================================================
# PRATA - ALIMENTAR BANCO
# ==================================================

# Essa funcao vai alimentar o banco
# Pega somente o csv mais recente e atualiza prata, ou adiciona quando necessario

def alimentar_banco(engine, tabela, dados):

    if dados is None or dados.empty:

        logging.info(
            f"Tabela {tabela}: sem dados para carregar."
        )

        return

    # Identificador original da tabela
    coluna_id = ID_COLUNAS[tabela]

    dados = dados.copy()

    if coluna_id not in dados.columns:
        raise ValueError(
            f"Coluna {coluna_id} não encontrada em {tabela}."
        )

    if dados[coluna_id].isna().any():
        raise ValueError(
            f"Existem IDs nulos em {tabela}."
        )

    if dados[coluna_id].duplicated().any():
        raise ValueError(
            f"Existem IDs duplicados em {tabela}."
        )

    # dt_processamento é controlado exclusivamente pela Prata
    dados = dados.drop(
        columns=["dt_processamento"],
        errors="ignore"
    )

    # Converte valores vazios para NULL
    dados = dados.astype(object).where(
        pd.notna(dados),
        None
    )

    # Identificadores SQL
    nome_staging = f"#stg_{tabela}"

    tabela_sql = f"[{SCHEMA_PRATA}].[{tabela}]"

    staging_sql = f"[{nome_staging}]"

    # Colunas recebidas do CSV
    colunas = list(dados.columns)

    colunas_update = [
        coluna for coluna in colunas
        if coluna != coluna_id
    ]

    # Protege os nomes das colunas no SQL
    def identificar(nome):
        return "[" + nome.replace("]", "]]") + "]"

    # Monta atualização
    set_colunas = [
        f"destino.{identificar(coluna)} = "
        f"origem.{identificar(coluna)}"
        for coluna in colunas_update
    ]

    # Atualiza a data somente quando houver alteração
    set_colunas.append(
        "destino.[dt_processamento] = GETDATE()"
    )

    set_sql = ", ".join(set_colunas)

    comparacao_sql = ", ".join(
        f"destino.{identificar(coluna)}"
        for coluna in colunas_update
    )

    comparacao_origem = ", ".join(
        f"origem.{identificar(coluna)}"
        for coluna in colunas_update
    )

    # Monta inserção
    colunas_insert = [
        identificar(coluna)
        for coluna in colunas
    ]

    colunas_insert.append(
        identificar("dt_processamento")
    )

    colunas_insert_sql = ", ".join(colunas_insert)

    colunas_select = [
        f"origem.{identificar(coluna)}"
        for coluna in colunas
    ]

    colunas_select.append("GETDATE()")

    colunas_select_sql = ", ".join(colunas_select)

    try:

        with engine.begin() as conexao:

            # Cria staging temporária e carrega os dados
            dados.to_sql(
                name=nome_staging,
                con=conexao,
                if_exists="replace",
                index=False,
                chunksize=1000
            )

            # Preserva os IDs originais nas colunas IDENTITY
            conexao.execute(
                text(
                    f"SET IDENTITY_INSERT {tabela_sql} ON"
                )
            )

            try:

                # Atualiza somente registros que realmente mudaram
                if colunas_update:

                    update_sql = f"""
                        UPDATE destino
                        SET {set_sql}
                        FROM {tabela_sql} AS destino
                        INNER JOIN {staging_sql} AS origem
                            ON destino.{identificar(coluna_id)}
                            = origem.{identificar(coluna_id)}
                        WHERE EXISTS (
                            SELECT {comparacao_sql}
                            EXCEPT
                            SELECT {comparacao_origem}
                        );
                    """

                    resultado_update = conexao.execute(
                        text(update_sql)
                    )

                    atualizados = resultado_update.rowcount

                else:
                    atualizados = 0

                # Insere somente registros inexistentes
                insert_sql = f"""
                    INSERT INTO {tabela_sql}
                    ({colunas_insert_sql})

                    SELECT {colunas_select_sql}
                    FROM {staging_sql} AS origem

                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {tabela_sql} AS destino
                        WHERE destino.{identificar(coluna_id)}
                        = origem.{identificar(coluna_id)}
                    );
                """

                resultado_insert = conexao.execute(
                    text(insert_sql)
                )

                inseridos = resultado_insert.rowcount

            finally:

                conexao.execute(
                    text(
                        f"SET IDENTITY_INSERT {tabela_sql} OFF"
                    )
                )

            # Remove somente a tabela temporária
            conexao.execute(
                text(f"DROP TABLE {staging_sql}")
            )

        logging.info(
            f"{tabela}: {inseridos} inseridos, "
            f"{atualizados} atualizados."
        )

    except Exception:

        logging.exception(
            f"Erro ao alimentar tabela {tabela}."
        )

        raise



# ==================================================
# PRATA - EXECUTAR CARGA
# ==================================================

def executar_prata():

    logging.info("Iniciando alimentação da Prata.")

    filesystem = conectar_bronze()
    engine = conectar_prata()

    try:

        for pasta, tabela in TABELAS.items():

            logging.info(
                f"Processando tabela {tabela}..."
            )

            dados = buscar_snapshot(
                filesystem,
                pasta
            )

            alimentar_banco(
                engine,
                tabela,
                dados
            )

        logging.info("Alimentação da Prata concluída.")

    except Exception:

        logging.exception(
            "Erro durante a alimentação da Prata."
        )

        raise

    finally:
        engine.dispose()


# ==================================================
# EXECUTAR PIPELINE COMPLETO
# ==================================================

def executar_pipeline():

    # Primeiro atualiza a Bronze
    executar_bronze()

    # Somente após sucesso, atualiza a Prata
    executar_prata()


# ==================================================
# TIMER TRIGGER
# ==================================================

@app.timer_trigger(
    schedule="0 0 9 * * *", #Altera aqui para fazer testes, esta para todos os dias as 6 da manha de Brasilia
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=True
)
def timer_trigger(myTimer: func.TimerRequest) -> None:

    logging.info("========================================")
    logging.info("Iniciando processamento ITSM.")
    logging.info("========================================")

    if myTimer.past_due:
        logging.warning("Execução do Timer está atrasada.")

    executar_pipeline()

    logging.info("========================================")
    logging.info("Processamento ITSM finalizado.")
    logging.info("========================================")
