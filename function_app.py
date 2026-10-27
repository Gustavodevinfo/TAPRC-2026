import logging
import os
from datetime import datetime, timezone

import azure.functions as func
import pandas as pd
import pyodbc
from azure.storage.filedatalake import DataLakeServiceClient

DB_SERVER = os.getenv("HOST")
DB_DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("USER")
DB_PASSWORD = os.getenv("PASSWORD")

STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "bronze"


def salvar_no_datalake(df: pd.DataFrame, tabela: str):
    if df.empty:
        logging.warning(f"Tabela {tabela} está vazia. Nada será salvo.")
        return

    agora = datetime.now(timezone.utc)
    timestamp_str = agora.strftime("%Y%m%dT%H%M%SZ")

    ano = agora.strftime("%Y")
    mes = agora.strftime("%m")
    dia = agora.strftime("%d")

    pasta_destino = f"itsm/{tabela}/ano={ano}/mes={mes}/dia={dia}"

    nome_arquivo = f"{tabela}_{timestamp_str}.csv"
    caminho_completo = f"{pasta_destino}/{nome_arquivo}"

    try:
        service_client = DataLakeServiceClient.from_connection_string(
            STORAGE_CONNECTION_STRING
        )
        file_system_client = service_client.get_file_system_client(CONTAINER_NAME)

        file_system_client.create_directory(pasta_destino)

        csv_data = df.to_csv(index=False)

        file_client = file_system_client.get_file_client(caminho_completo)
        file_client.upload_data(csv_data.encode("utf-8"), overwrite=True)

        logging.info(f"Arquivo salvo com sucesso: {caminho_completo}")

    except Exception as e:
        logging.error(f"Erro ao salvar {tabela} no Data Lake: {e}")
        raise


app = func.FunctionApp()


@app.timer_trigger(
    schedule="*/1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def timer_trigger(myTimer: func.TimerRequest) -> None:

    logging.info("========================================")
    logging.info("Iniciando consulta das tabelas ITSM")
    logging.info("========================================")

    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_DATABASE};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        "Encrypt=yes;"
        "Connection Timeout=30;"
    )

    try:
        logging.info("Consultando tabela CHAMADO_SLA...")
        chamado_sla = "SELECT * FROM [db-univille].itsm.chamado_sla"
        chamado_sla_selecionado = pd.read_sql(chamado_sla, conn)
        logging.info(
            f"Tabela CHAMADO_SLA consultada. Registros: {len(chamado_sla_selecionado)}"
        )
        salvar_no_datalake(chamado_sla_selecionado, "chamado_sla")

        logging.info("Consultando tabela CHAMADO_STATUS_HISTORICO...")
        chamado_status_historico = (
            "SELECT * FROM [db-univille].itsm.chamado_status_historico"
        )
        historico_selecionado = pd.read_sql(chamado_status_historico, conn)
        logging.info(
            f"Tabela CHAMADO_STATUS_HISTORICO consultada. "
            f"Registros: {len(historico_selecionado)}"
        )
        salvar_no_datalake(historico_selecionado, "chamado_status_historico")

        logging.info("Consultando tabela CLIENTE_ORGANIZACAO...")
        cliente_organizacao = "SELECT * FROM [db-univille].itsm.cliente_organizacao"
        cliente_organizacao_selecionado = pd.read_sql(cliente_organizacao, conn)
        logging.info(
            f"Tabela CLIENTE_ORGANIZACAO consultada. "
            f"Registros: {len(cliente_organizacao_selecionado)}"
        )
        salvar_no_datalake(cliente_organizacao_selecionado, "cliente_organizacao")

        logging.info("Consultando tabela CSAT_AVALIACAO...")
        csat_avaliacao = "SELECT * FROM [db-univille].itsm.csat_avaliacao"
        csat_avaliacao_selecionado = pd.read_sql(csat_avaliacao, conn)
        logging.info(
            f"Tabela CSAT_AVALIACAO consultada. "
            f"Registros: {len(csat_avaliacao_selecionado)}"
        )
        salvar_no_datalake(csat_avaliacao_selecionado, "csat_avaliacao")

        logging.info("Consultando tabela FILA...")
        fila = "SELECT * FROM [db-univille].itsm.fila"
        fila_selecionada = pd.read_sql(fila, conn)
        logging.info(f"Tabela FILA consultada. Registros: {len(fila_selecionada)}")
        salvar_no_datalake(fila_selecionada, "fila")

        logging.info("Consultando tabela SLA...")
        sla = "SELECT * FROM [db-univille].itsm.sla"
        sla_selecionada = pd.read_sql(sla, conn)
        logging.info(f"Tabela SLA consultada. Registros: {len(sla_selecionada)}")
        salvar_no_datalake(sla_selecionada, "sla")

        logging.info("Consultando tabela SOLICITANTE...")
        solicitante = "SELECT * FROM [db-univille].itsm.solicitante"
        solicitante_selecionado = pd.read_sql(solicitante, conn)
        logging.info(
            f"Tabela SOLICITANTE consultada. Registros: {len(solicitante_selecionado)}"
        )
        salvar_no_datalake(solicitante_selecionado, "solicitante")

        logging.info("Consultando tabela CATEGORIA...")
        categorias = "SELECT * FROM [db-univille].itsm.categoria"
        categorias_selecionada = pd.read_sql(categorias, conn)
        logging.info(
            f"Tabela CATEGORIA consultada. Registros: {len(categorias_selecionada)}"
        )
        salvar_no_datalake(categorias_selecionada, "categoria")

        logging.info("Consultando tabela ANALISTA...")
        analista = "SELECT * FROM [db-univille].itsm.analista"
        analista_selecionado = pd.read_sql(analista, conn)
        logging.info(
            f"Tabela ANALISTA consultada. Registros: {len(analista_selecionado)}"
        )
        salvar_no_datalake(analista_selecionado, "analista")

        logging.info("Consultando tabela CHAMADO...")
        chamado = "SELECT * FROM [db-univille].itsm.chamado"
        chamado_selecionado = pd.read_sql(chamado, conn)
        logging.info(
            f"Tabela CHAMADO consultada. Registros: {len(chamado_selecionado)}"
        )
        salvar_no_datalake(chamado_selecionado, "chamados")

        logging.info("========================================")
        logging.info("Todas as consultas foram realizadas.")
        logging.info("Processo finalizado com sucesso.")
        logging.info("========================================")

    except Exception as e:
        logging.error(f"Erro ao consultar as tabelas: {e}")
        raise
    finally:
        conn.close()
        logging.info("Conexão com o banco encerrada.")

    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function executed.")
