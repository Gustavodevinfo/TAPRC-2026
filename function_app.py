import logging
import azure.functions as func
import os
import pandas as pd
import pyodbc


# ==========================================================
# VARIÁVEIS DE AMBIENTE
# ==========================================================

DB_SERVER = os.getenv("HOST")
DB_DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("USER")
DB_PASSWORD = os.getenv("PASSWORD")


# ==========================================================
# CONFIGURAÇÃO DA FUNÇÃO
# ==========================================================

app = func.FunctionApp()


@app.timer_trigger(
    schedule="*/1 * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def timer_trigger(myTimer: func.TimerRequest) -> None:

    logging.info("========================================")
    logging.info("Iniciando consulta das tabelas ITSМ")
    logging.info("========================================")

    # ------------------------------------------------------
    # Conexão com o banco
    # ------------------------------------------------------

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

        # -------------------------------------------------
        #Inicio
        # -------------------------------------------------

        # --------------------------------------------------
        # 1. CHAMADO_SLA
        # --------------------------------------------------

        logging.info("Consultando tabela CHAMADO_SLA...")

        chamado_sla = """
            SELECT *
            FROM [db-univille].itsm.chamado_sla
        """

        chamado_sla_selecionado = pd.read_sql(chamado_sla, conn)

        logging.info(
            f"Tabela CHAMADO_SLA consultada com sucesso. "
            f"Registros encontrados: {len(chamado_sla_selecionado)}"
        )

        # --------------------------------------------------
        # 2. CHAMADO_STATUS_HISTORICO
        # --------------------------------------------------

        logging.info("Consultando tabela CHAMADO_STATUS_HISTORICO...")

        chamado_status_historico = """
            SELECT *
            FROM [db-univille].itsm.chamado_status_historico
        """

        historico_selecionado = pd.read_sql(
            chamado_status_historico,
            conn
        )

        logging.info(
            f"Tabela CHAMADO_STATUS_HISTORICO consultada com sucesso. "
            f"Registros encontrados: {len(historico_selecionado)}"
        )

        # --------------------------------------------------
        # 3. CLIENTE_ORGANIZACAO
        # --------------------------------------------------

        logging.info("Consultando tabela CLIENTE_ORGANIZACAO...")

        cliente_organizacao = """
            SELECT *
            FROM [db-univille].itsm.cliente_organizacao
        """

        cliente_organizacao_selecionado = pd.read_sql(
            cliente_organizacao,
            conn
        )

        logging.info(
            f"Tabela CLIENTE_ORGANIZACAO consultada com sucesso. "
            f"Registros encontrados: "
            f"{len(cliente_organizacao_selecionado)}"
        )

        # --------------------------------------------------
        # 4. CSAT_AVALIACAO
        # --------------------------------------------------

        logging.info("Consultando tabela CSAT_AVALIACAO...")

        csat_avaliacao = """
            SELECT *
            FROM [db-univille].itsm.csat_avaliacao
        """

        csat_avaliacao_selecionado = pd.read_sql(
            csat_avaliacao,
            conn
        )

        logging.info(
            f"Tabela CSAT_AVALIACAO consultada com sucesso. "
            f"Registros encontrados: "
            f"{len(csat_avaliacao_selecionado)}"
        )

        # --------------------------------------------------
        # Finalização
        # --------------------------------------------------

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