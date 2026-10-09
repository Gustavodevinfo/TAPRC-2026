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