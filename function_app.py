import logging
import os
import requests
import azure.functions as func

app = func.FunctionApp()


@app.timer_trigger(
    schedule="*/30 * * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def TimerCallHttp(myTimer: func.TimerRequest) -> None:

    logging.info("Timer Trigger executado.")

    url = os.environ.get("HTTP_FUNCTION_URL")
    parametro = "Teste"

    try:
        response = requests.get(
            url,
            params={"nome": parametro}
        )

        logging.info(
            f"Resposta da HTTP Function: {response.text}"
        )

    except Exception as e:
        logging.error(
            f"Erro ao chamar a HTTP Function: {e}"
        )


@app.route(
    route="HttpTrigger",
    auth_level=func.AuthLevel.ANONYMOUS
)
def HttpTrigger(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("HTTP Trigger recebeu uma requisição.")

    nome = req.params.get("nome")

    if nome:
        logging.info(f"Parâmetro recebido: {nome}")

        texto_resposta = f"Parâmetro recebido com sucesso: {nome}."

        return func.HttpResponse(
            texto_resposta,
            status_code=200
        )

    return func.HttpResponse(
        "Nenhum parâmetro foi informado. Utilize o parâmetro 'nome' na URL.",
        status_code=400
    )