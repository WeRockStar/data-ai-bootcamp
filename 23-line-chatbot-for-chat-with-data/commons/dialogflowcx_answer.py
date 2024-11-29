import os
from google.cloud import dialogflowcx_v3beta1 as dialogflow
from google.protobuf.json_format import MessageToDict


def detect_intent_text(text, session_id, language_code="th"):
    GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
    APP_LOCATION = os.environ["VERTEX_AGENT_LOCATION"]
    VERTEX_ENGINE = os.environ["VERTEX_AGENT_ID"]
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "privates/sa.json"

    agent = f"projects/{GCP_PROJECT_ID}/locations/{APP_LOCATION}/agents/{VERTEX_ENGINE}"
    session_path = f"{agent}/sessions/{session_id}"

    client_options = None
    if APP_LOCATION != "global":
        api_endpoint = f"{APP_LOCATION}-dialogflow.googleapis.com:443"
        client_options = {"api_endpoint": api_endpoint}

    session_client = dialogflow.SessionsClient(client_options=client_options)
    text_input = dialogflow.TextInput(text=text)
    query_input = dialogflow.QueryInput(text=text_input, language_code=language_code)
    request = dialogflow.DetectIntentRequest(
        session=session_path, query_input=query_input
    )
    response = session_client.detect_intent(request=request)
    response_dict = MessageToDict(response._pb)

    response_messages = response_dict["queryResult"]["generativeInfo"][
        "actionTracingInfo"
    ]["actions"][1]["toolUse"]["outputActionParameters"]["200"]["answer"]

    if response_messages == "":
        response_messages = response_dict["queryResult"]["generativeInfo"][
            "actionTracingInfo"
        ]["actions"][2]["agentUtterance"]["text"]

    return response_messages


if __name__ == "__main__":
    import uuid
    from manage_secret import load_secrets

    load_secrets(
        "/Users/punsiri/linedev/retail-chatbot-with-gemini/webhook_handler/scripts/line_secret.yml"
    )
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ] = "/Users/punsiri/linedev/retail-chatbot-with-gemini/webhook_handler/privates/sa.json"

    session_id = str(uuid.uuid4())
    response = detect_intent_text("ลดราคาไร", session_id=session_id)
