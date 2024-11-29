import os
from google.cloud import dialogflowcx_v3beta1 as dialogflow
from google.protobuf.json_format import MessageToDict


def detect_intent_text(text, session_id, language_code="th"):

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
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sa.json"
    GCP_PROJECT_ID = "GCP_PROJECT_ID"
    APP_LOCATION = "APP_LOCATION"
    VERTEX_ENGINE = "VERTEX_ENGINE"
    search_query = "แนะนำครีมกันแดดหน่อย"
    results = detect_intent_text(search_query=search_query)
    print(results)