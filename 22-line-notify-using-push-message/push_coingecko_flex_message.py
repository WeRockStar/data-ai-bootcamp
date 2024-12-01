import os
import sys
from datetime import datetime

from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    PushMessageRequest,
    FlexMessage,
    FlexContainer,
)

# Cryptocurrency data
crypto_data = {
    "bitcoin": {
        "usd": 96761,
        "thb": 3319393,
        "usd_24h_change": 1.7356,
        "thb_24h_change": 1.3825,
    },
    "ethereum": {
        "usd": 3574.35,
        "thb": 122618,
        "usd_24h_change": -1.2458,
        "thb_24h_change": -1.6214,
    },
    "tether": {
        "usd": 1.001,
        "thb": 34.33,
        "usd_24h_change": -0.0085,
        "thb_24h_change": -0.3556,
    },
}


def generate_crypto_flex(data):
    icon_img = {
        "bitcoin": "https://assets.coingecko.com/coins/images/1/standard/bitcoin.png",
        "ethereum": "https://assets.coingecko.com/coins/images/279/standard/ethereum.png",
        "tether": "https://assets.coingecko.com/coins/images/325/standard/Tether.png",
    }
    contents = []
    for name, info in data.items():
        contents.append(
            {
                "type": "box",
                "layout": "horizontal",
                "contents": [
                    {
                        "type": "image",
                        "url": icon_img[name],
                        "size": "xs",
                        "aspectMode": "cover",
                        "aspectRatio": "1:1",
                    },
                    {
                        "type": "text",
                        "text": f"{name.capitalize()} ({name[:3].upper()})",
                        "weight": "bold",
                        "size": "md",
                        "margin": "md",
                    },
                ],
                "spacing": "md",
                "margin": "lg",
            }
        )
        # Add prices
        contents.append(
            {
                "type": "box",
                "layout": "horizontal",
                "contents": [
                    {"type": "text", "text": "USD:", "size": "sm", "flex": 2},
                    {
                        "type": "text",
                        "text": f"${info['usd']:,}",
                        "size": "sm",
                        "flex": 3,
                    },
                    {"type": "text", "text": "THB:", "size": "sm", "flex": 2},
                    {
                        "type": "text",
                        "text": f"฿{info['thb']:,}",
                        "size": "sm",
                        "flex": 3,
                    },
                ],
            }
        )
        # Add 24h change
        contents.append(
            {
                "type": "text",
                "text": f"24h Change: {info['usd_24h_change']:.2f}%",
                "size": "sm",
                "color": "#00FF00" if info["usd_24h_change"] > 0 else "#FF0000",
            }
        )
        contents.append({"type": "separator", "margin": "sm"})

    # Remove the last separator
    if contents and contents[-1].get("type") == "separator":
        contents.pop()
    flex_bubles = {
        "type": "bubble",
        "size": "mega",
        "body": {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {
                    "type": "text",
                    "text": "Cryptocurrency Prices",
                    "weight": "bold",
                    "size": "lg",
                    "align": "center",
                },
                {
                    "type": "text",
                    "text": f"as of {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "weight": "bold",
                    "size": "sm",
                    "align": "center",
                },
                {"type": "separator", "margin": "md"},
                {
                    "type": "box",
                    "layout": "vertical",
                    "margin": "lg",
                    "contents": contents,
                },
            ],
        },
    }
    return FlexMessage(
        alt_text="Cryptocurrency Prices", contents=FlexContainer.from_dict(flex_bubles)
    )


flex_message = generate_crypto_flex(crypto_data)


outer_lib_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(outer_lib_path)
from commons.manage_secret import load_secrets

load_secrets("line_secret.yml")

configuration = Configuration(access_token=os.getenv("CHANNEL_ACCESS_TOKEN"))

# Initialize the Messaging API client once
with ApiClient(configuration) as api_client:
    line_bot_api = MessagingApi(api_client)
    user_id = os.getenv("LINE_USER_ID")
    line_bot_api.push_message(PushMessageRequest(to=user_id, messages=[flex_message]))
