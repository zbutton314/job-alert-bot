import os
import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def send_alert(df_new: pd.DataFrame) -> None:
    client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])

    text = ""
    for company in df_new["company"].unique():
        text += f"\n\n{company}:"
        df_company = df_new[df_new["company"] == company]
        for i, row in df_company.iterrows():
            title = row["title"]
            url = row["url"]
            text += f"\n• <{url}|{title}>"

    try:
        response = client.chat_postMessage(
            channel="#job-alerts",
            text=text.strip(),
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": text.strip()
                    }
                }
            ]
        )
    except SlackApiError as e:
        print(f"Slack error: {e.response['error']}")