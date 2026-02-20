import pandas as pd

import greenhouse
import slack


companies = ["mozilla", "missionlane"]
keywords = ["data scientist"]


def main():
    df_list = []
    for company in companies:
        print(f"Collecting jobs from {company}")
        df = greenhouse.fetch_jobs(company=company)
        df = greenhouse.filter_jobs(df=df, keywords=keywords, remote_only=True)
        df_list.append(df)
    
    df_combined = pd.concat(df_list, ignore_index=True)
    df_new = greenhouse.dedup_jobs(df_combined)

    if df_new.shape[0] > 0:
        slack.send_alert(df_new=df_new)


if __name__ == "__main__":
    main()