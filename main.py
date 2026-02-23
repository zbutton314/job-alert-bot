import argparse
import warnings

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

import pandas as pd

from greenhouse import Greenhouse
from lever import Lever
import slack
from config import companies, keywords


def dedup_jobs(df: pd.DataFrame, jobs_file: str = "jobs.csv") -> pd.DataFrame:
    """Remove jobs already in jobs.csv and append new ones."""
    if df.empty:
        return df

    jobs_df = pd.read_csv(jobs_file, header=0, dtype=str)
    merge_cols = ["company", "id"]

    df_new = df.merge(jobs_df[merge_cols], on=merge_cols, how="left", indicator=True)
    df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

    if not df_new.empty:
        df_new.to_csv(jobs_file, mode="a", index=False, header=False)

    return df_new


def main(alert: bool = True):
    greenhouse_scraper = Greenhouse(keywords=keywords, remote_only=True)
    lever_scraper = Lever(keywords=keywords, remote_only=True)

    df_list = []
    print("Collecting jobs:")
    for company, config in companies.items():
        print(f"-- {company}")
        if config["ats"] == "greenhouse":
            df = greenhouse_scraper.get_jobs(url=config["url"], company_name=company)
            df_list.append(df)
        elif config["ats"] == "lever":
            df = lever_scraper.get_jobs(url=config["url"], company_name=company)
            df_list.append(df)
        else:
            print(f"Unsupported ATS: {config['ats']}")

    if not df_list:
        print("No jobs collected")
        return

    df_combined = pd.concat(df_list, ignore_index=True)
    df_new = dedup_jobs(df_combined)

    if not df_new.empty:
        if alert:
            slack.send_alert(df_new=df_new)
        else:
            print(f"Found {len(df_new)} new jobs (alert disabled)")
    else:
        print("No new jobs found")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-alert", dest="alert", action="store_false", default=True,
                        help="Disable Slack alert")
    args = parser.parse_args()
    main(alert=args.alert)