import requests
import pandas as pd


def fetch_jobs(company: str) -> pd.DataFrame:
    """Fetch raw JSON from the Greenhouse jobs API and convert to simplified data frame."""
    url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"

    response = requests.get(url)
    response.raise_for_status()

    jobs = response.json().get("jobs", [])
    if not jobs:
        return pd.DataFrame()

    df = pd.json_normalize(jobs)
    df = df.rename(columns={"location.name": "location", "company_name": "company", "absolute_url": "url"})
    df["ats"] = "greenhouse"
    cols = ["company", "ats", "id", "title", "location", "url"]
    df = df[cols].astype(str)

    return df


def filter_jobs(df: pd.DataFrame, keywords: list[str], remote_only: bool) -> pd.DataFrame:
    if remote_only:
        df = df[df["location"].str.lower().str.contains("remote")]

    keyword_results = []
    for keyword in keywords:
        df_sub = df[df["title"].str.lower().str.contains(keyword)]
        keyword_results.append(df_sub)

    df_filtered = pd.concat(keyword_results, ignore_index=True)

    return df_filtered


def dedup_jobs(df: pd.DataFrame) -> pd.DataFrame:
    jobs_df = pd.read_csv("jobs.csv", header=0, dtype=str)
    merge_cols = ["company", "id"]

    df_new = df.merge(jobs_df[merge_cols], on=merge_cols, how="left", indicator=True)
    df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

    df_new.to_csv("jobs.csv", mode="a", index=False, header=False)

    return df_new
