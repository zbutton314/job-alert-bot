from typing import List

import requests
import pandas as pd


class Lever:
    def __init__(self, keywords: List[str], remote_only: bool = True):
        self.keywords = keywords
        self.remote_only = remote_only

    def fetch_jobs(self, url: str, company_name: str) -> pd.DataFrame:
        """Fetch jobs from Lever API and return simplified dataframe."""
        response = requests.get(url)
        response.raise_for_status()

        jobs = response.json()
        if not jobs:
            return pd.DataFrame()

        df = pd.json_normalize(jobs)
        df = df.rename(columns={
            "text": "title",
            "hostedUrl": "url",
            "categories.location": "location"
        })

        df["company"] = company_name
        df["ats"] = "lever"

        return df

    def filter_jobs(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter jobs by remote workplaceType and keywords."""
        if df.empty:
            return df

        if self.remote_only:
            df = df[df["workplaceType"] == "remote"]

        keyword_results = []
        for keyword in self.keywords:
            df_sub = df[df["title"].str.lower().str.contains(keyword, na=False)]
            keyword_results.append(df_sub)

        if not keyword_results:
            return pd.DataFrame()

        df_filtered = pd.concat(keyword_results, ignore_index=True)
        df_filtered = df_filtered.drop_duplicates(subset=["company", "id"])

        cols = ["company", "ats", "id", "title", "location", "url"]
        return df_filtered[cols].astype(str)

    def get_jobs(self, url: str, company_name: str) -> pd.DataFrame:
        """Fetch and filter jobs in one call."""
        df = self.fetch_jobs(url, company_name)
        return self.filter_jobs(df)
