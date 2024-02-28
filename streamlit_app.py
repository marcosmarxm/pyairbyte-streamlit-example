import airbyte as ab
import pandas as pd
import streamlit as st

source = ab.get_source(
    "source-github",
    install_if_missing=True,
    config={
        "repositories": ["airbytehq/quickstarts"],
        "credentials": {
            "personal_access_token": ab.get_secret("GITHUB_PERSONAL_ACCESS_TOKEN"),
        },
    },
)
source.check()
source.set_streams(["pull_requests", "issues", "stargazers"])
cache = ab.get_default_cache()
result = source.read(cache=cache)

stars = cache.streams["stargazers"].to_pandas()
stars["starred_at"] = pd.to_datetime(stars["starred_at"])
stars_trend = stars.resample("W", on="starred_at").size()

issues = cache.streams["issues"].to_pandas()
issues["created_at"] = pd.to_datetime(issues["created_at"])
issues_trend = issues.resample("W", on="created_at").size()

prs = cache.streams["pull_requests"].to_pandas()
prs["created_at"] = pd.to_datetime(issues["created_at"])
prs_trend = prs.resample("W", on="created_at").size()


st.title("Github Repo Summary")
st.markdown("""
Welcome to our new data app! 
We've crafted this using three awesome tools: `streamlit`, `pyAirbyte`, and `pandas` dataframes.
It pulls information from the public repo `airbytehq/quickstarts`.

But, **what is it `pyAirbyte`?**
            
PyAirbyte brings the power of more 200 Airbyte connector to every Python developer.
Simplify data ingestion from any source and accelerate the creation of fantastic data apps.
            
Happy hacking! ‍💻 🚀 🎉
""")

metric_col_1, metric_col_2 = st.columns(2)

st.subheader('Cumulative Star Growth Over Time')
metric_col_1.metric(label="Total Stars", value=sum(stars_trend), delta=int(stars_trend.cumsum()[-1] / stars_trend.cumsum()[-2]))
metric_col_2.metric(label="Last Month", value=stars_trend.cumsum()[-2], delta=" ")
st.line_chart(stars_trend.cumsum())

issues_col, pr_col = st.columns(2)

issues_col.subheader('Issues per Week')
issues_col.bar_chart(issues_trend)
issues_col.subheader('New Issues')
issues_col.dataframe(issues[["title", "created_at"]].sort_values(by='created_at', ascending=False).head(10))

pr_col.subheader('Pull Requests per Week')
pr_col.bar_chart(prs_trend)
pr_col.subheader('New Pull Requests')
pr_col.dataframe(prs[["title", "created_at"]].sort_values(by='created_at', ascending=False).head(10))

st.subheader("Conclusion")
st.markdown("""
I developed this app out of curiosity to explore `streamlit`, 
and the introduction of the `pyairbyte` library provided the perfect chance. 
It's truly impressive how effortless it is to construct the app 😲 benefiting 
from the quick feedback loop that `streamlit` offers. The resilience to errors is noteworthy
as failures don't disrupt the entire app but only the local component. The data used for 
this app it's kinda simple, for more intricate projects, I might opt to initiate in a notebook to 
accelerate the data model to be used by the app.             
""")