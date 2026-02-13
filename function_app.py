import azure.functions as func
import os
import base64
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime
import json
import nest_asyncio
nest_asyncio.apply()

app = func.FunctionApp()

# === CONFIGURATION ===
JIRA_BASE_URL = "https://aerialproduct.atlassian.net"
JIRA_DOMAIN = "aerialproduct.atlassian.net"
CONTAINER_ID = "ari:cloud:townsquare::site/11b5321b-ff75-4f9e-b925-34136a9fc3a3"
MAX_CONCURRENT = 40

def get_credentials():
    return (
        os.environ.get("JIRA_EMAIL"),
        os.environ.get("JIRA_API_TOKEN")
    )

# === GOALS FETCH ===
def fetch_goals():
    import requests
    JIRA_EMAIL, JIRA_API_TOKEN = get_credentials()
    auth_string = f"{JIRA_EMAIL}:{JIRA_API_TOKEN}"
    base64_auth = base64.b64encode(auth_string.encode('ascii')).decode('ascii')
    headers = {
        "Authorization": f"Basic {base64_auth}",
        "Content-Type": "application/json"
    }
    all_goals = []
    has_next_page = True
    cursor = None
    graphql_url = f"https://{JIRA_DOMAIN}/gateway/api/graphql"
    
    while has_next_page:
        after_clause = f', after: "{cursor}"' if cursor else ""
        query = f"""
        query GetGoals {{
            goals_search(
                containerId: "{CONTAINER_ID}"
                searchString: ""
                first: 100{after_clause}
            ) {{
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
                edges {{
                    node {{
                        id
                        key
                        name
                    }}
                }}
            }}
        }}
        """
        response = requests.post(graphql_url, headers=headers, json={"query": query})
        result = response.json()
        
        if "errors" in result or "data" not in result:
            break
        
        search_result = result["data"]["goals_search"]
        for edge in search_result["edges"]:
            goal = edge["node"]
            all_goals.append({
                "Goal_ARI": goal["id"],
                "Goal_Key": goal["key"],
                "Goal_Name_Lookup": goal["name"]
            })
        
        has_next_page = search_result["pageInfo"]["hasNextPage"]
        cursor = search_result["pageInfo"].get("endCursor")
    
    return pd.DataFrame(all_goals)

# === JIRA ASYNC FUNCTIONS ===
async def fetch_json(session, url):
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                return await resp.json()
    except:
        pass
    return None

async def get_all_issues(session, projects, start_date, end_date):
    projects_str = ",".join(projects)
    jql = f"project IN ({projects_str}) AND worklogDate >= '{start_date}' AND worklogDate <= '{end_date}'"
    issues = []
    next_page_token = None
    while True:
        url = f"{JIRA_BASE_URL}/rest/api/3/search/jql?jql={jql}&fields=project,summary,parent,issuetype,customfield_10485&maxResults=100"
        if next_page_token:
            url += f"&nextPageToken={next_page_token}"
        data = await fetch_json(session, url)
        if not data or "issues" not in data:
            break
        issues.extend(data["issues"])
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break
    return issues

async def fetch_issue_worklogs(session, issue_key, semaphore):
    async with semaphore:
        worklogs = []
        start_at = 0
        while True:
            url = f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}/worklog?maxResults=5000&startAt={start_at}"
            data = await fetch_json(session, url)
            if not data or "worklogs" not in data:
                break
            worklogs.extend(data["worklogs"])
            if start_at + len(data["worklogs"]) >= data.get("total", 0):
                break
            start_at += 5000
        return worklogs

async def get_epic_info(session, parent_key, issue_type, cache, semaphore):
    cache_key = f"{parent_key}_{issue_type}"
    if cache_key in cache:
        return cache[cache_key]
    
    async with semaphore:
        if issue_type in ("Story", "Task", "Triage Task", "Tech Enabler", "Bug", "Solution", "Defect"):
            url = f"{JIRA_BASE_URL}/rest/api/3/issue/{parent_key}?fields=summary,issuetype,customfield_10485"
            parent_data = await fetch_json(session, url)
            if not parent_data:
                cache[cache_key] = None
                return None
            parent_fields = parent_data.get("fields", {})
            parent_type = parent_fields.get("issuetype", {}).get("name", "")
            
            if parent_type != "Epic":
                cache[cache_key] = None
                return None
            
            goals_list = parent_fields.get("customfield_10485") or []
            result = {
                "Epic_Key": parent_key,
                "Epic_Name": parent_fields.get("summary"),
                "Goal_ARI": goals_list[0].get("id") if goals_list else None
            }
            cache[cache_key] = result
            return result
        
        if issue_type == "Sub-task":
            url = f"{JIRA_BASE_URL}/rest/api/3/issue/{parent_key}?fields=parent"
            parent_data = await fetch_json(session, url)
            if not parent_data:
                cache[cache_key] = None
                return None
            
            grandparent = parent_data.get("fields", {}).get("parent")
            if not grandparent:
                cache[cache_key] = None
                return None
            
            grandparent_key = grandparent.get("key")
            epic_url = f"{JIRA_BASE_URL}/rest/api/3/issue/{grandparent_key}?fields=summary,issuetype,customfield_10485"
            epic_data = await fetch_json(session, epic_url)
            if not epic_data:
                cache[cache_key] = None
                return None
            
            epic_fields = epic_data.get("fields", {})
            epic_type = epic_fields.get("issuetype", {}).get("name", "")
            
            if epic_type != "Epic":
                cache[cache_key] = None
                return None
            
            goals_list = epic_fields.get("customfield_10485") or []
            result = {
                "Epic_Key": grandparent_key,
                "Epic_Name": epic_fields.get("summary"),
                "Goal_ARI": goals_list[0].get("id") if goals_list else None
            }
            cache[cache_key] = result
            return result
        
        cache[cache_key] = None
        return None

async def fetch_worklogs_async(projects, start_date, end_date):
    JIRA_EMAIL, JIRA_API_TOKEN = get_credentials()
    filter_start = datetime.strptime(start_date, "%Y-%m-%d").date()
    filter_end = datetime.strptime(end_date, "%Y-%m-%d").date()
    auth = aiohttp.BasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    epic_cache = {}
    
    async with aiohttp.ClientSession(auth=auth) as session:
        issues = await get_all_issues(session, projects, start_date, end_date)
        if not issues:
            return pd.DataFrame()
        worklog_tasks = [fetch_issue_worklogs(session, issue["key"], semaphore) for issue in issues]
        all_worklogs = await asyncio.gather(*worklog_tasks)
        issue_worklogs = dict(zip([i["key"] for i in issues], all_worklogs))
        
        epic_tasks = [get_epic_info(session, issue.get("fields", {}).get("parent", {}).get("key"), 
                                    issue.get("fields", {}).get("issuetype", {}).get("name"),
                                    epic_cache, semaphore) 
                      for issue in issues if issue.get("fields", {}).get("parent")]
        await asyncio.gather(*epic_tasks)
        
        rows = []
        for issue in issues:
            fields = issue.get("fields", {})
            project = fields.get("project", {})
            parent = fields.get("parent")
            issue_type = fields.get("issuetype", {})
            issue_type_name = issue_type.get("name")
            
            epic_info = None
            if issue_type_name == "Epic":
                goals_list = fields.get("customfield_10485") or []
                epic_info = {
                    "Epic_Key": issue["key"],
                    "Epic_Name": fields.get("summary"),
                    "Goal_ARI": goals_list[0].get("id") if goals_list else None
                }
            elif parent:
                cache_key = f"{parent['key']}_{issue_type_name}"
                epic_info = epic_cache.get(cache_key)

            for wl in issue_worklogs.get(issue["key"], []):
                started_str = wl.get("started", "")
                try:
                    started_dt = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
                    logged_date = started_dt.date()
                except:
                    continue
                if not (filter_start <= logged_date <= filter_end):
                    continue
                time_spent_seconds = wl.get("timeSpentSeconds", 0)
                author = wl.get("author", {})
                rows.append({
                    "Issue_Key": issue["key"],
                    "Issue_Name": fields.get("summary"),
                    "Issue_Type": issue_type_name,
                    "Project_Key": project.get("key"),
                    "Project_Name": project.get("name"),
                    "Epic_Key": epic_info["Epic_Key"] if epic_info else None,
                    "Epic_Name": epic_info["Epic_Name"] if epic_info else None,
                    "Goal_ARI": epic_info["Goal_ARI"] if epic_info else None,
                    "User_Name": author.get("displayName"),
                    "started": started_dt.isoformat(),
                    "Logged_Date": logged_date.isoformat(),
                    "Logged_Hours": round(time_spent_seconds / 3600, 2)
                })
        return pd.DataFrame(rows)

@app.route(route="worklogs", auth_level=func.AuthLevel.FUNCTION)
def get_worklogs(req: func.HttpRequest) -> func.HttpResponse:
    # Get parameters
    projects = req.params.get('projects', 'CARE,UMVISION,PFORM,POP,OPS,ENG,CHANGE,AFNDRY')
    start_date = req.params.get('start_date', '2026-01-01')
    end_date = req.params.get('end_date', '2026-02-28')
    
    projects_list = [p.strip() for p in projects.split(',')]
    
    # Fetch goals
    goals_df = fetch_goals()
    
    # Fetch worklogs
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    df = loop.run_until_complete(fetch_worklogs_async(projects_list, start_date, end_date))
    loop.close()
    
    # Join goals
    if not df.empty and not goals_df.empty:
        df = df.merge(goals_df[["Goal_ARI", "Goal_Key", "Goal_Name_Lookup"]], on="Goal_ARI", how="left")
        df.rename(columns={"Goal_Name_Lookup": "Goal"}, inplace=True)
    else:
        df["Goal"] = None
        df["Goal_Key"] = None
    
    return func.HttpResponse(
        df.to_json(orient='records', date_format='iso'),
        mimetype="application/json"
    )
```

**requirements.txt:**
```
azure-functions
aiohttp
pandas
nest_asyncio
requests