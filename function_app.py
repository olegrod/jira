import azure.functions as func
import os
import base64
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, date, timedelta
import json
import calendar

app = func.FunctionApp()

# === CONFIGURATION ===
JIRA_BASE_URL = "https://aerialproduct.atlassian.net"
JIRA_DOMAIN = "aerialproduct.atlassian.net"
CONTAINER_ID = "ari:cloud:townsquare::site/11b5321b-ff75-4f9e-b925-34136a9fc3a3"
MAX_CONCURRENT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2.0


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
                pageInfo {{ hasNextPage endCursor }}
                edges {{
                    node {{ id key name }}
                }}
            }}
        }}
        """
        response = requests.post(
            graphql_url, headers=headers, json={"query": query}, timeout=30
        )
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


# === ASYNC HELPERS ===

async def fetch_json(session, url, retries=MAX_RETRIES):
    """GET with exponential backoff retry. Returns None on permanent failure."""
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status == 429:
                    retry_after = float(resp.headers.get("Retry-After", RETRY_DELAY * (attempt + 1)))
                    await asyncio.sleep(retry_after)
                    continue
                if resp.status >= 500:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                return None
        except asyncio.TimeoutError:
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
        except Exception:
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
    return None


async def get_all_issues(session, projects, start_date, end_date):
    projects_str = ",".join(projects)
    jql = (
        f"project IN ({projects_str}) "
        f"AND worklogDate >= '{start_date}' "
        f"AND worklogDate <= '{end_date}'"
    )
    issues = []
    next_page_token = None

    while True:
        payload = {
            "jql": jql,
            "fields": [
                "project", "summary", "parent", "issuetype",
                "customfield_10485", "customfield_10001"
            ],
            "maxResults": 1000
        }
        if next_page_token:
            payload["nextPageToken"] = next_page_token

        for attempt in range(MAX_RETRIES):
            try:
                async with session.post(
                    f"{JIRA_BASE_URL}/rest/api/3/search/jql",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    if resp.status != 200:
                        return issues
                    data = await resp.json()
                    break
            except Exception:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
        else:
            break

        batch = data.get("issues", [])
        if not batch:
            break
        issues.extend(batch)
        next_page_token = data.get("nextPageToken")
        if not next_page_token or data.get("isLast", False):
            break

    return issues


async def fetch_all_epics(session, projects):
    """
    Fetch ALL epics for the given projects in a single paginated JQL query.
    Returns a dict keyed by Epic_Key -> {Epic_Key, Epic_Name, Goal_ARI}.
    Called once before month chunks and reused across all of them.
    """
    projects_str = ",".join(projects)
    jql = f"project IN ({projects_str}) AND issuetype = Epic"
    epics = {}
    next_page_token = None

    while True:
        payload = {
            "jql": jql,
            "fields": ["summary", "customfield_10485"],
            "maxResults": 1000
        }
        if next_page_token:
            payload["nextPageToken"] = next_page_token

        for attempt in range(MAX_RETRIES):
            try:
                async with session.post(
                    f"{JIRA_BASE_URL}/rest/api/3/search/jql",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    if resp.status != 200:
                        return epics
                    data = await resp.json()
                    break
            except Exception:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
        else:
            break

        for issue in data.get("issues", []):
            fields = issue.get("fields", {})
            goals_list = fields.get("customfield_10485") or []
            epics[issue["key"]] = {
                "Epic_Key": issue["key"],
                "Epic_Name": fields.get("summary"),
                "Goal_ARI": goals_list[0].get("id") if goals_list else None
            }

        next_page_token = data.get("nextPageToken")
        if not next_page_token or data.get("isLast", False):
            break

    return epics


async def fetch_issue_worklogs(session, issue_key, semaphore):
    """
    Fetch first page to get total, then fetch remaining pages in parallel.
    Returns flat list of all worklog dicts.
    """
    async with semaphore:
        first_url = f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}/worklog?maxResults=100&startAt=0"
        first = await fetch_json(session, first_url)
        if not first or "worklogs" not in first:
            return []

        worklogs = list(first["worklogs"])
        total = first.get("total", 0)

        if total <= 100:
            return worklogs

        offsets = range(100, total, 100)
        tasks = [
            fetch_json(
                session,
                f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}/worklog?maxResults=100&startAt={offset}"
            )
            for offset in offsets
        ]
        pages = await asyncio.gather(*tasks)
        for page in pages:
            if page and "worklogs" in page:
                worklogs.extend(page["worklogs"])

        return worklogs


async def resolve_subtask_epic(session, parent_key, epic_dict, semaphore, subtask_cache):
    """
    For a sub-task: fetch its immediate parent to get the grandparent key,
    then look up the grandparent in the pre-loaded epic_dict.
    Results cached in subtask_cache to avoid duplicate calls for shared parents.
    """
    if parent_key in subtask_cache:
        return subtask_cache[parent_key]

    async with semaphore:
        url = f"{JIRA_BASE_URL}/rest/api/3/issue/{parent_key}?fields=parent"
        parent_data = await fetch_json(session, url)

    result = None
    if parent_data:
        grandparent = parent_data.get("fields", {}).get("parent")
        if grandparent:
            grandparent_key = grandparent.get("key")
            result = epic_dict.get(grandparent_key)  # look up in pre-loaded epics

    subtask_cache[parent_key] = result
    return result


async def fetch_worklogs_for_range(session, projects, start_date, end_date, epic_dict):
    """
    Fetch worklogs for a single date range chunk.
    epic_dict is pre-loaded and shared across all chunks.
    Returns DataFrame.
    """
    filter_start = datetime.strptime(start_date, "%Y-%m-%d").date()
    filter_end = datetime.strptime(end_date, "%Y-%m-%d").date()

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    subtask_cache = {}  # cache for sub-task intermediate parent lookups

    issues = await get_all_issues(session, projects, start_date, end_date)
    if not issues:
        return pd.DataFrame()

    # 1. Fetch all worklogs in parallel
    worklog_tasks = [
        fetch_issue_worklogs(session, issue["key"], semaphore)
        for issue in issues
    ]
    all_worklogs = await asyncio.gather(*worklog_tasks)
    issue_worklogs = dict(zip([i["key"] for i in issues], all_worklogs))

    # 2. Resolve sub-task parents — use issues dataset first, API only for misses
    issues_by_key = {i["key"]: i for i in issues}

    subtask_parent_keys = set()
    for issue in issues:
        fields = issue.get("fields", {})
        if fields.get("issuetype", {}).get("name") == "Sub-task":
            parent = fields.get("parent")
            if not parent:
                continue
            parent_key = parent["key"]
            # If intermediate parent is already in our issues dataset,
            # pre-populate subtask_cache from it — no API call needed
            if parent_key in issues_by_key:
                grandparent = issues_by_key[parent_key].get("fields", {}).get("parent")
                if grandparent:
                    subtask_cache[parent_key] = epic_dict.get(grandparent["key"])
                else:
                    subtask_cache[parent_key] = None
            else:
                subtask_parent_keys.add(parent_key)

    # print(f"[{start_date}→{end_date}] Issues: {len(issues)}, Sub-task parents: {len(subtask_parent_keys)} API calls (rest resolved from dataset)")                
    
    if subtask_parent_keys:
        await asyncio.gather(*[
            resolve_subtask_epic(session, pk, epic_dict, semaphore, subtask_cache)
            for pk in subtask_parent_keys
        ])
    # subtask_cache is now fully populated

    # 3. Build rows
    rows = []
    for issue in issues:
        fields = issue.get("fields", {})
        project = fields.get("project", {})
        parent = fields.get("parent")
        issue_type_name = fields.get("issuetype", {}).get("name")
        team = fields.get("customfield_10001")
        team_name = team.get("name") if isinstance(team, dict) else None

        # Resolve epic_info from pre-loaded dict
        if issue_type_name == "Epic":
            # Epic logs time against itself
            epic_info = epic_dict.get(issue["key"])
        elif issue_type_name == "Sub-task":
            # Sub-task: resolve via subtask_cache (grandparent lookup)
            epic_info = subtask_cache.get(parent["key"]) if parent else None
        elif parent:
            # Story/Task/Bug/etc: parent should be Epic — look up directly
            epic_info = epic_dict.get(parent["key"])
        else:
            epic_info = None

        for wl in issue_worklogs.get(issue["key"], []):
            started_str = wl.get("started", "")
            try:
                started_dt = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
                logged_date = started_dt.date()
            except Exception:
                continue
            if not (filter_start <= logged_date <= filter_end):
                continue

            author = wl.get("author", {})
            rows.append({
                "Issue_Key": issue["key"],
                "Issue_Name": fields.get("summary"),
                "Issue_Type": issue_type_name,
                "Team": team_name,
                "Project_Key": project.get("key"),
                "Project_Name": project.get("name"),
                "Epic_Key": epic_info["Epic_Key"] if epic_info else None,
                "Epic_Name": epic_info["Epic_Name"] if epic_info else None,
                "Goal_ARI": epic_info["Goal_ARI"] if epic_info else None,
                "User_Name": author.get("displayName"),
                "started": started_dt.isoformat(),
                "Logged_Date": logged_date.isoformat(),
                "Logged_Hours": round(wl.get("timeSpentSeconds", 0) / 3600, 2)
            })

    return pd.DataFrame(rows)


def split_into_months(start_date: str, end_date: str):
    """Split a date range into monthly chunks. Returns list of (start, end) strings."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    chunks = []
    current = start
    while current <= end:
        last_day = date(
            current.year,
            current.month,
            calendar.monthrange(current.year, current.month)[1]
        )
        chunk_end = min(last_day, end)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)
    return chunks


async def fetch_worklogs_async(projects, start_date, end_date):
    JIRA_EMAIL, JIRA_API_TOKEN = get_credentials()
    auth_string = base64.b64encode(f"{JIRA_EMAIL}:{JIRA_API_TOKEN}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_string}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    date_chunks = split_into_months(start_date, end_date)

    async with aiohttp.ClientSession(headers=headers) as session:
        # Fetch goals and epics once, before any month chunks
        goals_df = fetch_goals()
        epic_dict = await fetch_all_epics(session, projects)

        # Process months sequentially to stay within Azure timeout budget
        all_frames = []
        chunk_semaphore = asyncio.Semaphore(3)

        async def fetch_chunk_limited(sem, session, projects, chunk_start, chunk_end, epic_dict):
            async with sem:
                return await fetch_worklogs_for_range(session, projects, chunk_start, chunk_end, epic_dict)

        tasks = [
            fetch_chunk_limited(chunk_semaphore, session, projects, s, e, epic_dict)
            for s, e in date_chunks
        ]
        all_frames = [df for df in await asyncio.gather(*tasks) if not df.empty]

    if not all_frames:
        return pd.DataFrame(), goals_df

    return pd.concat(all_frames, ignore_index=True), goals_df


# === HTTP ENDPOINTS ===

@app.route(route="worklogs", auth_level=func.AuthLevel.ANONYMOUS)
async def get_worklogs(req: func.HttpRequest) -> func.HttpResponse:
    try:
        projects = req.params.get('projects', 'CARE,UMVISION,PFORM,POP,OPS,ENG,CHANGE,AFNDRY')
        start_date = req.params.get('start_date', '2026-01-01')
        end_date = req.params.get('end_date', '2026-02-28')
        projects_list = [p.strip() for p in projects.split(',')]

        df, goals_df = await fetch_worklogs_async(projects_list, start_date, end_date)

        if not df.empty and not goals_df.empty:
            df = df.merge(
                goals_df[["Goal_ARI", "Goal_Key", "Goal_Name_Lookup"]],
                on="Goal_ARI", how="left"
            )
            df.rename(columns={"Goal_Name_Lookup": "Goal"}, inplace=True)
        elif df.empty:
            df = pd.DataFrame(columns=[
                "Issue_Key", "Issue_Name", "Issue_Type", "Team",
                "Project_Key", "Project_Name",
                "Epic_Key", "Epic_Name",
                "Goal_ARI", "Goal_Key", "Goal",
                "User_Name", "started", "Logged_Date", "Logged_Hours"
            ])

        return func.HttpResponse(
            df.to_json(orient='records', date_format='iso'),
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@app.route(route="debug", auth_level=func.AuthLevel.ANONYMOUS)
def debug(req: func.HttpRequest) -> func.HttpResponse:
    try:
        import requests
        JIRA_EMAIL, JIRA_API_TOKEN = get_credentials()
        payload = {
            "jql": "project IN (CARE) AND worklogDate >= '2026-01-01' AND worklogDate <= '2026-03-31'",
            "fields": ["summary", "project", "parent", "issuetype"],
            "maxResults": 1
        }
        resp = requests.post(
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            auth=(JIRA_EMAIL, JIRA_API_TOKEN),
            json=payload, timeout=10
        )
        data = resp.json()
        issues = data.get("issues", [])
        if not issues:
            return func.HttpResponse(
                f"Search OK but no issues\n{resp.text[:300]}", mimetype="text/plain"
            )
        issue_key = issues[0]["key"]
        resp2 = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}/worklog?maxResults=5&startAt=0",
            auth=(JIRA_EMAIL, JIRA_API_TOKEN), timeout=10
        )
        return func.HttpResponse(
            f"Issue: {issue_key}\n"
            f"Worklog status: {resp2.status_code}\n"
            f"Worklog body: {resp2.text[:500]}",
            mimetype="text/plain"
        )
    except Exception as e:
        return func.HttpResponse(f"CRASHED: {str(e)}", mimetype="text/plain")


@app.route(route="issues", auth_level=func.AuthLevel.ANONYMOUS)
async def get_issues(req: func.HttpRequest) -> func.HttpResponse:
    try:
        projects = req.params.get('projects', 'CARE,UMVISION,PFORM,POP,OPS,ENG,CHANGE,AFNDRY')
        projects_list = [p.strip() for p in projects.split(',')]
        projects_str = ",".join(projects_list)

        JIRA_EMAIL, JIRA_API_TOKEN = get_credentials()
        auth_string = base64.b64encode(f"{JIRA_EMAIL}:{JIRA_API_TOKEN}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth_string}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        async with aiohttp.ClientSession(headers=headers) as session:
            # Fetch goals and epics once
            goals_df = fetch_goals()
            goal_lookup = {
                row["Goal_ARI"]: row["Goal_Name_Lookup"]
                for _, row in goals_df.iterrows()
            } if not goals_df.empty else {}

            epic_dict = await fetch_all_epics(session, projects_list)

            # Fetch issues
            issue_jql = (
                f"project IN ({projects_str}) "
                f"AND issuetype IN (Epic, Feature, Story, Solution, Task, 'Tech Enabler', 'Triage Task') "
                f"AND updated >= '2025-12-01'"
            )
            issues = []
            next_page_token = None

            while True:
                payload = {
                    "jql": issue_jql,
                    "fields": [
                        "project", "summary", "parent", "issuetype",
                        "customfield_10485", "customfield_10001",
                        "status", "created", "updated", "resolutiondate"
                    ],
                    "maxResults": 1000
                }
                if next_page_token:
                    payload["nextPageToken"] = next_page_token

                for attempt in range(MAX_RETRIES):
                    try:
                        async with session.post(
                            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
                            json=payload,
                            timeout=aiohttp.ClientTimeout(total=60)
                        ) as resp:
                            if resp.status == 429:
                                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                                continue
                            if resp.status != 200:
                                return func.HttpResponse(
                                    json.dumps({"error": f"Jira returned {resp.status}"}),
                                    mimetype="application/json", status_code=502
                                )
                            data = await resp.json()
                            break
                    except Exception:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    break

                batch = data.get("issues", [])
                if not batch:
                    break
                issues.extend(batch)
                next_page_token = data.get("nextPageToken")
                if not next_page_token or data.get("isLast", False):
                    break

        # Build rows
        rows = []
        for issue in issues:
            fields = issue.get("fields", {})
            project = fields.get("project", {})
            parent = fields.get("parent")
            issue_type_name = fields.get("issuetype", {}).get("name")
            team = fields.get("customfield_10001")
            team_name = team.get("name") if isinstance(team, dict) else None

            if issue_type_name == "Epic":
                epic_info = epic_dict.get(issue["key"])
            elif parent:
                epic_info = epic_dict.get(parent["key"])
            else:
                epic_info = None

            goal_ari = epic_info["Goal_ARI"] if epic_info else None

            rows.append({
                "Issue_Key": issue["key"],
                "Issue_Name": fields.get("summary"),
                "Issue_Type": issue_type_name,
                "Team": team_name,
                "Project_Key": project.get("key"),
                "Project_Name": project.get("name"),
                "Status": fields.get("status", {}).get("name"),
                "Created_Date": fields.get("created"),
                "Updated_Date": fields.get("updated"),
                "Resolved_Date": fields.get("resolutiondate"),
                "Parent_Key": parent.get("key") if parent else None,
                "Epic_Key": epic_info["Epic_Key"] if epic_info else None,
                "Epic_Name": epic_info["Epic_Name"] if epic_info else None,
                "Goal": goal_lookup.get(goal_ari)
            })

        df = pd.DataFrame(rows)

        return func.HttpResponse(
            df.to_json(orient='records', date_format='iso'),
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )