import pandas as pd
import requests
from fuzzywuzzy import process
from pac_hunter.states import us_state_to_abbrev, abbrev_to_us_state


def clean_df(df):
    # Drop to lowercase
    clean = df.applymap(lambda x: x.strip().lower() if isinstance(x, str) else x)
    clean.columns = df.columns.str.lower()

    # Convert currency string to float
    if "total" in clean.columns:
        clean["total_float"] = clean["total"].apply(lambda x: float(re.sub(r'[^\d.]', '', x)))

    # Fill name column
    if not "name" in clean.columns:
        clean["name"] = clean["candidate"]
    clean["first_name"] = clean["name"].apply(lambda x: x.split(" ")[:-1])
    clean["last_name"] = clean["name"].apply(lambda x: x.split(" ")[-1])

    # Trim party names to first letter
    clean["party"] = clean["party"].apply(lambda x: x[0])

    # State full name or abbreviation to 2 letter code
    state_codes = []
    for s in clean["state"]:
        if len(s) == 2:
            # State is already in two-letter code form
            state_codes.append(s)
        else:
            try:
                # Try for exact match (will not occur in current setup due to capitalization)
                state_codes.append(us_state_to_abbrev[s].lower())
            except KeyError:
                # Look for closest state match
                closest_state_name = process.extractOne(s, list(us_state_to_abbrev.keys()))[0]
                state_codes.append(us_state_to_abbrev[closest_state_name].lower())
    clean["state_code"] = state_codes
    return clean



def fetch_committee_distributions(api_key, committee_name, recipient_names=[], committee_args={}, distribution_args={}):
    res = fec_query_committee(api_key, committee_name, **committee_args)
    if len(res["results"]) == 0:
        return None
    elif len(res["results"]) > 1:
        print(f"Multiple results for committee {committee_name}, taking the first entry")
    committee_id = res["results"][0]["id"]
    
    responses = fec_query_distributions(api_key, [committee_id], recipient_names, **distribution_args)

    distributions = []
    for res in responses:
        for entry in res["results"]:
            distributions.append(entry)
    df = pd.DataFrame(distributions)
    return df


def fec_query_committee(api_key, query, **kwargs):
    url = "https://api.open.fec.gov/v1/names/committees"
    payload = dict(
        api_key=api_key,
        q=query,
        **kwargs,
    )
    
    res = requests.get(url, params=payload)
    if res.status_code == 200:
        return res.json()    

    else:
        raise RuntimeError(res.text)


def fec_query_distributions(api_key, committee_ids, recipient_names, **kwargs):
    url = "https://api.open.fec.gov/v1/schedules/schedule_b/by_recipient"

    # Limit query to 50 names at a time
    nbatch = 50
    print("Dividing recipient names into groups of 50")
    batch_recipient_names = [
        recipient_names[i * nbatch:(i + 1) * nbatch] for i in range((len(recipient_names) + nbatch - 1) // nbatch )
    ]
    responses = []
    for recipient_group in batch_recipient_names:
        payload = dict(
            api_key=api_key,
            committee_id=committee_ids,
            recipient_name=recipient_group,
            **kwargs,
        )
        res = requests.get(url, params=payload)
        if res.status_code == 200:
            data = res.json()
        else:
            print(f"HTTP error: {res.status_code}")
            print(f"Request: {res.url}")
            raise RuntimeError(res.text)

        page_count = data["pagination"]["pages"]
        responses.append(res.json())
        if page_count > 1:
            for ipage in range(1, page_count):
                payload.update(page=ipage)
                next = requests.get(url, params=payload)
                if next.status_code == 200:
                    responses.append(next.json())
                else:
                    print(next.text)
    return responses
