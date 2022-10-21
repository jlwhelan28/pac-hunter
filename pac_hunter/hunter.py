import pandas as pd
import requests


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
