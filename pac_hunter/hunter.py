# Core modules
import asyncio
import os
import re
from io import BytesIO
from typing import List, Union
from urllib.request import urlopen
from zipfile import ZipFile

# Non-core modules
import httpx
import numpy as np
import pandas as pd
from requests import ReadTimeout
from thefuzz import process

# Local modules
from pac_hunter.states import abbrev_to_us_state, us_state_to_abbrev


def read_bulk_file(zip_or_url: str, fn=None) -> list:
    """Read the contents of a FEC bulk data file

    Args:
        zip_or_url: Path to zip file or url
        fn: Filename within zip to read if more than one files exist in the archive

    Returns:
        List of parsed lines from the bulk data file

    """
    if os.path.isfile(zip_or_url):
        with open(zip_or_url) as f:
            zipf = ZipFile(BytesIO(f))
    else:
        res = urlopen(zip_or_url)
        zipf = ZipFile(BytesIO(res.read()))

    if fn:
        lines = [line.decode().strip().split("|") for line in zipf.open(fn).readlines()]
    else:
        fns = zipf.namelist()
        if len(fns) == 1:
            lines = [
                line.decode().strip().split("|")
                for line in zipf.open(fns[0]).readlines()
            ]
        elif len(fns) == 0:
            raise ValueError("Zip file is empty")
        else:
            raise ValueError(
                f"Provide filename to extract if zip has more than one entry"
            )
    return lines


def bulk_file_to_df(headers: str, content: str) -> pd.DataFrame:
    """Build a dataframe from FEC bulk data source

    Args:
        headers: url or path to csv file containing column headers
        content: url or path to bulk columnar data from FEC

    Returns:
        Dataframe from FEC bulk data
    """
    df = pd.read_csv(headers)
    bulk = read_bulk_file(content)

    # Assert that the number of headers matches the length of each row
    assert len(df.columns) == len(bulk[0])
    data = np.array(bulk)

    return pd.DataFrame(data, columns=df.columns)


def clean_candidate_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dataframe with candidate info to consistent format

    Args:
        df: Pandas dataframe with candidate info

    Returns
        Dataframe with consistent format
    """
    # Drop to lowercase
    clean = df.applymap(lambda x: x.strip().lower() if isinstance(x, str) else x)
    clean.columns = df.columns.str.lower()

    # Convert currency string to float
    if "total" in clean.columns:
        clean["total_float"] = clean["total"].apply(
            lambda x: float(re.sub(r"[^\d.]", "", x))
        )

    # Fill name column
    if not "name" in clean.columns:
        clean["name"] = clean["candidate"]
    clean["first_name"] = clean["name"].apply(lambda x: " ".join(x.split(" ")[:-1]))
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
                closest_state_name = process.extractOne(
                    s, list(us_state_to_abbrev.keys())
                )[0]
                state_codes.append(us_state_to_abbrev[closest_state_name].lower())
    clean["state_code"] = state_codes
    return clean


async def openfec_get(
    url: str, params: dict, rate: int = 4, limit: int = 1, **kwargs
) -> dict:
    """Facilitate asynchronous OpenFEC GET request

    Args:
        url: OpenFEC endpoint
        params: GET parameters
        rate: Frequency in seconds to allow a number of requests specified by `limit`
        limit: Count per rate to allow a GET request

    Returns:
        HTTP response json data

    """
    params = params.copy()
    params.update(kwargs)
    throttle = asyncio.Semaphore(limit)
    async with throttle:
        async with httpx.AsyncClient() as client:
            try:
                res = await client.get(url, params=params)
            except ReadTimeout:
                await asyncio.sleep(rate)
                res = await client.get(url, params=params)

            if res.status_code == 200:
                data = res.json()
            else:
                print(f"HTTP error: {res.status_code}")
                print(f"Request: {res.url}")
                raise RuntimeError(res.text)
            await asyncio.sleep(rate)
    return data


async def openfec_get_pages(url: str, api_key: str, **kwargs) -> list:
    """Get paginated results from OpenFEC endpoints

    Args:
        url: OpenFEC endpoint
        api_key: OpenFEC api key `https://api.open.fec.gov/developers/`
        kwargs: OpenFEC GET parameters

    Returns:
        List of http response json data

    """
    params = dict(
        api_key=api_key,
        per_page=100,
        **kwargs,
    )
    data = await openfec_get(url, params, **kwargs)

    responses = [data]
    if "pagination" in data.keys():
        page_count = data["pagination"]["pages"]
    else:
        page_count = 1
    if page_count > 1:
        for ipage in range(1, page_count):
            params["page"] = ipage
            try:
                responses.append(await openfec_get(url, params, **kwargs))
            except RuntimeError:
                print(f"Warning: page {ipage + 1} returned a html error")
    return responses


async def openfec_get_pages_by_chunks(
    url: str, api_key: str, chunk_parameter: str, nbatch: int = 50, **kwargs
) -> list:
    """Get paginated results from OpenFEC endpoints, splitting a array-like parameter into chunks

    Args:
        url: OpenFEC endpoint
        api_key: OpenFEC api key `https://api.open.fec.gov/developers/`
        chunk_parameter: Parameter name for an array-like kwarg param to split into chunks
        nbatch: Batch size for the chunk
        kwargs: OpenFEC GET parameters

    Returns:
        List of http response json data
    """

    # Unpack the chunked parameter from kwargs
    params = kwargs.copy()
    array_to_chunk = params.pop(chunk_parameter)
    array_to_chunk = list(array_to_chunk)

    # Chunk the array
    chunks = [
        array_to_chunk[i * nbatch : (i + 1) * nbatch]
        for i in range((len(array_to_chunk) + nbatch - 1) // nbatch)
    ]

    # Repack the chunked parameter into a list of kwargs
    chunked_params = []
    for chunk in chunks:
        d = {}
        d.update(params)
        d.update({chunk_parameter: chunk})
        chunked_params.append(d)

    # Gather the responses
    responses = await asyncio.gather(
        *(
            openfec_get_pages(url, api_key, **chunk_params)
            for chunk_params in chunked_params
        )
    )
    responses = [res for group in responses for res in group]

    return responses


async def fetch_committee_distributions(
    committee_name: str,
    recipient_names: Union[str, List[str]],
    api_key: str = "DEMO_KEY",
    candidate_args: dict = {},
    committee_args: dict = {},
    distribution_args: dict = {},
) -> pd.DataFrame:
    """Match distributions from a PAC to candidates of interest using OpenFEC

    Args:
        committee_name: Searchable Political Action Committee name
        recipient_names: Searchable list of candidates
        api_key: OpenFEC api key `https://api.open.fec.gov/developers/`
        candidate_args: kwargs passed to /names/candidates
        committee_args: kwargs passed to /names/committees
        distribution_args: kwargs passed to /schedule_b/by_recipient_id
    """

    url_candidates = "https://api.open.fec.gov/v1/names/candidates/"
    url_committees = "https://api.open.fec.gov/v1/names/committees/"
    url_distributions = (
        "https://api.open.fec.gov/v1/schedules/schedule_b/by_recipient_id"
    )
    url_candidate_to_committee_data = (
        "https://www.fec.gov/files/bulk-downloads/2022/ccl22.zip"
    )
    url_candidate_to_committee_headers = (
        "https://www.fec.gov/files/bulk-downloads/data_dictionaries/ccl_header_file.csv"
    )

    # Search candidates by name to get FEC ids
    responses = await openfec_get_pages_by_chunks(
        url_candidates,
        api_key,
        chunk_parameter="q",
        q=recipient_names,
        **candidate_args,
    )
    candidates = [entry for res in responses for entry in res["results"]]
    candidate_df = pd.DataFrame(candidates)

    # Match the candidates FEC id to their campaign committee, the recipient of donations
    ccl_df = bulk_file_to_df(
        url_candidate_to_committee_headers, url_candidate_to_committee_data
    )
    candidate_df["CAND_ID"] = candidate_df["id"]
    candidate_df = candidate_df.merge(ccl_df, on="CAND_ID")
    candidate_committee_ids = candidate_df["CMTE_ID"].to_list()

    # Search committees by name to get FEC ids
    responses = await openfec_get_pages(
        url_committees, api_key, q=committee_name, **committee_args
    )
    if len(responses[0]["results"]) == 0:
        return None
    elif len(responses[0]["results"]) > 1:
        print(
            f"Multiple results for committee {committee_name}, taking the first entry"
        )
    committee_id = responses[0]["results"][0]["id"]

    # Collect all schedule b distrubitions from the selected committee to the list of campaign committees
    responses = await openfec_get_pages_by_chunks(
        url_distributions,
        api_key,
        chunk_parameter="recipient_id",
        committee_id=committee_id,
        recipient_id=candidate_committee_ids,
        **distribution_args,
    )
    distributions = [entry for res in responses for entry in res["results"]]

    candidate_df = candidate_df.rename(
        columns={"CMTE_ID": "recipient_id", "CAND_ID": "candidate_id"}
    )
    df = pd.DataFrame(distributions)
    df = df.merge(candidate_df, on="recipient_id")
    return pd.DataFrame(df)
