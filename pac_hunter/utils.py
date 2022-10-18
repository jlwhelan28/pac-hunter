import re
import io
import requests
import pandas as pd
from fuzzywuzzy import fuzz, process
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
        clean["name"] = "candidate"
    
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


def fuzzy_filter(df_1, df_2, key1, key2, threshold=90, guarantee=["party", "state_code"]):
    """
    :param df_1: the left table to join
    :param df_2: the right table to join
    :param key1: key column of the left table
    :param key2: key column of the right table
    :param threshold: how close the matches should be to return a match, based on Levenshtein distance
    :param limit: the amount of matches that will get returned, these are sorted high to low
    :return: dataframe with boths keys and matches
    """
    s = df_2[key2].tolist()
    
    nearest_match = df_1[key1].apply(lambda x: process.extract(x, s))    
    match = nearest_match.apply(lambda x: x[0][0] if x[0][1] >= threshold else None)
    filtered = df_1.copy()
    filtered["matched_denier"] = match
    filtered = filtered[filtered["matched_denier"].notna()]
    for key in guarantee:
        for ix, row in filtered.iterrows():
            guarantee_value = df_2.loc[df_2["name"] == row["matched_denier"]][key].iloc[0]
            if not row[key] == guarantee_value:
                filtered = filtered.drop(ix)

    return filtered
