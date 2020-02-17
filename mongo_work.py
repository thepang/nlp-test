import glob
import json

import pandas as pd
from pymongo import MongoClient


path_to_root = "/Users/pang/repos/nlp-test"
client = MongoClient()
db = client.tal_pod


def insert_documents_to_mongo(year):
    """
    :param year: Used to tell function which year to process.
    :return: None
    """

    for path in glob.glob(f"{path_to_root}/data/005_JSON_full/005_{year}*"):
        ep_num = path.split("_")[-1].split(".")[0]

        if list(db.episodes.find({"ep": ep_num}, {})):
            print(f"Already found episode {ep_num} in database. SKIPPED")
            continue

        print(f"Processing file at {path} for Mongo")
        with open(path, "r") as file:
            merged_transcripts = json.load(file)

        print(f"Inserting record for {ep_num}")
        db.episodes.insert_one(merged_transcripts)


def get_episodes(query=None):
    find_ep = list(db.episodes.find(query, {"_id": 0}))
    records = []
    for ep in find_ep:
        for act in ep["acts"]:
            for timestamp, timestamp_data in act["transcript"].items():
                record = dict(
                    ep_num=ep["ep"],
                    ep_title=ep["title"],
                    ep_air_date=ep["air_date"],
                    ep_summary=ep["ep_summary"],
                    speaker=timestamp_data["speaker"],
                    words=timestamp_data["words"],
                    timestamp=timestamp,
                    act=act["name"],
                )
                records.append(record)
    return pd.DataFrame(records)


# for year in range(1995, 2021):
#     insert_documents_to_mongo(year)
