import glob
import json
import time

import requests
from bs4 import BeautifulSoup

path_to_root = "/Users/pang/repos/nlp-test"


def get_transcript_html(years):
    """
    Goes to hardcoded TAL site for the given years and grab the data.
    :param years: Gets transcripts for the years provided (as list)
    :return: None
    """

    for year in years:
        url = f"https://www.thisamericanlife.org/archive?year={str(year)}"
        r = requests.get(url)

        if r.status_code != 200:
            raise Exception(
                f"Did not connect successfully. HTTP status code: {r.status_code}\nURL: {url}"
            )

        soup = BeautifulSoup(r.text, "lxml")

        with open(
            f"{path_to_root}/data/001_html/001_html_{str(year)}.txt", "w"
        ) as file:
            file.write(soup.prettify())


def get_ep_metadata(year):
    """
    Opens file for given year from get_transcript_html, parses for metadata for each podcast in that year.
    Saves the data into JSON file
    :param year: Used to tell function which year to process.
    :return: None
    """
    with open(f"{path_to_root}/data/001_html/001_html_{year}.txt", "r") as file:
        soup = BeautifulSoup(file, "lxml")

    metadata = {}

    for episode in soup.findAll("article", attrs={"data-type": "episode"}):
        ep_html = episode.find("a", attrs={"class": "goto goto-episode"})
        _, num, title = ep_html["href"].split("/")

        date = episode.find("span", attrs={"class": "date-display-single"}).text.strip()

        summary = episode.find("p").text.strip()

        d = {"ep_title": title, "air_date": date, "ep_summary": summary}

        metadata[num] = d

    with open(
        f"{path_to_root}/data/002_metadata/002_ep_metadata_{year}.json", "w"
    ) as file:
        file.write(json.dumps(metadata))
    print(f'Wrote file "002_ep_metadata_{year}.json"')


def get_transcripts(year):
    """
    Given an episode metadata file from get_ep_metadata,
    finds episode transcript page and saves the transcripts as raw HTML
    :param year: Used to tell function which year to process.
    :return: None
    """
    with open(
        f"{path_to_root}/data/002_metadata/002_ep_metadata_{year}.json", "r"
    ) as file:
        x = file.read()
    episodes = json.loads(x)

    for ep in episodes:
        print(f"Getting transcript for episode: {ep}")
        url = f"https://www.thisamericanlife.org/{ep}/transcript"
        r = requests.get(url)

        if r.status_code != 200:
            raise Exception(
                f"Did not connect successfully. HTTP status code: {r.status_code}\nURL: {url}"
            )

        soup = BeautifulSoup(r.text, "lxml")

        with open(
            f"{path_to_root}/data/003_transcripts/003_transcript_{str(year)}_{ep}.txt",
            "w",
        ) as file:
            file.write(soup.prettify())

        print(f"Saved transcript for episode {ep}. Sleeping 10 seconds...")
        # TAL robots text request 10 second crawl delay
        time.sleep(10)


def transcript_to_json(year):
    """
    Takes a HTML text file from get_transcripts and converts to JSON file.
    :param year: Used to tell function which year to process.
    :return: None
    """

    for path in glob.glob(
        f"{path_to_root}/data/003_transcripts/003_transcript_{year}_*"
    ):
        transcript = {}
        with open(path, "r") as file:
            soup = BeautifulSoup(file, "lxml")

        ep, title = soup.find("title").text.split(":")

        transcript["ep"] = ep.strip()
        transcript["title"] = title.replace(" - This American Life", "").strip()

        acts = soup.findAll("div", attrs={"class": "act"})
        transcript["acts"] = []

        for act in acts:
            name = act.find("h3").text.strip().replace(".", "")
            act_transcript = {}
            timestamps = act.findAll("p")
            for timestamp in timestamps:
                time_content = {
                    # Mongo doesn't like keys with periods so they are replaced with underscores
                    timestamp["begin"].replace(".", "_"): {
                        "speaker": timestamp.find_previous("h4").text.strip(),
                        "words": timestamp.text.strip()
                        .replace("\n", "")
                        .replace("                           ", " "),
                    }
                }
                act_transcript.update(time_content)
            # transcript[act_name.text.strip().replace('.', '')] = content
            act_dictionary = {"name": name, "transcript": act_transcript}
            transcript["acts"].append(act_dictionary)

        with open(
            f"{path_to_root}/data/004_JSON_transcript/004_{str(year)}_{ep.strip()}.json",
            "w",
        ) as file:
            file.write(json.dumps(transcript))
        print(f"Wrote transcript for {ep.strip()} to file.")


def join_metadata_json(year):
    with open(
        f"{path_to_root}/data/002_metadata/002_ep_metadata_{year}.json", "r"
    ) as file:
        ep_metadatas = json.loads(file.read())
    print(f"Grabbed metadata information for year {year}")

    for ep_num, metadata in ep_metadatas.items():
        with open(
            f"{path_to_root}/data/004_JSON_transcript/004_{year}_{ep_num}.json", "r"
        ) as file:
            transcript = json.loads(file.read())
        transcript.update(metadata)

        with open(
            f"{path_to_root}/data/005_JSON_full/005_{str(year)}_{ep_num.strip()}.json",
            "w",
        ) as file:
            file.write(json.dumps(transcript))
        print(f"Combined and wrote full JSON for episode {ep_num.strip()}")


# # Grab HTML files for 1995-2020
# for_fn = []
# for year in range(1995, 2021):
#     for_fn.append(year)
# get_transcript_html(for_fn)
#
# get_ep_metadata(2019)
#
# get_transcripts(2019)
#
#
# transcript_to_json(2019)
#
# join_metadata_json(2019)
