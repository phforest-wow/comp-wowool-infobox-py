import argparse
import sys
import json
import re
import requests
import re
import json
from pathlib import Path
import traceback
from wowool.infobox.session import session
from wowool.infobox.session import InfoBoxInstance, InfoBoxData
from wowool.infobox.utilities import update_concept, convert_args
from wowool.string import *
from wowool.infobox.process.process import process as mp_process
from wowool.infobox.utilities import add_search_literal, find_search_literal
from bs4 import BeautifulSoup

ATTRIBUTE_PATTERN = re.compile(r"{{(.*?)(:?[|](.*?))?}}")
ATTRIBUTE_PATTERN_ATTR = re.compile(r"(.+?)= *{{(.+?)?}}")
ATTRIBUTE_PATTERN_REFER_TO = re.compile(r".*?refer to:(.*).?")
REDIRECTION_PATTERN = re.compile(r"#REDIRECT *\[\[(.*?)\]\].*")
SEARCHRESULT_PATTERN = re.compile(r"([0-9]+) statements?, ([0-9]+) sitelinks?.*")

SOURCE = "wikipedia"


def get_rec_wikipedia(literal, language_code):
    return session().query(InfoBoxData).filter_by(literal=literal, language_code=language_code, source=SOURCE).first()


def get_infobox_wikipedia(input, language="english", redirect=None):

    try:
        literal, encoded_literal, language_code, language = convert_args(input, language)

        print("wikipedia:encoded_literal:", encoded_literal)
        item = session().query(InfoBoxData).filter_by(literal=literal, language_code=language_code, source=SOURCE).first()
        if item:
            return item
        else:
            url = f"""https://{language_code}.wikipedia.org/w/api.php?action=query&prop=revisions&rvprop=content&format=json&titles={encoded_literal}&rvsection=0&rvslots=main"""
            data = requests.get(url).text
            response = json.loads(data)
            # print("wikipedia:response:", response)
            for page in response["query"]["pages"].items():
                if page[0] == "-1":
                    item = InfoBoxData(literal=literal, language_code=language_code, source=SOURCE, json_string="{}")
                    session().add(item)
                    session().commit()
                    return item
                try:
                    ibd = page[1]["revisions"][0]["slots"]["main"]["*"]
                    # see if we have a redirection in the response.
                    m = REDIRECTION_PATTERN.match(ibd)
                    if m:
                        redirection = m.group(1)
                        if redirect == None:
                            # call ourself with the redirected entry.
                            return get_infobox_wikipedia(redirection, language, redirect=input)
                        else:
                            return False
                except Exception as ex:
                    # print(f"Exception: {input}: {page} ", ex)
                    ibd = "{'contentmodel': 'wikitext'}"

                # We did not have redirections.
                ibd = re.sub(r"(\[\[|\]\])", "", ibd)
                ibd = " ".join(ibd.split("\n"))

            results = {}
            for match in ATTRIBUTE_PATTERN.findall(ibd):
                print("infobox:match:", match[0])
                results[match[0].lower()] = [v.strip() for v in filter(None, match[1].split("|"))]
            for match in ATTRIBUTE_PATTERN_ATTR.findall(ibd):
                print("infobox:match:", match[0])
                results[match[0].lower()] = [v.strip() for v in filter(None, match[1].split("|"))]

            for match in ATTRIBUTE_PATTERN_REFER_TO.findall(ibd):
                results["ambiguous description"] = match

            if redirect:
                literal = redirect

            item = InfoBoxData(literal=literal, language_code=language_code, source=SOURCE, json_string=json.dumps(results))
            session().add(item)
            session().commit()
            return item
    except Exception as ex:
        print("infobox:get_infobox_wikipedia:", ex)
        return None


def is_descriptor(concept):
    return concept.uri == "Descriptor"


# infobox_descriptors
ibdesc = {
    "danish": [("sd", "short description"), ("key_prefix", ["infoboks", "infobox"])],
    "dutch": [("key_prefix", ["infobox"]), ("keys", True)],
    "english": [("sd", "short description"), ("sd", "ambiguous description"), ("key_prefix", ["infobox"]), ("keys", True)],
    "french": [("key_prefix", ["infobox"]), ("keys", True)],
    "german": [("key_prefix", ["infobox"]), ("keys", True)],
    "italian": [("key_prefix", ["infobox"]), ("keys", True)],
    "portuguese": [("key_prefix", ["infobox"]), ("keys", True)],
    "norwegian": [("key_prefix", ["infoboks"]), ("keys", True)],
    "spanish": [("key_prefix", ["ficha de", "ficha del", "ficha"]), ("keys", True)],
    "swedish": [("key_prefix", ["infoboks"]), ("keys", True)],
}


def add_descriptions(item, doc, literal, language):
    from wowool.annotation import Concept
    from wowool.infobox import update_concept

    found = False
    for concept in Concept.iter(doc, is_descriptor):
        if "type" in concept.attributes:
            concept_type = camelize(concept.attributes["type"][0])
            # print("is as : ", concept_type)
            attributes = concept.attributes
            del attributes["type"]
            attributes["descriptor"] = concept.stem
            attributes["source"] = SOURCE
            update_concept(literal, language, concept_type, attributes)
            found = True
    return found


def parse_wikipedia_data(item, literal, language, infobox, verbose):
    from wowool.infobox import update_concept

    results = []
    found = False
    if language in ibdesc:
        for desc in ibdesc[language]:
            data = None
            if desc[0] == "sd":
                data = infobox[desc[1]] if desc[1] in infobox else None
            elif desc[0] == "key_prefix":
                data = ""
                for prefix in desc[1]:
                    concept_type = None
                    for key in [key for key in infobox.keys() if key.startswith(prefix)]:
                        suffix = key[len(prefix) :]
                        if "<!--" in suffix:
                            # some key have comment in them ex:
                            suffix = suffix[0 : suffix.find("<!--")]
                        if isinstance(infobox[key], str):
                            concept_type = camelize(suffix)
                            update_concept(literal, language, concept_type, {"source": f"{SOURCE}:key"})
                            found = True
                            data += suffix + ". "
                    if concept_type:
                        break

            elif desc[0] == "keys" and desc[1] == True:
                data = ". ".join(infobox.keys())

            if data:
                try:
                    if isinstance(data, list):
                        data = " ".join(data)
                    if verbose:
                        print("infobox:wikipedia_data:", data)
                    doc = mp_process(f"{language},entity,dates,discovery", data)
                    # if verbose:
                    #     print(doc)
                    found = add_descriptions(item, doc, literal, language)
                except Exception as ex:
                    traceback.print_exc(file=sys.stdout)
                    print(ex, file=sys.stderr)
    return found


REMOVE_BRAKETS = re.compile(r"(.*)\(.+\)")


def wikipedia_discover(input, language, verbose=False):
    literal, encoded_literal, language_code, language = convert_args(input, language)
    item = get_rec_wikipedia(literal, language_code)
    if not item:
        item = get_infobox_wikipedia(input, language)
    assert item
    infobox = json.loads(item.json_string)
    parse_wikipedia_data(item, literal, language, infobox, verbose)


def get_infobox_attributes(
    literal: str,
    attributes: list,
    language: str = "english",
):
    wiki_literal, encoded_literal, language_code, language = convert_args(literal, language)
    instance_data = get_rec_wikipedia(literal, language_code)
    if not instance_data:
        url = f"""https://{language_code}.wikipedia.org/wiki/{encoded_literal}"""
        data = requests.get(url).text
        soup = BeautifulSoup(str(data), "html5lib")
        infobox = soup.select_one("table.infobox")

        item = InfoBoxData(literal=literal, language_code=language_code, source=SOURCE, json_string=infobox.prettify(formatter="minimal"))
        session().add(item)
        session().commit()

    instance_data = get_rec_wikipedia(literal, language_code)
    if instance_data:
        soup = BeautifulSoup(str(instance_data.json_string), "html5lib")
        infobox = soup.select_one("table.infobox")
        infobox_header = infobox.find(class_="infobox-header")
        attributes_data = {}
        if infobox_header:
            attributes_data["descriptor"] = infobox_header.get_text(" ", strip=True)
        for label_item in infobox.find_all(class_="infobox-label"):
            label = label_item.get_text(" ", strip=True).lower().replace(" ", "_")
            value_items = label_item.find_next_sibling("td")
            if value_items and attributes and label in attributes:
                values = []
                for value_item in value_items.select(".hlist li"):
                    values.append(value_item.get_text(" ", strip=True).lower())
                if not values:
                    value = value_items.get_text(" ", strip=True).lower()
                    if m := REMOVE_BRAKETS.match(value):
                        value = m.group(1).strip()
                    values.append(value)
                if values:
                    attributes_data[label] = values
        if attributes_data:
            instance = InfoBoxInstance(
                literal=literal, language_code=language_code, concept="infobox", attributes=json.dumps(attributes_data)
            )
            session().add(instance)
            session().commit()
            return attributes_data
