import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import random

import click

import utils
from models import BookIO, TopicClassification, TopicClassificationTrainingDataset


LOC_CO_TO_GXML_TOPICS = {
    "GENERAL WORKS": [
        "Encyclopedias and dictionaries",
        "Newspapers",
        "Periodicals",
    ],
    "PHILOSOPHY. PSYCHOLOGY. RELIGION": [
        "Philosophy",
        "Theology",
        "Logic",
        "Psychology",
        "Aesthetics",
        "Ethics",
        "Mythology",
        "Rationalism",
        "Judaism",
        "Islam",
        "Theosophy",
        "Buddhism",
        "Christianity",
    ],
    "AUXILIARY SCIENCES OF HISTORY": [
        "Archaeology",
        "Numismatics",
        "Heraldry",
        "Genealogy",
        "Biography",
    ],
    "WORLD HISTORY AND HISTORY OF EUROPE, ASIA, AFRICA, AUSTRALIA, NEW ZEALAND, ETC.": [
        "World history"
    ],
    "HISTORY OF THE AMERICAS": [
        "Indians of South America",
        "Indians of North America",
    ],
    "GEOGRAPHY. ANTHROPOLOGY. RECREATION": [
        "Geography",
        "Cartography",
        "Anthropology",
        "Folklore",
        "Manners and customs",
        "Oceanography",
        "Atlases",
        "Mathematical geography",
    ],
    "SOCIAL SCIENCES": [
        "Social sciences",
        "Statistics",
        "Commerce",
        "Finance",
        "Sociology",
        "Socialism",
        "Communism",
        "Anarchism",
        "Criminology",
    ],
    "POLITICAL SCIENCE": [
        "Political science",
        "Democracy",
        "Local government",
        "Municipal government",
        "International relations",
        "Representative government and representation",
    ],
    "LAW": [
        "Law",
        "Civil law",
        "Criminal law",
        "Constitutional law",
        "Commercial law",
        "Maritime law",
        "Administrative law",
        "Military law",
        "Mining law",
        "Corporation law",
        "Educational law and legislation",
        "Labor laws and legislation",
        "Railroad law",
        "Fishery law and legislation",
        "Banking law",
        "Marriage law",
        "Liquor laws",
        "Insurance law",
        "Customary law",
        "Patent laws and legislation",
        "Building laws",
        "Press law",
        "Emigration and immigration law",
    ],
    "EDUCATION": [
        "Education",
        "Textbooks",
    ],
    "MUSIC AND BOOKS ON MUSIC": [
        "Music",
        "Piano music",
        "Music theory",
        "Musical notation",
        "Orchestral music",
    ],
    "FINE ARTS": [
        "Architecture",
        "Sculpture",
        "Drawing",
        "Painting",
        "Decorative arts",
    ],
    "LANGUAGE AND LITERATURE": [
        "Philology",
        "Classical philology",
        "Oriental philology",
        "Romance philology",
        "Russian philology",
        "Greek philology",
        "Language and languages",
        "English language",
        "French language",
        "German language",
        "Latin language",
        "Greek language",
        "Hebrew language",
        "Spanish language",
        "Italian language",
        "Arabic language",
        "Sanskrit language",
        "Chinese language",
        "Indo-European languages",
        "Russian language",
        "Dutch language",
        "Portuguese language",
        "Swedigh language",
        "Irish language",
        "Japanese language",
        "Syriac language",
        "Romance languages",
        "Old Norse language",
        "Literature",
        "English literature",
        "French literature",
        "Italian literature",
        "American literature",
        "Russian literature",
        "Spanish literature",
        "Greek literature",
        "Chinese literarure",
        "Latin literature",
        "Polish literature",
        "Comparative literature",
        "Children's literature",
    ],
    "SCIENCE": [
        "Science",
        "Mathematics",
        "Astronomy",
        "Physics",
        "Chemistry",
        "Geology",
        "Natural history",
        "Biology",
        "Botany",
        "Zoology",
        "Physiology",
        "Human anatomy",
    ],
    "MEDICINE": [
        "Medicine",
        "Pathology",
        "General Surgery",
        "Ophthalmology",
        "Gynecology",
        "Obstetrics",
        "Pediatrics",
        "Dentistry",
        "Dermatology",
        "Therapeutics",
        "Pharmacology",
        "Pharmacy",
        "Homeopathy",
    ],
    "AGRICULTURE": ["Agriculture", "Horticulture", "Forests and forestry", "Hunting"],
    "TECHNOLOGY": [
        "Technology",
        "Engineering",
        "Civil engineering",
        "Electrical engineering",
        "Electric engineering",
        "Mechanical engineering",
        "Mining engineering",
        "Hydraulic engineering",
        "Steam engineering",
        "Home economics",
    ],
    "MILITARY SCIENCE": [
        "Artillery",
        "Military engineering",
        "Infantry drill and tactics",
    ],
    "NAVAL SCIENCE": [
        "Naval art and science",
        "Naval architecture",
        "Shipbuilding",
        "Marine engineering",
    ],
    "BIBLIOGRAPHY. LIBRARY SCIENCE. INFORMATION RESOURCES (GENERAL)": [
        "Library science",
        "Bibliography",
        "Paleography",
    ],
}
""" 
    Loose mapping between target topics and examples of existing topic/subjects present in the collection's metadata.
    This mapping is used to build a training dataset.
    Target content categories: first level the Library of Congress' Classification Outline (https://www.loc.gov/catdir/cpso/lcco/).
"""

GXML_TOPICS_TO_LOC_CO = {
    gxml_topic: target_topic
    for target_topic, gxml_topics in LOC_CO_TO_GXML_TOPICS.items()
    for gxml_topic in gxml_topics
}
""" 
    Opposite of `LOC_CO_TO_GXML_TOPICS`. 
    Target topics indexed by existing topic/subjects from the collection. 
"""


@click.command("extract-topic-classification-training-dataset")
@utils.needs_pipeline_ready
def extract_topic_classification_training_dataset():
    """
    Collects topic classification items that can be used to train a text classification model.
    Said text classification model's goal is to assign a top-level category from the Library of Congress' Classification Outline to a given book based on its metadata.

    Isolates entries where:
    - `TopicClassification.from_metadata` only contains 1 term (no comma).
    - Said term can be matched with one of the top-level items from the Library Of Congress Classification Outline (see `LOC_CO_TO_GXML_TOPICS`).

    Notes:
    - Replaces existing training set if already present.
    - Training dataset is split between "train" (most entries), "test" (validation, 5000 entries), "benchmark" (1000 entries).
    - See `export topic-classification-training-set` to export the results of this command.
    """
    full_set = []
    train = []
    test = []
    benchmark = []

    #
    # Dependencies check
    #
    try:
        assert BookIO.select().count() == TopicClassification.select().count()
    except:
        click.echo("This command needs metadata-based topic classification data.")
        exit(1)

    # Delete existing set
    if TopicClassificationTrainingDataset.select().count():
        TopicClassificationTrainingDataset.delete().where().execute()

    #
    # Filter elements where topic can be matched with an item from LOC CO's 1st level.
    #
    for topic_classification in TopicClassification.select().iterator():

        topic_from_metadata = topic_classification.from_metadata

        # Skip if there is no topic classification
        if not topic_from_metadata:
            continue

        # Skip if > 1 topic
        if "," in topic_from_metadata:
            continue

        # Skip if not in list of match-able topics
        if topic_from_metadata not in GXML_TOPICS_TO_LOC_CO:
            continue

        example = TopicClassificationTrainingDataset(
            book=topic_classification.book.barcode,
            target_topic=GXML_TOPICS_TO_LOC_CO[topic_from_metadata],
            set=None,
        )

        full_set.append(example)

    #
    # Assign each record to a set, save
    #
    if len(full_set) < 15_000:
        click.echo(
            f"Did not find enough suitable items in the collection to generate a training set. "
            + f"(min: 15,000, found: {len(full_set)})."
        )
        exit(1)

    random.shuffle(full_set)

    benchmark = full_set[0:1000]
    test = full_set[1000:6000]
    train = full_set[6000:]

    for set_name, set in {"benchmark": benchmark, "test": test, "train": train}.items():
        for item in set:
            item.set = set_name

    utils.process_db_write_batch(
        TopicClassificationTrainingDataset,
        benchmark + test + train,
        [],
        [],
    )

    click.echo(f"✅ Topic classification training set saved ({len(full_set)} entries).")
