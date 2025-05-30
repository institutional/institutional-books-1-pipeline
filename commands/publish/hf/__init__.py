import click
from datasets import Dataset, Features, Value, Sequence

from .generate import generate
from .push import push
from .check_integrity import check_integrity


@click.group("hf")
def hf():
    """Command group: publish > hf."""
    pass


hf.add_command(generate)
hf.add_command(push)
hf.add_command(check_integrity)

HF_DATASET_FEATURES = Features(
    {
        "barcode_src": Value("string"),
        "title_src": Value("string"),
        "author_src": Value("string"),
        "date1_src": Value("string"),
        "date2_src": Value("string"),
        "date_types_src": Value("string"),
        "page_count_src": Value("int32"),
        "token_count_o200k_base_gen": Value("int32"),
        "language_src": Value("string"),
        "language_gen": Value("string"),
        "language_distribution_gen": Sequence(
            {
                "language": Value("string"),
                "proportion": Value("double"),
            }
        ),
        "topic_or_subject_src": Value("string"),
        "topic_or_subject_gen": Value("string"),
        "topic_or_subject_score_gen": Value("double"),
        "genre_or_form_src": Value("string"),
        "general_note_src": Value("string"),
        "ocr_score_src": Value("int32"),
        "ocr_score_gen": Value("int32"),
        "likely_duplicates_barcodes_gen": Sequence(Value("string")),
        "text_analysis_gen": {
            "text_by_page_src": {
                "tokenizability_score": Value("double"),
                "char_count": Value("int32"),
                "word_count": Value("int32"),
                "word_count_unique": Value("int32"),
                "word_type_token_ratio": Value("double"),
                "bigram_count": Value("int32"),
                "bigram_count_unique": Value("int32"),
                "bigram_type_token_ratio": Value("double"),
                "trigram_count": Value("int32"),
                "trigram_count_unique": Value("int32"),
                "trigram_type_token_ratio": Value("double"),
                "sentence_count": Value("int32"),
                "sentence_count_unique": Value("int32"),
            },
            "text_by_page_gen": {
                "tokenizability_score": Value("double"),
                "char_count": Value("int32"),
                "word_count": Value("int32"),
                "word_count_unique": Value("int32"),
                "word_type_token_ratio": Value("double"),
                "bigram_count": Value("int32"),
                "bigram_count_unique": Value("int32"),
                "bigram_type_token_ratio": Value("double"),
                "trigram_count": Value("int32"),
                "trigram_count_unique": Value("int32"),
                "trigram_type_token_ratio": Value("double"),
                "sentence_count": Value("int32"),
                "sentence_count_unique": Value("int32"),
            },
        },
        "identifiers_src": {
            "lccn": Sequence(Value("string")),
            "isbn": Sequence(Value("string")),
            "ocolc": Sequence(Value("string")),
        },
        "hathitrust_data_ext": {
            "url": Value("string"),
            "rights_code": Value("string"),
            "reason_code": Value("string"),
            "last_check": Value("string"),
        },
        "text_by_page_src": Sequence(Value("large_string")),
        "text_by_page_gen": Sequence(Value("large_string")),
    }
)
""" 
    HuggingFace features for this dataset. 
    `text_by_page_xyz` columns are removed in metadata-only mode. 
"""


def get_hf_row_from_book(
    book,
    likely_duplicates: dict,
    pd_only=True,
    include_text=False,
) -> dict | None:
    """
    Processes a book into a HuggingFace dataset row matching HF_DATASET_FEATURES.

    Notes:
    - `likely_duplicates`: output of `utils.get_filtered_duplicates()`.
    """
    from models import (
        BookIO,
        HathitrustRightsDetermination,
        LanguageDetection,
        TokenCount,
        TextAnalysis,
        OCRPostProcessingTextAnalysis,
    )

    from const import HATHITRUST_PD_CODES, HATHITRUST_PD_STRING

    rights_determination: HathitrustRightsDetermination = None
    token_count_o200k_base: int = 0

    row = {}
    row["barcode_src"] = book.barcode

    #
    # Skip if book is not PD
    #
    if pd_only:
        rights_determination = book.hathitrustrightsdetermination_set[0]

        assert rights_determination

        try:
            assert rights_determination.rights_code in HATHITRUST_PD_CODES
            assert rights_determination.us_rights_string == HATHITRUST_PD_STRING
        except:
            return None

    #
    # Skip book if they have no text (token_count < 100)
    #
    token_count_o200k_base = (
        TokenCount.select(TokenCount.count)
        .where(
            TokenCount.book == book.barcode,
            TokenCount.target_llm == "openai/gpt-4o",
        )
        .scalar()
    )

    if token_count_o200k_base <= 100:
        return None

    #
    # Text
    #
    if include_text:
        row["text_by_page_src"] = book.text_by_page
        row["text_by_page_gen"] = None

        try:
            row["text_by_page_gen"] = book.postprocessed_ocr["text_by_page"]
        except:
            pass

    #
    # Base bibliographic info
    #
    row["title_src"] = " ".join(
        [
            book.csv_data["gxml Title"].strip(),
            book.csv_data["gxml Title Remainder"].strip(),
        ]
    ).strip()

    if row["title_src"] and row["title_src"][-1] == "/":
        row["title_src"] = row["title_src"][:-1].strip()

    row["author_src"] = " ".join(
        [
            book.csv_data["gxml Author (Personal Name)"].strip(),
            book.csv_data["gxml Author (Corporate Name)"].strip(),
            book.csv_data["gxml Author (Meeting Name)"].strip(),
        ]
    ).strip()

    if row["author_src"] and row["author_src"][-1] == ",":
        row["author_src"] = row["author_src"][:-1].strip()

    row["date1_src"] = book.csv_data["gxml Date 1"].strip()
    row["date2_src"] = book.csv_data["gxml Date 2"].strip()

    if not row["date1_src"]:
        row["date1_src"] = None

    if not row["date2_src"]:
        row["date2_src"] = None

    row["date_types_src"] = book.csv_data["gxml Date Type"]

    row["general_note_src"] = book.csv_data["gxml General Note"]

    #
    # Language(s)
    #
    row["language_src"] = book.mainlanguage_set[0].from_metadata_iso639_3
    row["language_gen"] = book.mainlanguage_set[0].from_detection_iso639_3

    row["language_distribution_gen"] = []

    for detection in (
        LanguageDetection.select()
        .where(LanguageDetection.book == book.barcode)
        .order_by(LanguageDetection.token_count.desc())
        .iterator()
    ):
        if (
            detection.token_count < 1000
            or not detection.iso639_3
            or not detection.percentage_of_total
        ):
            continue

        row["language_distribution_gen"].append(
            {
                "language": detection.iso639_3,
                "proportion": detection.percentage_of_total,
            },
        )

    #
    # Topic and genre
    #
    row["topic_or_subject_src"] = book.topicclassification_set[0].from_metadata
    row["topic_or_subject_gen"] = book.topicclassification_set[0].from_detection
    row["topic_or_subject_score_gen"] = book.topicclassification_set[0].detection_confidence
    row["genre_or_form_src"] = book.genreclassification_set[0].from_metadata

    #
    # OCR quality
    #
    row["ocr_score_src"] = book.ocrquality_set[0].from_metadata
    row["ocr_score_gen"] = book.ocrquality_set[0].from_detection

    #
    # Likely duplicates
    #
    simhash = book.scannedtextsimhash_set[0].hash
    row["likely_duplicates_barcodes_gen"] = None

    if simhash and likely_duplicates.get(simhash, None) != None:
        dupes_as_bookios = likely_duplicates[simhash]
        dupes_as_barcodes = []

        for dupe in dupes_as_bookios:
            if dupe.barcode == book.barcode:
                continue

            dupes_as_barcodes.append(dupe.barcode)

        row["likely_duplicates_barcodes_gen"] = dupes_as_barcodes

    #
    # Text analysis (from source and OCR postprocessing)
    #
    row["page_count_src"] = book.pagecount_set[0].count_from_metadata
    row["token_count_o200k_base_gen"] = token_count_o200k_base

    ta_src: TextAnalysis = book.textanalysis_set[0]
    ta_gen: OCRPostProcessingTextAnalysis = book.ocrpostprocessingtextanalysis_set[0]

    row["text_analysis_gen"] = {
        "text_by_page_src": None,
        "text_by_page_gen": None,
    }

    for is_gen, analysis in enumerate([ta_src, ta_gen]):
        if not analysis or analysis.tokenizability_o200k_base_ratio is None:
            continue

        ta = {}
        ta["tokenizability_score"] = analysis.tokenizability_o200k_base_ratio
        ta["char_count"] = analysis.char_count
        ta["word_count"] = analysis.word_count
        ta["word_count_unique"] = analysis.word_count_unique
        ta["word_type_token_ratio"] = analysis.word_type_token_ratio
        ta["bigram_count"] = analysis.bigram_count
        ta["bigram_count_unique"] = analysis.bigram_count_unique
        ta["bigram_type_token_ratio"] = analysis.bigram_type_token_ratio
        ta["trigram_count"] = analysis.trigram_count
        ta["trigram_count_unique"] = analysis.trigram_count_unique
        ta["trigram_type_token_ratio"] = analysis.trigram_type_token_ratio
        ta["sentence_count"] = analysis.sentence_count
        ta["sentence_count_unique"] = analysis.sentence_count_unique

        # is_gen -> i == 1
        key = "text_by_page_gen" if is_gen else "text_by_page_src"
        row["text_analysis_gen"][key] = ta

    #
    # Identifiers
    #
    row["identifiers_src"] = {}

    row["identifiers_src"]["lccn"] = [
        lccn.strip()
        for lccn in book.csv_data["gxml Library of Congress Control Number"].split(",")
        if lccn.strip()
    ]

    row["identifiers_src"]["isbn"] = [
        isbn.strip() for isbn in book.csv_data["gxml ISBN"].split(",") if isbn.strip()
    ]

    row["identifiers_src"]["ocolc"] = [
        ocolc.strip() for ocolc in book.csv_data["gxml OCoLC Number(s)"].split(",") if ocolc.strip()
    ]

    # Hathitrust data
    hathitrust_data_ext = {}

    hathitrust_data_ext["url"] = f"https://hdl.handle.net/2027/{rights_determination.htid}"
    hathitrust_data_ext["rights_code"] = rights_determination.rights_code
    hathitrust_data_ext["reason_code"] = rights_determination.reason_code

    hathitrust_data_ext["last_check"] = rights_determination.retrieved_date.isoformat()
    hathitrust_data_ext["last_check"] = hathitrust_data_ext["last_check"][0:10]

    row["hathitrust_data_ext"] = hathitrust_data_ext

    return row
