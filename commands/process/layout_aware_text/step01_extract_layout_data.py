import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback
from datetime import datetime

import click
from bs4 import BeautifulSoup, NavigableString, Tag
import numpy as np
import cv2
from PIL import Image, ImageDraw, ImageFont

import utils
from models import BookIO, PageCount, MainLanguage, HathitrustRightsDetermination, LayoutData
from models.layout_data import (
    PageMetadata,
    OCRChunk,
    OCRChunkType,
    LayoutSeparator,
    LayoutSeparatorType,
)

TARGET_LANGUAGES = """
eng,fra,deu,spa,por,ita,nld,swe,nor,dan,fin,isl,
ron,pol,ces,slk,hun,hrv,bos,slv,est,lav,lit,gle,
gla,cym,mlt,epo,ina,ido,lat,afr,swa,tgl,vie,ind,
mlg,hat,som,yor,ibo,zul,xho,fij,mri,smo,ton,cha,
mah,bis,tpi,pap,grn,que,aym,fao,ltz,glg,cat,eus,
oci,srd,fur,lld,cos,arg,ast,wln,bre,cor,glv,fry,
lim,szl,rus,ukr,bel,bul,srp,mkd,tat,chu,ell,grc
"""
""" List of ISO-639-3 codes for languages this method can likely handle. """

MIN_OCR_CONFIDENCE_SCORE = 70
""" OCR confidence score threshold under which chunks will be skipped. """

LAYOUT_SEPARATOR_LINE_THICKNESS_TOLERANCE = 10
""" Thickness tolerance for line detection, in pixels. """

LAYOUT_SEPARATOR_LINE_LENGTH_RATIO = 0.33
""" Length, in % of the document's width or height, a detected line needs to be in order to be considered a layout separator. """

BBOX_SCALING_FACTOR = 4
""" Reduces the size of all bboxes for faster processing. """


@click.command("step01-extract-layout-data")
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="If set, overwrites existing entries.",
)
@click.option(
    "--offset",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the collection (sorted by BookIO.barcode).",
)
@click.option(
    "--limit",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the collection (sorted by BookIO.barcode).",
)
@click.option(
    "--max-workers",
    type=int,
    required=False,
    default=multiprocessing.cpu_count(),
    help="Determines how many subprocesses can be run in parallel.",
)
@utils.needs_pipeline_ready
def step01_extract_layout_data(
    overwrite: bool,
    offset: int | None,
    limit: int | None,
    max_workers: int,
):
    """
    Layout-aware text processing, step 01:
    Extracts layout data from Google Books hOCR and raw scans.
    Focuses on capturing word-level bboxes and detecting layout separators (lines separating columns and sometimes sections).

    Notes:
    - LayoutData entries are stored on disc under `OUTPUT_MISC_DIR_PATH`.
    - Skips entries that were already processed, unless instructed otherwise.
    - All dimensions / bboxes are shrunk by 4 for easier and faster processing.
    """
    #
    # Data dependency checks
    #
    try:
        assert BookIO.select().count() == PageCount.select().count()
    except:
        click.echo("This command needs page count data.")
        exit(1)

    try:
        assert BookIO.select().count() == MainLanguage.select().count()

        count = (
            MainLanguage.select().where(MainLanguage.from_detection_iso693_3.is_null(False)).count()
        )
        assert count
    except:
        click.echo("This command needs language detection data.")
        exit(1)

    # Hathitrust rights determination data
    try:
        assert BookIO.select().count() == HathitrustRightsDetermination.select().count()
    except:
        click.echo("This command needs Hathitrust rights determination data.")
        exit(1)

    #
    # Process books
    #
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():
            future = executor.submit(process_book, book, overwrite)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not extract layout data. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def process_book(book: BookIO, overwrite: bool) -> bool:
    """
    Runs layout data extraction on all the pages of a given book and saves results to disk.
    """
    processing_start = datetime.now()
    processing_end = None

    target_languages = TARGET_LANGUAGES.strip().replace("\n", "").split(",")

    already_exists = False
    entry: LayoutData = None
    should_skip = False

    book_raw_data = None

    page_metadata_by_page: list[PageMetadata] = []
    words_by_page: list[OCRChunk] = []
    separators_by_page: list[LayoutSeparator] = []

    #
    # Check if record already exists
    #
    try:
        already_exists = LayoutData.exists(barcode=book.barcode)

        if already_exists and not overwrite:
            click.echo(f"⏭️ #{book.barcode} already processed. Skipping")
            return False
    except:
        pass

    #
    # Skip processing book if:
    # - It doesn't have text
    # - It is not PD
    # - Its main language is not in the target list
    #
    if book.pagecount_set[0].count_from_ocr < 1:
        should_skip = True

    if (
        should_skip is False
        and book.hathitrustrightsdetermination_set[0].rights_code not in ["pd", "pdus", "cc-zero"]
        and book.hathitrustrightsdetermination_set[0].us_rights_string != "Full view"
    ):
        should_skip = True

    if (
        should_skip is False
        and book.mainlanguage_set[0].from_detection_iso693_3 not in target_languages
    ):
        should_skip = True

    # Write empty record if book should not be processed.
    if should_skip:
        click.echo(f"⏭️ #{book.barcode} was skipped.")
        return False

    #
    # Retrieve the book's raw data
    #
    book_raw_data = book.raw_data

    #
    # Step 0 - Pre-populate page-indexed lists
    #
    for i in range(0, len(book_raw_data.hocr)):
        page_metadata_by_page.append(None)
        words_by_page.append(None)
        separators_by_page.append(None)

    #
    # Step 1 - Extract page and word-level metadata from raw hOCR
    #
    for index, raw_hocr in enumerate(book_raw_data.hocr):
        page_metadata, words = parse_hocr(raw_hocr)
        page_metadata_by_page[index] = page_metadata
        words_by_page[index] = words

    #
    # Step 2 - Extract layout separators
    #
    for index, image_bytes in enumerate(book_raw_data.images):
        page_metadata = page_metadata_by_page[index]
        separators = detect_layout_separators(image_bytes, page_metadata)
        separators_by_page[index] = separators

    #
    # Step 3 - Save
    #
    entry = LayoutData()
    entry.barcode = book.barcode
    entry.page_metadata_by_page = page_metadata_by_page
    entry.words_by_page = words_by_page
    entry.separators_by_page = separators_by_page
    entry.save()

    processing_end = datetime.now()
    click.echo(f"⏱️ #{book.barcode} - Processed in {processing_end - processing_start}")
    return True


def parse_hocr(raw_hocr: str) -> tuple[PageMetadata, list[OCRChunk]]:
    """
    Extract page and word-level metadata from raw hOCR

    Note:
    - Documents were OCR'd by Google at 50% of the page's original size
    - All dimensions and bounding boxes are further reduced for faster processing (BBOX_SCALING_FACTOR)
    """
    parsed_hocr = BeautifulSoup(raw_hocr, features="html.parser")
    page_metadata = PageMetadata()
    words: list[OCRChunk] = []

    #
    # Page metadata
    #
    page_attrs = parsed_hocr.select_one("div.ocr_page").get("title")

    for prop in page_attrs.split(";"):
        if prop.startswith("bbox"):
            _, __, ___, page_metadata.width, page_metadata.height = prop.split(" ")

        if prop.startswith("ppageno"):
            page_metadata.number = prop.split(" ")[-1]

        if prop.startswith("ocrp_lang"):
            page_metadata.language = prop.split(" ")[-1]

    page_metadata.width = int(page_metadata.width) // BBOX_SCALING_FACTOR
    page_metadata.height = int(page_metadata.height) // BBOX_SCALING_FACTOR
    page_metadata.number = int(page_metadata.number)

    #
    # Word-level metadata
    #
    for node in parsed_hocr.select(".ocrx_word"):
        next_node = node.next_sibling
        word = OCRChunk()
        word.type = OCRChunkType.WORD
        word.text = node.text

        # Grab text from node, account for possible white space or line-break after that
        word.text = node.text

        if isinstance(next_node, NavigableString):
            word.text += " "

        if isinstance(next_node, Tag) and next_node.get("class") in [
            "ocrx_block",
            "ocr_par",
            "ocr_line",
        ]:
            word.text += " "

        if isinstance(next_node, Tag) and next_node.name == "br":
            word.text += " "

        # Parse bounding box and confidence score
        for attr in node.get("title").split(";"):
            if attr.startswith("bbox"):
                bbox = attr.split(" ")[1:]
                word.x_min, word.y_min, word.x_max, word.y_max = [
                    int(val) // BBOX_SCALING_FACTOR for val in bbox
                ]

            if attr.startswith("x_wconf"):
                word.confidence = int(attr.split(" ")[1])

        # Skip words with a confidence score that is too low
        if word.confidence < MIN_OCR_CONFIDENCE_SCORE:
            continue

        words.append(word)

    return (page_metadata, words)


def detect_layout_separators(
    image_bytes: bytes,
    page_metadata: PageMetadata,
    line_thickness_tolerance: int = LAYOUT_SEPARATOR_LINE_THICKNESS_TOLERANCE,
    line_length_ratio: float = LAYOUT_SEPARATOR_LINE_LENGTH_RATIO,
) -> list[LayoutSeparator]:
    """
    Tries to detect horizontal and vertical layout separators in a raw scan.
    """
    image_np = None
    image_binary = None

    h_min_line_length: int
    v_min_line_length: int

    separators = []

    # Open, resize and convert image
    image_np = cv2.imdecode(
        np.frombuffer(image_bytes, np.uint8),
        cv2.IMREAD_GRAYSCALE,
    )

    image_np = cv2.resize(
        image_np,
        (page_metadata.width, page_metadata.height),
        interpolation=cv2.INTER_AREA,
    )

    # Set minimun line length as X% of respective dimension
    h_min_line_length = int(page_metadata.width * line_length_ratio)
    v_min_line_length = int(page_metadata.height * line_length_ratio)

    # Create binary image
    image_binary = cv2.adaptiveThreshold(
        image_np,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY_INV,
        11,
        2,
    )

    #
    # Detect horizontal lines
    #
    h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (h_min_line_length, 1))

    h_morph = cv2.morphologyEx(image_binary, cv2.MORPH_OPEN, h_kernel)

    h_contours, _ = cv2.findContours(h_morph, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    #
    # Detect vertical lines
    #
    v_close_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 5))

    v_open_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 30))

    v_closed_morph = cv2.morphologyEx(image_binary, cv2.MORPH_CLOSE, v_close_kernel)

    v_morph = cv2.morphologyEx(v_closed_morph, cv2.MORPH_OPEN, v_open_kernel)

    v_contours, _ = cv2.findContours(v_morph, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    #
    # Process horizontal lines
    #
    for contour in h_contours:
        x, y, w, h = cv2.boundingRect(contour)
        if h <= line_thickness_tolerance and w >= h_min_line_length:
            separators.append(
                LayoutSeparator(
                    x_min=x,
                    y_min=y,
                    x_max=x + max(1, w),
                    y_max=y + max(1, h),
                    type=LayoutSeparatorType.HORIZONTAL,
                )
            )

    #
    # Process vertical lines
    #
    for contour in v_contours:
        x, y, w, h = cv2.boundingRect(contour)
        if w <= line_thickness_tolerance and h >= v_min_line_length:
            separators.append(
                LayoutSeparator(
                    x_min=x,
                    y_min=y,
                    x_max=x + max(1, w),
                    y_max=y + max(1, h),
                    type=LayoutSeparatorType.VERTICAL,
                )
            )

    return separators
