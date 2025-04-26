import multiprocessing
from statistics import median
from concurrent.futures import ProcessPoolExecutor, as_completed

import peewee


def get_filtered_duplicates(max_workers: int = multiprocessing.cpu_count()) -> dict:
    """
    Returns a filtered list of duplicate books.
    Groups books by simhash, further filter them by detected language and continuous char count.
    """
    from models import BookIO, ScannedTextSimhash, MainLanguage, TextAnalysis

    hashes_to_books = {}
    hashes_to_discard = set()

    #
    # Data dependencies check
    #
    try:
        assert BookIO.select().count() == ScannedTextSimhash.select().count()
    except:
        raise Exception("Simhash data is not available.")

    try:
        assert BookIO.select().count() == TextAnalysis.select().count()
    except:
        raise Exception("Text analysis data is not available.")

    try:
        count = (
            MainLanguage.select().where(MainLanguage.from_detection_iso639_3.is_null(False)).count()
        )
        assert count
    except:
        raise Exception("Language detection data is not available.")

    #
    # Group all books by simhash into a hashmap
    #
    hash_group_subquery = (
        ScannedTextSimhash.select(ScannedTextSimhash.hash)
        .where(ScannedTextSimhash.hash.is_null(False))
        .group_by(ScannedTextSimhash.hash)
        .having(peewee.fn.COUNT(ScannedTextSimhash.hash) > 1)
    )

    for entry in (
        ScannedTextSimhash.select()
        .where(ScannedTextSimhash.hash.in_(hash_group_subquery))
        .iterator()
    ):
        hash = entry.hash
        book = entry.book

        if hash is None:
            continue

        if hashes_to_books.get(hash, None) is None:
            hashes_to_books[hash] = []

        hashes_to_books[hash].append(book)

    #
    # For each group, filter and eliminate false positives
    #
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        # Parallelize filtering
        for hash, books in hashes_to_books.items():
            future = executor.submit(__filter, books)
            futures[future] = hash

        # As filtered lists come back:
        # - Update book list for hash
        # - Mark for deletion hashes where there is no duplicates
        for future in as_completed(futures):
            try:
                filtered = future.result()
                hash = futures[future]

                hashes_to_books[hash] = filtered

                if len(filtered) < 2:
                    hashes_to_discard.add(hash)

                del future

            except Exception as err:
                executor.shutdown(wait=False, cancel_futures=True)
                raise err

    # Discard hashes that no longer carry duplicates
    for hash in hashes_to_discard:
        del hashes_to_books[hash]

    return hashes_to_books


def __filter(books: list) -> list:
    """
    Chains filtering operations on group of suspected duplicates.
    """
    return __filter_by_continuous_char_length(__filter_by_detected_language(books))


def __filter_by_detected_language(books: list) -> list:
    """
    Eliminates from group of suspected duplicates books that don't have the same main detected language.
    """
    books_by_language = {}

    for book in books:
        lang = "und"

        if book.mainlanguage_set:
            lang = book.mainlanguage_set[0].from_detection_iso639_3

        if books_by_language.get(lang, None) is None:
            books_by_language[lang] = []

        books_by_language[lang].append(book)

    books_by_language = dict(
        sorted(
            books_by_language.items(),
            key=lambda x: len(x[1]),
            reverse=True,
        )
    )

    return books_by_language[list(books_by_language.keys())[0]]


def __filter_by_continuous_char_length(books: list) -> list:
    """
    Eliminates from group of suspected duplicates books that have character count different > or < 15% of group median.

    Notes:
    - Books are compared using "continous char length" (char length without spaces and line breaks)
    """
    filtered = []

    median_char_count = median([book.textanalysis_set[0].char_count_continous for book in books])

    for book in books:
        char_count = book.textanalysis_set[0].char_count_continous

        if char_count and char_count > int(median_char_count * 1.15):
            continue

        if char_count and char_count < int(median_char_count / 1.15):
            continue

        filtered.append(book)

    return filtered
