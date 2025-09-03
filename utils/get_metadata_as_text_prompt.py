import iso639


def get_metadata_as_text_prompt(
    book,
    skip_topic: bool = False,
    skip_genre: bool = False,
) -> str:
    """
    Returns available metadata for a given book at a text prompt.
    Can be used for classification tasks.

    Notes:
    - If `skip_topic` is set, the prompt will not include existing topic/subject metadata
    - If `skip_genre` is set, the prompt will not include existing genre/form metadata
    """
    from models import BookIO

    prompt = ""

    # Title
    title = ""

    for gxml_key in ["MARC Title", "MARC Title Remainder"]:
        title += book.metadata.get(gxml_key, "")
        title += " "

    if title.strip():
        prompt += f"Title: {title}\n"

    # Author
    author = ""

    for gxml_key in [
        "MARC Author Personal",
        "MARC Author Corporate",
        "MARC Author Meeting",
    ]:
        author += book.metadata.get(gxml_key, "")
        author += " "

    if author.strip():
        prompt += f"Author: {author}\n"

    # Year
    try:
        year_of_publication = book.yearofpublication_set[0].year
        assert year_of_publication
        prompt += f"Year: {year_of_publication}\n"
    except Exception:
        pass

    # Language
    try:
        main_language = book.mainlanguage_set[0].from_detection_iso639_3
        assert main_language

        main_language = iso639.Lang(pt3=main_language).name
        prompt += f"Language: {main_language}\n"
    except Exception:
        pass

    # Topic
    if not skip_topic:
        try:
            topic = book.topicclassification_set[0].from_metadata
            assert topic
            prompt += f"Subject/Topic: {topic}\n"
        except Exception:
            pass

    # Genre
    if not skip_genre:
        try:
            genre = book.genreclassification_set[0].from_metadata
            assert genre
            prompt += f"Genre/Form: {genre}\n"
        except Exception:
            pass

    # General note
    general_note = book.metadata.get("MARC General Note", None)

    if general_note:
        prompt += f"General note: {general_note}"

    # Throw if no metadata was available
    if not prompt.strip():
        raise Exception("No metadata available.")

    return prompt
