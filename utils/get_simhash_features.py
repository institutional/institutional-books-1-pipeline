import re


def get_simhash_features(text: str, shingle_width: int = 5) -> list:
    """
    Processes a string into a list of shingles for use with Simhash.
    via: https://leons.im/posts/a-python-implementation-of-simhash-algorithm/
    """
    text = text.lower()
    text = re.sub(r"[^\w]+", "", text)
    return [text[i : i + shingle_width] for i in range(max(len(text) - shingle_width + 1, 1))]
