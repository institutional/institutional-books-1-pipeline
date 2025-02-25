import multiprocessing


def get_batch_max_size(items_count: int, max_workers=0) -> int:
    """
    Returns a "likely reasonable" batch max size for multiprocessing purposes based on:
    - The number of items to process
    - The number of workers available

    If `max_workers` is not provided, it defaults to the number of available threads.
    """
    if max_workers < 1:
        max_workers = multiprocessing.cpu_count()

    # Default
    batch_size = max_workers * 4

    # Small amount of items to process
    if items_count < max_workers * 4:
        batch_size = round(items_count / max_workers)

        if batch_size < 1:
            batch_size = 1

    return batch_size
