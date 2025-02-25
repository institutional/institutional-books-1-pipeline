def get_batch_max_size(items_count: int, max_workers: int) -> int:
    """
    Returns a "likely reasonable" batch max size for multiprocessing purposes based on:
    - The number of items to process
    - The number of workers available
    """
    # Default
    batch_size = max_workers * 4

    # Small amount of items to process
    if items_count < max_workers * 4:
        batch_size = round(items_count / max_workers)

        if batch_size < 1:
            batch_size = 1

    return batch_size
