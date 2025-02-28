import peewee


def process_db_write_batch(
    model: peewee.Model,
    entries_to_create: list[peewee.Model],
    entries_to_update: list[peewee.Model],
    fields_to_update: list[peewee.Field],
) -> bool:
    """
    Processes a batch of database create/update operations.

    Notes:
    - `entries_to_create` an `entries_to_update` are emptied in place
    """
    # Calculates the optimal size for SQLite based on max variable number
    # https://www.sqlite.org/limits.html#max_variable_number
    sqlite_batch_size = 32766 // len(model._meta.fields.keys())

    if entries_to_create:
        model.bulk_create(entries_to_create, batch_size=sqlite_batch_size)
        entries_to_create.clear()

    if entries_to_update:
        model.bulk_update(
            entries_to_update,
            fields=fields_to_update,
            batch_size=sqlite_batch_size,
        )
        entries_to_update.clear()

    return True
