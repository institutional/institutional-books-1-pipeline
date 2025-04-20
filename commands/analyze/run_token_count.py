import os
import traceback
import multiprocessing

import click
import tiktoken

import utils
from models import BookIO, TokenCount


@click.command("run-token-count")
@click.option(
    "--target-llm",
    default="openai/gpt-4o",
    type=str,
    help="Target text-generation model, used to identify a tokenizer. Works with OpenAI and Hugging Face models.",
)
@click.option(
    "--tokenizer-threads",
    default=multiprocessing.cpu_count(),
    type=int,
)
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
    "--db-write-batch-size",
    type=int,
    required=False,
    default=10_000,
    help="Determines the frequency at which the database will be updated (every X entries). By default: every 10,000 entries.",
)
@utils.needs_pipeline_ready
def run_token_count(
    target_llm: str,
    tokenizer_threads: int,
    overwrite: bool,
    offset: int | None,
    limit: int | None,
    db_write_batch_size: int,
):
    """
    Tokenizes the OCR'd text of each entry and saves the resulting token counts in the database.
    Uses the tokenizer of the target LLM specified via `--target-llm`.

    Notes:
    - `--target-llm` can identify both OpenAI and HuggingFace-hosted models. Prefix with `openai/` for OpenAI models.
    - Skips texts that were already analyzed with this specific tokenizer, unless instructed otherwise.
    - A valid HuggingFace token might be needed to access some of the target tokenizers.
    """
    tokenizer = None
    tokenizer_name = ""

    entries_to_update = []
    entries_to_create = []
    fields_to_update = [TokenCount.count]

    # Configure HF AutoTokenizer's parallelisim before importing it
    os.environ["TOKENIZERS_PARALLELISM"] = "true"
    os.environ["RAYON_NUM_THREADS"] = str(tokenizer_threads)

    from transformers import AutoTokenizer

    #
    # Try to load tokenizer based on model_name
    #
    try:
        if "openai/" in target_llm:
            tokenizer = tiktoken.encoding_for_model(target_llm.replace("openai/", ""))
            tokenizer_name = tokenizer.name
        else:
            tokenizer = AutoTokenizer.from_pretrained(target_llm)
            tokenizer_name = tokenizer.name_or_path
    except Exception:
        click.echo(traceback.format_exc())
        click.echo(f"Could not load tokenizer for model {target_llm}. Interrupting.")
        exit(1)

    click.echo(f"🤖 Target LLM: {target_llm}, tokenizer: {tokenizer_name}")

    #
    # Count token for each record
    #
    for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():
        token_count = None
        already_exists = False
        text_by_page = book.text
        total = 0

        # Check if token count already exists
        # NOTE: That check is done on the fly so this process can be easily parallelized.
        try:
            # throws if not found
            token_count = TokenCount.get(book=book.barcode, tokenizer=tokenizer_name)
            assert token_count
            already_exists = True

            if already_exists and not overwrite:
                click.echo(f"⏭️ #{book.barcode} {tokenizer_name} already exists.")
                continue
        except Exception:
            pass

        # Only run tokenizer if text is not empty
        if book.merged_text.strip():
            # Tiktoken (GPT-X)
            if target_llm.startswith("openai"):
                token_batches = tokenizer.encode_batch(text_by_page, num_threads=tokenizer_threads)

                for tokens in token_batches:
                    total += len(tokens)

            # Transformers (other)
            else:
                token_batches = tokenizer.batch_encode_plus(text_by_page)

                for tokens in token_batches["input_ids"]:
                    total += len(tokens)

        # Prepare record
        token_count = TokenCount() if not already_exists else token_count
        token_count.book = book.barcode
        token_count.target_llm = target_llm
        token_count.tokenizer = tokenizer_name
        token_count.count = total

        click.echo(f"🧮 #{book.barcode} + {tokenizer_name} = {total} tokens.")

        # Add to batch
        if already_exists:
            entries_to_update.append(token_count)
        else:
            entries_to_create.append(token_count)

        # Empty batches every X row
        if len(entries_to_create) + len(entries_to_update) >= db_write_batch_size:
            utils.process_db_write_batch(
                TokenCount,
                entries_to_create,
                entries_to_update,
                fields_to_update,
            )

    # Save remaining items from batches
    utils.process_db_write_batch(
        TokenCount,
        entries_to_create,
        entries_to_update,
        fields_to_update,
    )
