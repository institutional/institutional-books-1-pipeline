import os
import traceback
import multiprocessing

import click
import tiktoken

import utils
from models import BookIO, TokenCount
import const


@click.command("token-count")
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
    "--start",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@click.option(
    "--end",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@utils.needs_pipeline_ready
def token_count(
    target_llm: str,
    tokenizer_threads: int,
    overwrite: bool,
    start: int | None,
    end: int | None,
):
    """
    Tokenizes the text of each newspaper issue and saves the resulting token counts in the database.
    Uses the tokenizer of the target LLM specified via `--target-llm`.

    Notes:
    - Skips texts that were already analyzed with this specific tokenizer, unless instructed otherwise.
    - A valid HuggingFace access might be needed to access some of the target tokenizers: See huggingface_cli's documentation.
    """
    tokenizer = None
    tokenizer_name = ""

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
    for book in BookIO.select().offset(start).limit(end).order_by(BookIO.barcode).iterator():
        token_count = None
        text_by_page = book.jsonl_data["text_by_page"]
        total = 0

        # Check if token count already exists
        # NOTE: That check is done on the fly so this process can be easily parallelized.
        try:
            # throws if not found
            token_count = TokenCount.get(book=book.barcode, tokenizer=tokenizer_name)

            if not overwrite:
                click.echo(f"⏭️ #{book.barcode} {tokenizer_name} already exists.")
                continue
        except Exception:
            pass

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

        # Save
        token_count = TokenCount() if not token_count else token_count
        token_count.book = book.barcode
        token_count.target_llm = target_llm
        token_count.tokenizer = tokenizer_name
        token_count.count = total

        token_count.save(force_insert=True if not overwrite else False)

        click.echo(f"🧮 #{book.barcode} + {tokenizer_name} = {total} tokens.")
