## Identifying Japanese Books

The first step in processing Japanese book data is to identify books that are primarily in the Japanese language. This pipeline provides two main ways to ascertain the language of a book: metadata and automated language detection. Both methods store their results in the `MainLanguage` table, linked to each `BookIO` entry. The ISO 639-3 code for Japanese is "jpn".

### 1. Using Language Information from Metadata

Some books may have language information embedded in their source metadata. The pipeline extracts this information using the following command:

```bash
python pipeline.py analyze extract-main-language-from-metadata
```

This command populates the `from_metadata_iso639_3` field in the `MainLanguage` table.

### 2. Using Automated Language Detection

The pipeline can also analyze the OCR text of each book to detect its language(s):

```bash
python pipeline.py analyze run-language-detection
```

This command populates the `from_detection_iso639_3` field in the `MainLanguage` table with the most prevalent language detected in the text. It also stores the distribution of all detected languages in the `language_detection` table (not detailed here).

### 3. Querying for Japanese Books

Once the language analysis commands have been run, you can identify Japanese books by querying the database. You would typically look for entries where `from_metadata_iso639_3` or `from_detection_iso639_3` is 'jpn'.

Using Peewee ORM (as shown in the main README.md), an example query might look like this:

```python
from models import MainLanguage

# Find books identified as Japanese by metadata
japanese_books_metadata = MainLanguage.select().where(MainLanguage.from_metadata_iso639_3 == 'jpn')

# Find books identified as Japanese by text detection
japanese_books_detection = MainLanguage.select().where(MainLanguage.from_detection_iso639_3 == 'jpn')

# Combine results (e.g., using Python sets or further SQL queries)
for lang_entry in japanese_books_metadata:
    print(f"Book barcode (metadata): {lang_entry.book_id}")

for lang_entry in japanese_books_detection:
    print(f"Book barcode (detection): {lang_entry.book_id}")

```

You can adapt these queries to select books based on your preferred criteria (e.g., relying more on detection or metadata, or requiring both to agree). The `book_id` attribute of a `MainLanguage` entry corresponds to the `barcode` in the `BookIO` table.

---
## General Guidelines for Cleaning Japanese Book Data

Once Japanese books are identified, the next step is to ensure the quality and usability of their OCR-extracted text. Japanese text presents unique challenges that may require specific cleaning and normalization procedures. The existing OCR post-processing in this pipeline (`python pipeline.py process ocr-postprocessing step03-process`) is primarily designed for Latin-script languages and may not be suitable for Japanese without significant modification.

### Common Challenges with Japanese OCR

1.  **Character Sets and Encoding:**
    *   Ensure text is consistently encoded (UTF-8 is standard).
    *   Japanese uses multiple character sets (Hiragana, Katakana, Kanji, Romaji). OCR can sometimes confuse similar-looking characters or produce errors if the correct character set is not recognized.

2.  **Vertical Text (縦書き - tategaki) vs. Horizontal Text (横書き - yokogaki):**
    *   Traditional Japanese is written vertically, from top to bottom, with columns running from right to left. Modern Japanese also uses horizontal, left-to-right layout, especially in digital media and technical texts.
    *   OCR software might struggle with correct segmentation and ordering of text, especially if a document mixes both layouts or if the layout detection is imperfect. Reconstructing the correct reading order can be complex.

3.  **Furigana (振り仮名):**
    *   These are small phonetic characters (kana) printed next to Kanji to indicate their pronunciation.
    *   OCR might misinterpret furigana as part of the main text, intersperse it incorrectly, or omit it. Deciding whether to keep, separate, or discard furigana depends on the downstream use case.

4.  **Ligatures and Character Variants:**
    *   Older Japanese texts might use character variants (異体字 - itaiji) or ligatures that are not common in modern Japanese. Normalizing these to standard forms might be necessary.

5.  **Punctuation and Symbols:**
    *   Japanese punctuation (e.g., 「 」, 『 』, 。, 、) differs from Western punctuation. OCR systems need to correctly identify and render these.
    *   Special symbols or layout elements (e.g., warichu - 割注, notes in double lines) can also pose challenges.

6.  **OCR Noise and Errors:**
    *   General OCR errors like misrecognized characters, merged/split characters, or inclusion of page artifacts (stains, bleed-through) also apply.
    *   Specific to Japanese, errors might include confusion between similar radicals in Kanji or misidentification of small kana.

### Potential Approaches and Tools (High-Level)

Addressing these challenges often requires a combination of techniques, and specific tool implementation is beyond the scope of this pipeline's current capabilities. However, here are some general approaches:

1.  **Character Normalization:**
    *   Use libraries like `unicodedata` in Python to normalize Unicode characters (e.g., NFC, NFKC).
    *   Convert half-width Katakana (半角カナ) to full-width (全角カナ) if consistency is desired.
    *   Map archaic or variant characters to their modern equivalents if necessary for the application.

2.  **Layout Analysis and Reordering:**
    *   For complex layouts, especially vertical text, simple line-by-line reading of OCR output is often insufficient.
    *   Specialized tools or libraries for document layout analysis might be needed to correctly segment text blocks and reconstruct reading order. (Note: This pipeline does not currently include such specialized tools for Japanese.)
    *   Heuristics based on character types (e.g., punctuation indicating ends of sentences/clauses) and line spacing might offer partial solutions.

3.  **Furigana Handling:**
    *   Develop heuristics or use pattern matching (regex) to identify potential furigana based on character size (if available from OCR) or typical positioning.
    *   Decide on a strategy: remove furigana, enclose it in parentheses, or attempt to link it to the corresponding Kanji.

4.  **Regular Expressions for Common Issues:**
    *   Regex can be useful for cleaning specific, predictable OCR error patterns, removing unwanted spaces, or standardizing punctuation.

5.  **Manual Review and Correction (Sampling):**
    *   For high-quality datasets, especially for smaller collections, manual review and correction of a sample of texts can help identify common error patterns and refine automated cleaning scripts.

6.  **Domain-Specific Dictionaries:**
    *   If the books belong to a specific domain, using domain-specific dictionaries might help in correcting OCR errors for technical terms or proper nouns.

### Using the Existing OCR Post-Processing Framework

While the current `ocr-postprocessing` scripts are not tailored for Japanese, the framework itself (classifying lines of text and then recomposing them) could potentially be adapted. This would require:
    *   Training a new classification model (Step 1 & 2 of `ocr-postprocessing`) with Japanese text examples and appropriate line type labels relevant to Japanese documents.
    *   Developing new heuristics in Step 3 (`process_book` and `convert_page_chunks_to_text` in `step03_process.py`) to handle Japanese-specific text features.

This would be a significant development effort. For now, it's recommended to focus on identifying Japanese books and then applying external or custom scripts for cleaning, based on the guidelines above.

---
## Creating and Managing a "Clean Data Set" of Japanese Books

After identifying Japanese books and applying any desired cleaning procedures (potentially using external scripts or tools, as discussed in the previous section), you'll need a way to manage this curated subset of data. This might involve exporting the data or flagging it within the existing database structure.

### Exporting Cleaned Japanese Book Data

1.  **Filtering and Selecting Data:**
    *   Use the querying methods described in "Identifying Japanese Books" to get a list of relevant `BookIO` entries (e.g., by their barcodes).
    *   If you have performed cleaning and stored the cleaned text (e.g., in new files or a separate data store), you'll need to associate this cleaned text with the corresponding book barcodes.

2.  **Using Existing Export Commands (with customization):**
    *   The pipeline includes commands for exporting data, such as `python pipeline.py export stats overview` or scripts in the `commands/export/misc/` directory.
    *   Currently, these commands export data based on the existing database fields. To export your *cleaned* Japanese text, you would likely need to:
        *   Modify an existing export script or create a new one.
        *   This new/modified script would first identify the Japanese books (e.g., by reading a list of barcodes or querying the `MainLanguage` table).
        *   Then, instead of pulling text data directly from `BookIO.text_by_page` or `BookIO.postprocessed_ocr`, it would fetch your externally cleaned text for these specific barcodes.
        *   The script could then output this data in a desired format (e.g., CSV, JSONL).

    An example snippet for a custom export script might involve:

    ```python
    # (Assuming 'cleaned_japanese_texts' is a dictionary mapping barcode to cleaned text string)
    # and 'japanese_book_barcodes' is a list of relevant barcodes.

    import csv
    from models import BookIO

    output_data = []
    for barcode in japanese_book_barcodes:
        book_entry = BookIO.get_or_none(BookIO.barcode == barcode)
        if book_entry:
            # Access your cleaned text, for example:
            # cleaned_text = get_my_externally_cleaned_text(barcode)
            # For this example, let's assume it's in a dict
            cleaned_text = cleaned_japanese_texts.get(barcode, "")

            output_data.append({
                "barcode": barcode,
                "title": book_entry.csv_data.get("title_src", ""), # Example metadata
                "cleaned_text": cleaned_text
                # Add other relevant metadata fields
            })

    # Write to CSV
    # with open("cleaned_japanese_books.csv", "w", newline="", encoding="utf-8") as csvfile:
    #     writer = csv.DictWriter(csvfile, fieldnames=output_data[0].keys())
    #     writer.writeheader()
    #     writer.writerows(output_data)
    ```

### Flagging or Storing Cleaned Data Within the Database

Alternatively, if the cleaning process is standardized and you wish to integrate the cleaned text more directly into the pipeline's database:

1.  **Adding a New Field/Table:**
    *   You could extend the `BookIO` model (or a related model) with a new field specifically for cleaned Japanese text. This would require modifying `models/book_io.py` and potentially creating a new database migration.
    *   Alternatively, a new table could be created, linking `BookIO.barcode` to the cleaned text and any metadata about the cleaning process (e.g., version of cleaning script used).

2.  **Updating `BookIO.postprocessed_ocr`:**
    *   If your Japanese cleaning process can be framed within the existing `BookIO.postprocessed_ocr` structure (a dictionary with `stats` and `text_by_page`), you could potentially adapt the `BookIO.postprocessed_ocr` setter to store your Japanese-specific cleaned text.
    *   This would involve ensuring your cleaning output matches this structure. You would then save it using `book.postprocessed_ocr = your_cleaned_data_dict`.
    *   **Caution:** This approach means that `postprocessed_ocr` would contain data from different processing methods depending on the language. Clear documentation and careful handling in downstream tasks would be essential.

### Considerations

*   **Scalability:** For large numbers of books, exporting to files might be more manageable than storing very large text fields directly in the primary SQLite database.
*   **Versioning:** If your cleaning process evolves, keep track of which version of the cleaning was applied to which books.
*   **Purpose of the Clean Data Set:** The best way to manage the data will depend on how you intend to use it (e.g., for direct analysis, as input to other models, for publication).

For most users, **exporting the identified Japanese book data along with their cleaned text to a separate set of files (e.g., JSONL or CSV)** is likely the most straightforward approach, offering flexibility without requiring modifications to the core database schema.

---
