#!/usr/bin/env python3
"""
Download a single book from the Institutional Books HuggingFace dataset for testing.
"""

from datasets import load_dataset
import json
import os

def download_single_book(barcode=None, save_dir="./test_data"):
    """
    Download a single book from the institutional-books-1.0 dataset.
    
    Args:
        barcode (str, optional): Specific barcode to download. If None, downloads first book.
        save_dir (str): Directory to save the book data
    """
    print("Loading dataset from HuggingFace...")
    
    # Load dataset in streaming mode to avoid downloading everything
    dataset = load_dataset("instdin/institutional-books-1.0", split="train", streaming=True)
    
    os.makedirs(save_dir, exist_ok=True)
    
    for i, row in enumerate(dataset):
        # If specific barcode requested, check for match
        if barcode and row["barcode_src"] != barcode:
            continue
            
        # Save the book data
        book_data = {
            "barcode": row["barcode_src"],
            "title": row["title_src"],
            "author": row["author_src"],
            "text_by_page_src": row["text_by_page_src"],
            "text_by_page_gen": row.get("text_by_page_gen"),
            "language_src": row.get("language_src"),
            "language_gen": row.get("language_gen"),
            "page_count_src": row.get("page_count_src"),
            "token_count_o200k_base_gen": row.get("token_count_o200k_base_gen"),
            "ocr_score_src": row.get("ocr_score_src"),
            "ocr_score_gen": row.get("ocr_score_gen"),
        }
        
        filename = f"{save_dir}/book_{row['barcode_src']}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(book_data, f, indent=2, ensure_ascii=False)
        
        print(f"Downloaded book: {row['title_src']}")
        print(f"Barcode: {row['barcode_src']}")
        print(f"Author: {row['author_src']}")
        print(f"Pages: {len(row['text_by_page_src']) if row['text_by_page_src'] else 0}")
        print(f"Saved to: {filename}")
        
        return row["barcode_src"]
    
    if barcode:
        print(f"Book with barcode {barcode} not found in dataset")
        return None
    else:
        print("No books found in dataset")
        return None

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Download a single book from Institutional Books dataset")
    parser.add_argument("--barcode", help="Specific barcode to download")
    parser.add_argument("--save-dir", default="./test_data", help="Directory to save book data")
    
    args = parser.parse_args()
    
    downloaded_barcode = download_single_book(args.barcode, args.save_dir)
    
    if downloaded_barcode:
        print(f"\nTo test with this book, you can create a minimal database entry or")
        print(f"modify the pipeline to work with the downloaded JSON file at:")
        print(f"./test_data/book_{downloaded_barcode}.json")