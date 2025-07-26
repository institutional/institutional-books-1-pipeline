#!/usr/bin/env python3
"""
Test different ways to access specific books from HuggingFace dataset
"""

from datasets import load_dataset
import time

def test_streaming_access():
    """Test streaming access to find specific book"""
    print("🔍 Testing streaming access to find specific barcode...")
    
    target_barcode = "32044000000018"  # The one we downloaded
    
    dataset = load_dataset("instdin/institutional-books-1.0", split="train", streaming=True)
    
    start_time = time.time()
    found = False
    
    for i, row in enumerate(dataset):
        if row["barcode_src"] == target_barcode:
            print(f"✅ Found barcode {target_barcode} at position {i}")
            print(f"   Title: {row['title_src']}")
            print(f"   Time taken: {time.time() - start_time:.2f}s")
            found = True
            break
        
        if i % 1000 == 0:
            print(f"   Searched {i} records...")
            
        # Stop after reasonable search
        if i > 10000:
            print(f"   Stopped search after {i} records")
            break
    
    if not found:
        print(f"❌ Barcode {target_barcode} not found in first 10k records")

def test_filter_access():
    """Test filtering dataset by barcode"""
    print("\n🔍 Testing filter access...")
    
    try:
        # This might not work with streaming
        dataset = load_dataset("instdin/institutional-books-1.0", split="train")
        
        # Try filtering
        filtered = dataset.filter(lambda x: x["barcode_src"] == "32044000000018")
        
        if len(filtered) > 0:
            print(f"✅ Found via filter: {filtered[0]['title_src']}")
        else:
            print("❌ No results from filter")
            
    except Exception as e:
        print(f"❌ Filter method failed: {e}")

def test_direct_access():
    """Test if we can access by index"""
    print("\n🔍 Testing direct index access...")
    
    try:
        dataset = load_dataset("instdin/institutional-books-1.0", split="train")
        
        # Try accessing first few records
        for i in range(min(5, len(dataset))):
            row = dataset[i]
            print(f"   Record {i}: {row['barcode_src']} - {row['title_src'][:50]}...")
            
    except Exception as e:
        print(f"❌ Direct access failed: {e}")

def test_barcode_search_function(target_barcode: str):
    """Function to find a specific book by barcode"""
    print(f"\n🎯 Searching for barcode: {target_barcode}")
    
    dataset = load_dataset("instdin/institutional-books-1.0", split="train", streaming=True)
    
    for i, row in enumerate(dataset):
        if row["barcode_src"] == target_barcode:
            return {
                "found": True,
                "position": i,
                "data": row
            }
        
        # Reasonable search limit
        if i > 50000:
            break
    
    return {"found": False, "position": None, "data": None}

if __name__ == "__main__":
    print("🧪 Testing HuggingFace Dataset Access Methods")
    print("=" * 60)
    
    # Test different access methods
    test_streaming_access()
    test_filter_access() 
    test_direct_access()
    
    # Test our search function
    result = test_barcode_search_function("32044000000018")
    if result["found"]:
        print(f"✅ Custom search found book at position {result['position']}")
    else:
        print("❌ Custom search didn't find the book")
    
    print("\n" + "=" * 60)
    print("📝 Summary:")
    print("- Streaming: Sequential search through dataset")
    print("- Filter: May require downloading full dataset") 
    print("- Direct: Access by position if known")
    print("- Best for specific book: Cache locally or use streaming search")