import json
import os
from typing import Dict, Any, List

"""
Utility script to read the photo descriptions database and output a console estimate
of Token usage and Pricing based on Vertex AI Gemini Batch discounts.
"""

def estimate_cost() -> None:
    """
    Reads the main JSON database, averages token output costs based on English character
    length heuristics, and multiplies against the Vertex AI Gemini 1.5 Flash batch pricing.
    
    Args:
        None
        
    Returns:
        None (Outputs pricing stats to the console)
    """
    # Dynamically locate the database via relative absolute path
    json_path: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "photo_descriptions.json")
    
    if not os.path.exists(json_path):
        print(f"Could not find {json_path}")
        return
        
    data: List[Dict[str, Any]] = []
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    total_photos: int = len(data)
    
    # Gemini 1.5 Flash Token Math
    # 1 Image = 258 tokens
    # Prompt text = ~40 tokens
    input_tokens_per_photo: int = 258 + 40
    total_input_tokens: int = total_photos * input_tokens_per_photo
    
    total_output_chars: int = 0
    for item in data:
        desc: str = item.get("description", "")
        total_output_chars += len(desc)
        
    # Roughly 4 characters per token for English text calculation
    total_output_tokens: int = int(total_output_chars / 4)
    
    print("=" * 50)
    print(" VERTEX AI BATCH COST ESTIMATOR (Gemini 1.5 Flash)")
    print("=" * 50)
    print(f"Total Photos Processed: {total_photos:,}")
    print(f"Estimated Input Tokens: {total_input_tokens:,}")
    print(f"Estimated Output Tokens: {total_output_tokens:,}")
    print("-" * 50)
    
    # Pricing for Gemini 1.5 Flash (Batch API is 50% off standard pricing)
    # Standard: $0.075 / 1M input tokens | $0.30 / 1M output tokens (for prompts < 128k)
    # Batch (-50%): $0.0375 / 1M input tokens | $0.15 / 1M output tokens 
    
    input_cost: float = (total_input_tokens / 1_000_000) * 0.0375
    output_cost: float = (total_output_tokens / 1_000_000) * 0.150
    total_cost: float = input_cost + output_cost
    
    print(f"Input Cost:  ${input_cost:.4f}")
    print(f"Output Cost: ${output_cost:.4f}")
    print(f"TOTAL Cost:  ${total_cost:.4f}")
    print("=" * 50)
    
    print("\nNote: To track exact billing going forward, the retrieve script has been updated to save exact token usage to api_cost_tracker.json.")

if __name__ == "__main__":
    estimate_cost()
