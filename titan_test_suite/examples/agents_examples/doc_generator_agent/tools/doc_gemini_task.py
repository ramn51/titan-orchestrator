import sys
import os
import json
from google import genai
from google.genai import types
from titan_sdk import TitanClient
from dotenv import load_dotenv

def generate_doc_json(language, code_content):
    doc_style = "Javadoc" if language == "Java" else "Python Docstrings"
    
    prompt = f"""
    You are an expert Documentation Engineer.
    
    TASK: Generate high-quality {doc_style} for the following {language} code.
    
    CRITICAL RULES:
    1. Output STRICTLY valid JSON. Do not include markdown wrappers.
    2. The JSON must contain a "docs" array.
    3. The "signature" field MUST be an EXACT string copy of the line of code defining the class or method, matching the source code exactly.
    
    CODE:
    {code_content}
    
    EXPECTED JSON FORMAT:
    {{
      "docs": [
        {{
          "signature": "exact line of code here",
          "docstring": "the generated documentation here"
        }}
      ]
    }}
    """
    
    load_dotenv()
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    
    try:
        print("Calling Gemini 2.5 Flash API for JSON...")
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.1 # Low variance to keep signatures exact
            )
        )
        return json.loads(response.text)
    except Exception as e:
        print(f"[API ERROR] {e}")
        return None

# --- MAIN LOGIC ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python doc_gemini_task.py <raw_key>")
        sys.exit(1)

    raw_key = sys.argv[1] 
    file_path = raw_key.replace("RAW:", "")
    language = "Java" if file_path.endswith(".java") else "Python"
    print(f"[WORKER] Processing {os.path.basename(file_path)}...")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            original_code = f.read()
    except Exception as e:
        print(f"[ERROR] Could not read file direct from disk: {e}")
        sys.exit(1)

    doc_data = generate_doc_json(language, original_code)

    if doc_data and "docs" in doc_data:
        updated_code = original_code
        
        for item in doc_data["docs"]:
            # Strip whitespace to prevent indentation mismatch failures
            sig = item.get("signature", "").strip()
            doc = item.get("docstring", "")
            
            if sig and doc and sig in updated_code:
                # Java: Docstring goes BEFORE the method signature
                if language == "Java":
                    updated_code = updated_code.replace(sig, f"{doc}\n    {sig}")
                # Python: Docstring goes AFTER the method signature
                else:
                    updated_code = updated_code.replace(sig, f"{sig}\n        {doc}")

        # Overwrite the actual source file safely
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(updated_code)
        
        print(f"[SUCCESS] Documentation safely injected into {os.path.basename(file_path)}")
    else:
        print("[FAIL] Failed to generate or parse JSON.")
        sys.exit(1)