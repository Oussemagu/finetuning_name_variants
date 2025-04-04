from pymongo import MongoClient
import json
def normalize_script(script):
            """
                Normalize the script name to a standard ISO 15924 format (TitleCase).
            """
            if script!= None:
                script = script.lower()
                match script:
                    case "latin" | "latn":
                        return "Latn"
                    case "arabic" | "arab":
                        return "Arab"
                    case "cyrillic" | "cyrl":
                        return "Cyrl"
                    case "greek" | "grek":
                        return "Grek"
                    case "hebrew" | "hebr":
                        return "Hebr"
                    case "devanagari" | "deva":
                        return "Deva"
                    case "han" | "hans" | "hant" | "chinese" | "hani" | "hanzi":
                        return "Hani"
                    case "hang" | "hangul":
                        return "Hang"
                    case "ethiopic" | "ethi":
                        return "Ethi"
                    case _:
                        return None  # Return None for unsupported scripts
            else:
                return None

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["Normalized_db"]
collection1 = db["final_target_1"]
collection2 = db["final_target_2"]
collection3 = db["final_target_3"]
target_db = client["extracted_names"]
collection_target = target_db["extracted_names"]
count = 0
count_primary = 0
count_documents_with_primary = 0

# Collections to process
collections = [collection1, collection2, collection3]

# List to store processed names
processed_data = []

for collection in collections:
    for document in collection.find():
        # Extract the names field
        names = document.get("names", [])
        doc_type = document.get("type", "")
        cleaned_names = []
        primary_name = None
        
        for name_entry in names:
            # Remove 'lang' key and rename 'variantStrength' to 'strength'
            cleaned_entry = {
                "name": name_entry.get("name"),
                "type": name_entry.get("type"),
                "script": normalize_script(name_entry.get("script")),
                "strength": name_entry.get("variantStrength", name_entry.get("strength"))
            }
            cleaned_names.append(cleaned_entry)
            
            # Set the first name as primary if not set yet
            if primary_name is None and name_entry.get("name"):
                primary_name = name_entry.get("name")
                count_primary += 1
        
        # Only add to processed_data if we found a primary name
        if primary_name:
            processed_doc = {
                "type": doc_type,
                "names": cleaned_names,
                "primary_name": primary_name
            }
            processed_data.append(processed_doc)
            count_documents_with_primary += 1
        
        count += 1

# Save to JSON file
with open("cleaned_names.json", "w+", encoding="utf-8") as json_file:
    json.dump(processed_data, json_file, indent=4, ensure_ascii=False)

print(f"Processed {count} documents in total")
print(f"Found {count_primary} primary names")
print(f"Saved {count_documents_with_primary} documents with primary names to JSON file")