import os
import re
import csv

try:
    import pandas as pd
    import pandera as pa
    from pandera import Column, DataFrameSchema, Check
except ModuleNotFoundError:
    pd = None
    pa = None

RAW_ROOT = os.environ.get("EBISU_RAW_ROOT", "/import")
PROCESSED_ROOT = os.environ.get("EBISU_PROCESSED_ROOT", RAW_ROOT)
REFERENCE_OUT = os.path.join(PROCESSED_ROOT, "reference")
LOG_ROOT = os.environ.get("EBISU_LOG_ROOT", os.path.join(PROCESSED_ROOT, "logs"))
os.makedirs(REFERENCE_OUT, exist_ok=True)
os.makedirs(LOG_ROOT, exist_ok=True)

INPUT = os.path.join(REFERENCE_OUT, "ASFIS_sp_2025_preprocessed.csv")
OUTPUT = os.path.join(REFERENCE_OUT, "ASFIS_sp_2025_cleaned.csv")

# Updated schema with new column names and structure
if pa is not None:
    schema = pa.DataFrameSchema({
        "ISSCAAP_Group": Column(float, nullable=True),
        "Taxonomic_Code": Column(str),
        "Alpha3_Code": Column(str, Check.str_length(3)),
        "taxonRank": Column(str, nullable=True),
        "scientificName": Column(str, nullable=True),
        "English_name": Column(str, nullable=True),
        "French_name": Column(str, nullable=True),
        "Spanish_name": Column(str, nullable=True),
        "Arabic_name": Column(str, nullable=True),
        "Chinese_name": Column(str, nullable=True),
        "Russian_name": Column(str, nullable=True),
        "Author": Column(str, nullable=True),
        "Family": Column(str, nullable=True),
        "Order_or_higher_taxa": Column(str, nullable=True),
        "FishStat_Data": Column(bool, nullable=True)
    })
else:
    schema = None

def clean_with_csv():
    rank_mappings = {
        'species': 'Species',
        'genus': 'Genus',
        'family': 'Family',
        'order': 'Order',
        'class': 'Class',
        'phylum': 'Phylum',
        'kingdom': 'Kingdom',
        'subfamily': 'Subfamily',
        'suborder': 'Suborder',
        'infraorder': 'Infraorder',
        'superorder': 'Superorder',
        'tribe': 'Tribe',
        'subspecies': 'Subspecies'
    }

    def strip_or_empty(value):
        return value.strip() if value is not None else ''

    def normalize_scientific(name: str) -> str:
        name = strip_or_empty(name)
        if not name:
            return ''
        name = re.sub(r'\([^)]*\)', '', name)
        name = re.sub(r'\s+', ' ', name)
        return name.strip()

    def capitalize_words(value: str) -> str:
        value = strip_or_empty(value)
        if not value:
            return ''
        return ' '.join(word.capitalize() for word in value.split())

    bool_map = {'YES': 'True', 'NO': 'False'}

    with open(INPUT, 'r', newline='', encoding='utf-8-sig') as infile:
        reader = csv.DictReader(infile)
        header_map = {h: h.strip().replace(' ', '_') for h in reader.fieldnames}
        cleaned_headers = [header_map[h] for h in reader.fieldnames]

        os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
        with open(OUTPUT, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=cleaned_headers)
            writer.writeheader()

            for row in reader:
                cleaned = {}
                for original, cleaned_name in header_map.items():
                    value = row.get(original, '')
                    value = '' if value is None else value.strip()

                    if cleaned_name == 'scientificName':
                        value = normalize_scientific(value)
                    elif cleaned_name == 'taxonRank':
                        key = value.lower()
                        value = rank_mappings.get(key, value)
                    elif cleaned_name in {'Family', 'Order_or_higher_taxa'}:
                        value = capitalize_words(value)
                    elif cleaned_name == 'FishStat_Data':
                        value = bool_map.get(value.upper(), '') if value else ''

                    cleaned[cleaned_name] = value

                writer.writerow(cleaned)

    print(f"✅ ASFIS rule-based cleaning completed (fallback): {OUTPUT}")
    print("ℹ️ Schema validation skipped (pandera unavailable)")


def main():
    if pd is None or pa is None:
        print("⚠️ pandas/pandera not available; running lightweight CSV-based cleaning.")
        clean_with_csv()
        return

    try:
        print("🧼 Starting ASFIS rule-based cleaning (Step 2)...")
        df = pd.read_csv(INPUT)
        
        # Clean column names (in case there are any spacing issues)
        df.columns = df.columns.str.strip().str.replace(" ", "_")
        
        # Additional rule-based cleaning on the preprocessed data
        
        # 1. Clean scientificName field
        if 'scientificName' in df.columns:
            df['scientificName'] = df['scientificName'].str.strip()
            # Remove any remaining parenthetical content that might have been missed
            df['scientificName'] = df['scientificName'].str.replace(r'\([^)]*\)', '', regex=True).str.strip()
            # Normalize whitespace
            df['scientificName'] = df['scientificName'].str.replace(r'\s+', ' ', regex=True)
        
        # 2. Clean taxonRank field
        if 'taxonRank' in df.columns:
            df['taxonRank'] = df['taxonRank'].str.strip()
            # Standardize rank names
            rank_mappings = {
                'species': 'Species',
                'genus': 'Genus', 
                'family': 'Family',
                'order': 'Order',
                'class': 'Class',
                'phylum': 'Phylum',
                'kingdom': 'Kingdom',
                'subfamily': 'Subfamily',
                'suborder': 'Suborder',
                'infraorder': 'Infraorder',
                'superorder': 'Superorder',
                'tribe': 'Tribe',
                'subspecies': 'Subspecies'
            }
            df['taxonRank'] = df['taxonRank'].str.lower().map(rank_mappings).fillna(df['taxonRank'])
        
        # 3. Clean Family field - only capitalize properly
        if 'Family' in df.columns:
            def capitalize_family_name(name):
                if pd.isna(name) or name == '':
                    return name
                name = str(name).strip()
                # Convert each word: first letter uppercase, rest lowercase
                words = name.split()
                capitalized_words = [word.capitalize() for word in words]
                return ' '.join(capitalized_words)
            
            df['Family'] = df['Family'].apply(capitalize_family_name)
        
        # 4. Clean Order_or_higher_taxa field - only capitalize properly
        if 'Order_or_higher_taxa' in df.columns:
            def capitalize_order_name(name):
                if pd.isna(name) or name == '':
                    return name
                name = str(name).strip()
                # Convert each word: first letter uppercase, rest lowercase
                words = name.split()
                capitalized_words = [word.capitalize() for word in words]
                return ' '.join(capitalized_words)
            
            df['Order_or_higher_taxa'] = df['Order_or_higher_taxa'].apply(capitalize_order_name)

        # 5. Clean FishStat_Data field - convert YES/NO to boolean
        if 'FishStat_Data' in df.columns:
            df['FishStat_Data'] = df['FishStat_Data'].str.strip().str.upper()
            # Map YES/NO to boolean
            fishstat_mappings = {
                'YES': True,
                'NO': False
            }
            df['FishStat_Data'] = df['FishStat_Data'].map(fishstat_mappings)
        
        # Optional: print null counts for monitoring
        null_counts = df.isnull().sum()
        print(f"ℹ️ Null counts after cleaning:")
        for col, count in null_counts[null_counts > 0].items():
            print(f"   {col}: {count}")
        
        # Validate against schema
        print("🔍 Validating cleaned data against schema...")
        if schema is not None:
            schema.validate(df)
        
        # Save cleaned data
        df.to_csv(OUTPUT, index=False)
        print(f"✅ ASFIS rule-based cleaning completed: {OUTPUT}")
        print(f"📊 Cleaned records: {len(df)}")
        
    except pa.errors.SchemaError as e:
        print(f"❌ Schema validation failed: {e}")
        # Save the problematic data for review
        error_file = os.path.join(REFERENCE_OUT, "ASFIS_sp_2025_validation_errors.csv")
        df.to_csv(error_file, index=False)
        print(f"💾 Saved problematic data to: {error_file}")
        raise
    except Exception as e:
        print(f"❌ ASFIS rule-based cleaning failed: {e}")
        raise

if __name__ == "__main__":
    main()
