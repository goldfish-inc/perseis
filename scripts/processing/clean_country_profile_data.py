import pandas as pd
import numpy as np
import os

def clean_country_iso_foc():
    """Clean country_iso_foc.csv - remove manual ID generation, let DB handle UUIDs"""
    print("🧼 Cleaning country_iso_foc.csv...")
    
    try:
        df = pd.read_csv("/import/country_iso_foc.csv")
        
        # NO LONGER ADD MANUAL ID - database will generate UUID
        # NO LONGER MAP TO country_id integers - use alpha_3_code for UUID lookup in SQL
        
        # Clean string field
        df['alpha_3_code'] = df['alpha_3_code'].astype(str).str.strip()
        
        # Standardize boolean field name and clean it
        # Handle both possible column names for flexibility
        foc_column = None
        if 'isFOC' in df.columns:
            foc_column = 'isFOC'
        elif 'is_foc' in df.columns:
            foc_column = 'is_foc'
        else:
            raise ValueError("FOC status column not found (looking for 'isFOC' or 'is_foc')")
        
        # Standardize to 'is_foc' and clean boolean field
        df['is_foc'] = df[foc_column].astype(bool)
        
        # Keep only essential columns for SQL import (UUID mapping will happen in SQL)
        df = df[['alpha_3_code', 'is_foc']]
        
        # Remove any duplicates
        original_count = len(df)
        df = df.drop_duplicates()
        if len(df) < original_count:
            print(f"   🔧 Removed {original_count - len(df)} duplicate records")
        
        # Save cleaned file
        df.to_csv("/import/country_iso_foc_cleaned.csv", index=False)
        print(f"✅ Cleaned country_iso_foc.csv: {len(df)} records")
        print(f"📋 Sample data: {df.head(2).to_dict('records')}")
        print("🔄 UUID mapping will be handled during SQL import phase")
        return df
        
    except Exception as e:
        print(f"❌ Error cleaning country_iso_foc.csv: {e}")
        import traceback
        traceback.print_exc()
        raise

def clean_country_iso_ilo_c188():
    """Clean country_iso_ILO_c188.csv - remove manual ID generation, handle dates properly"""
    print("🧼 Cleaning country_iso_ILO_c188.csv...")
    
    try:
        df = pd.read_csv("/import/country_iso_ILO_c188.csv")
        
        # NO LONGER ADD MANUAL ID - database will generate UUID
        # NO LONGER MAP TO country_id integers - use alpha_3_code for UUID lookup in SQL
        
        # Clean string field
        df['alpha_3_code'] = df['alpha_3_code'].astype(str).str.strip()
        
        # Standardize boolean field name and clean it
        # Handle both possible column names for flexibility
        ratified_column = None
        if 'isC188ratified' in df.columns:
            ratified_column = 'isC188ratified'
        elif 'is_c188_ratified' in df.columns:
            ratified_column = 'is_c188_ratified'
        else:
            raise ValueError("C188 ratification column not found (looking for 'isC188ratified' or 'is_c188_ratified')")
        
        # Standardize to 'is_c188_ratified'
        df['is_c188_ratified'] = df[ratified_column].astype(bool)
        
        # Handle date fields with flexible column naming
        date_field_mapping = {
            'dateEnteredForce': 'date_entered_force',
            'date_entered_force': 'date_entered_force',
            'dateRatified': 'date_ratified', 
            'date_ratified': 'date_ratified',
            'dateFutureEnterForceBy': 'date_future_enter_force_by',
            'date_future_enter_force_by': 'date_future_enter_force_by'
        }
        
        # Process date fields - handle all date columns
        date_columns_processed = []
        for old_name, new_name in date_field_mapping.items():
            if old_name in df.columns and new_name not in date_columns_processed:
                print(f"   📅 Processing date field: {old_name} -> {new_name}")
                # Convert to datetime, handle errors gracefully
                df[new_name] = pd.to_datetime(df[old_name], format='%Y-%m-%d', errors='coerce')
                # Convert NaT to None for SQL compatibility
                df[new_name] = df[new_name].where(pd.notnull(df[new_name]), None)
                date_columns_processed.append(new_name)
        
        # Handle optional string fields with flexible column naming
        string_field_mapping = {
            'conventionOrg': 'convention_org',
            'convention_org': 'convention_org',
            'convention_shortname': 'convention_shortname',
            'convention_fullname': 'convention_fullname'
        }
        
        # Process string fields - handle all string columns
        string_columns_processed = []
        for old_name, new_name in string_field_mapping.items():
            if old_name in df.columns and new_name not in string_columns_processed:
                print(f"   📝 Processing string field: {old_name} -> {new_name}")
                # Clean and handle empty strings
                df[new_name] = df[old_name].astype(str).str.strip()
                df[new_name] = df[new_name].replace(['', 'nan', 'NaN'], None)
                string_columns_processed.append(new_name)
        
        # Build columns to keep (only include columns that exist)
        columns_to_keep = ['alpha_3_code', 'is_c188_ratified']
        
        # Add optional columns if they were processed
        optional_columns = [
            'date_entered_force', 'date_ratified', 'date_future_enter_force_by',
            'convention_org', 'convention_shortname', 'convention_fullname'
        ]
        
        for col in optional_columns:
            if col in df.columns:
                columns_to_keep.append(col)
        
        # Keep only processed columns
        df = df[columns_to_keep]
        
        # Remove any duplicates
        original_count = len(df)
        df = df.drop_duplicates()
        if len(df) < original_count:
            print(f"   🔧 Removed {original_count - len(df)} duplicate records")
        
        # Save cleaned file
        df.to_csv("/import/country_iso_ILO_c188_cleaned.csv", index=False)
        print(f"✅ Cleaned country_iso_ILO_c188.csv: {len(df)} records")
        print(f"📋 Final columns: {list(df.columns)}")
        print(f"📋 Sample data: {df.head(2).to_dict('records')}")
        
        # Check for any remaining non-date data in date columns
        for col in ['date_entered_force', 'date_ratified', 'date_future_enter_force_by']:
            if col in df.columns:
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    print(f"   📅 {col}: {len(non_null_values)} non-null values, sample: {non_null_values.iloc[0] if len(non_null_values) > 0 else 'None'}")
        
        print("🔄 UUID mapping will be handled during SQL import phase")
        return df
        
    except Exception as e:
        print(f"❌ Error cleaning country_iso_ILO_c188.csv: {e}")
        import traceback
        traceback.print_exc()
        raise

def show_country_source_mapping_plan():
    """Show the planned source mapping for country profile data"""
    print("\n📋 PLANNED COUNTRY PROFILE SOURCE MAPPING:")
    print("=" * 55)
    print("Country Profile Table -> Original Source")
    print("-" * 55)
    print("country_iso_foc -> FOC")
    print("country_iso_ilo_c188 -> ILO C188") 
    print("\n⚠️  These sources must exist in original_sources.csv")
    print("   with source_shortname = 'FOC' and 'ILO C188'")
    print("\n🔄 UUID Mapping Strategy:")
    print("   ✅ No more manual ID generation")
    print("   ✅ Keep alpha_3_code for country_id UUID lookup in SQL")
    print("   ✅ Database generates all primary key UUIDs")
    print("   ✅ Foreign key relationships established in SQL import")
    print("=" * 55)

def validate_cleaned_data():
    """Validate that cleaned files are ready for SQL import"""
    print("\n🔍 Validating cleaned data for SQL import...")
    
    validation_passed = True
    
    # Check FOC file
    try:
        foc_df = pd.read_csv("/import/country_iso_foc_cleaned.csv")
        required_foc_cols = ['alpha_3_code', 'is_foc']
        
        missing_foc_cols = set(required_foc_cols) - set(foc_df.columns)
        if missing_foc_cols:
            print(f"❌ FOC file missing columns: {missing_foc_cols}")
            validation_passed = False
        else:
            print(f"✅ FOC file structure valid: {len(foc_df)} records, columns: {list(foc_df.columns)}")
            
    except Exception as e:
        print(f"❌ Could not validate FOC file: {e}")
        validation_passed = False
    
    # Check ILO file
    try:
        ilo_df = pd.read_csv("/import/country_iso_ILO_c188_cleaned.csv")
        required_ilo_cols = ['alpha_3_code', 'is_c188_ratified']
        
        missing_ilo_cols = set(required_ilo_cols) - set(ilo_df.columns)
        if missing_ilo_cols:
            print(f"❌ ILO file missing columns: {missing_ilo_cols}")
            validation_passed = False
        else:
            print(f"✅ ILO file structure valid: {len(ilo_df)} records, columns: {list(ilo_df.columns)}")
            
    except Exception as e:
        print(f"❌ Could not validate ILO file: {e}")
        validation_passed = False
    
    return validation_passed

def main():
    """Clean all country profile data files with UUID support"""
    print("🚀 Starting ENHANCED country profile data cleaning (UUID Support)...")
    print("🔧 Changes: Removed manual ID generation, database will use UUIDs")
    print("📊 Source mapping will happen during SQL import phase")
    print("🔄 No dependency on country_iso_cleaned.csv - direct alpha_3_code lookup")
    
    try:
        # Show the planned source mapping
        show_country_source_mapping_plan()
        
        # Clean individual files
        print("\n🧼 Cleaning country profile files...")
        clean_country_iso_foc()
        clean_country_iso_ilo_c188()
        
        # Validate the results
        if validate_cleaned_data():
            print("\n✅ All ENHANCED country profile data cleaning completed!")
            print("🎯 Ready for UUID-based import with source tracking")
            print("📊 All manual ID generation removed - database will handle UUIDs")
            print("🔗 Source_id mapping will be handled in SQL import phase")
            print("🏳️ Country profile tables will connect to original_sources via FOC and ILO C188")
            print("🔄 country_id foreign keys will be resolved via alpha_3_code -> UUID lookup")
        else:
            print("\n❌ Data validation failed - check errors above")
            raise ValueError("Cleaned data validation failed")
        
    except Exception as e:
        print(f"❌ Enhanced country profile data cleaning failed: {e}")
        raise

if __name__ == "__main__":
    main()