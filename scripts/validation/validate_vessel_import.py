#!/usr/bin/env python3
"""
Enhanced vessel import validation with comprehensive error tracking
"""
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import sys
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import re

class VesselImportValidator:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.conn = None
        self.errors = []
        self.warnings = []
        self.valid_records = 0
        self.invalid_records = 0
        
    def connect(self):
        """Establish database connection"""
        self.conn = psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )
        
    def validate_imo(self, imo: str) -> Tuple[bool, Optional[str]]:
        """Validate IMO number format and check digit"""
        if pd.isna(imo) or imo == '':
            return True, None  # Empty is valid
            
        # Clean IMO
        imo_clean = re.sub(r'[^0-9]', '', str(imo))
        
        if len(imo_clean) != 7:
            return False, f"Invalid length: {len(imo_clean)}"
            
        # Check digit validation
        try:
            digits = [int(d) for d in imo_clean[:6]]
            check_digit = int(imo_clean[6])
            calculated = sum(d * (7 - i) for i, d in enumerate(digits)) % 10
            
            if calculated != check_digit:
                return False, f"Invalid check digit: expected {calculated}, got {check_digit}"
                
            return True, imo_clean
            
        except ValueError:
            return False, "Non-numeric characters"
            
    def validate_flag_code(self, flag_code: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Validate flag code against country_iso table"""
        if pd.isna(flag_code) or flag_code == '':
            return True, None, None
            
        with self.conn.cursor() as cur:
            # Check alpha-3
            cur.execute(
                "SELECT id, alpha_3_code FROM country_iso WHERE alpha_3_code = %s",
                (flag_code.upper(),)
            )
            result = cur.fetchone()
            
            if result:
                return True, result[0], result[1]
                
            # Check alpha-2
            cur.execute(
                "SELECT id, alpha_3_code FROM country_iso WHERE alpha_2_code = %s",
                (flag_code.upper(),)
            )
            result = cur.fetchone()
            
            if result:
                return True, result[0], result[1]
                
            # Try common mappings
            mappings = {
                'UK': 'GBR',
                'ENG': 'GBR',
                'SCO': 'GBR',
                'GER': 'DEU',
                'NED': 'NLD',
                'POR': 'PRT'
            }
            
            if flag_code.upper() in mappings:
                return self.validate_flag_code(mappings[flag_code.upper()])
                
        return False, None, flag_code
        
    def validate_gear_type(self, gear_code: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Validate and fuzzy match gear types"""
        if pd.isna(gear_code) or gear_code == '':
            return True, None, None
            
        with self.conn.cursor() as cur:
            # Exact match
            cur.execute(
                "SELECT id, fao_isscfg_code FROM gear_types_fao WHERE fao_isscfg_code = %s",
                (str(gear_code),)
            )
            result = cur.fetchone()
            
            if result:
                return True, result[0], result[1]
                
            # Fuzzy match using trigram similarity
            cur.execute("""
                SELECT id, fao_isscfg_code, similarity(fao_isscfg_code, %s) as sim
                FROM gear_types_fao
                WHERE similarity(fao_isscfg_code, %s) > 0.6
                ORDER BY sim DESC
                LIMIT 1
            """, (str(gear_code), str(gear_code)))
            
            result = cur.fetchone()
            if result:
                self.warnings.append({
                    'type': 'GEAR_FUZZY_MATCH',
                    'original': gear_code,
                    'matched': result[1],
                    'similarity': result[2]
                })
                return True, result[0], result[1]
                
        return False, None, gear_code
        
    def validate_vessel_type(self, vessel_type_code: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Validate vessel type against reference table"""
        if pd.isna(vessel_type_code) or vessel_type_code == '':
            return True, None, None
            
        with self.conn.cursor() as cur:
            # Try exact match on code
            cur.execute(
                "SELECT id, vessel_type_isscfv_code FROM vessel_types WHERE vessel_type_isscfv_code = %s",
                (str(vessel_type_code),)
            )
            result = cur.fetchone()
            
            if result:
                return True, result[0], result[1]
                
            # Try match on alpha code
            cur.execute(
                "SELECT id, vessel_type_isscfv_code FROM vessel_types WHERE vessel_type_isscfv_alpha = %s",
                (str(vessel_type_code),)
            )
            result = cur.fetchone()
            
            if result:
                return True, result[0], result[1]
                
        return False, None, vessel_type_code
        
    def check_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check for duplicate vessels by IMO, name+flag combination"""
        # Check IMO duplicates
        imo_dups = df[df['imo'].notna()].groupby('imo').size()
        imo_dups = imo_dups[imo_dups > 1]
        
        for imo, count in imo_dups.items():
            self.warnings.append({
                'type': 'DUPLICATE_IMO',
                'imo': imo,
                'count': count
            })
            
        # Check name+flag duplicates
        name_flag_dups = df.groupby(['vessel_name', 'flag_code']).size()
        name_flag_dups = name_flag_dups[name_flag_dups > 1]
        
        for (name, flag), count in name_flag_dups.items():
            self.warnings.append({
                'type': 'DUPLICATE_NAME_FLAG',
                'vessel_name': name,
                'flag': flag,
                'count': count
            })
            
        return df
        
    def validate_dataframe(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Validate entire dataframe with comprehensive checks"""
        print(f"Validating {len(df)} records from {source_file}")
        
        # Add validation columns
        df['validation_status'] = 'VALID'
        df['validation_errors'] = None
        df['validation_warnings'] = None
        df['imo_validated'] = None
        df['flag_uuid'] = None
        df['gear_type_uuid'] = None
        df['vessel_type_uuid'] = None
        
        # Check for duplicates
        df = self.check_duplicates(df)
        
        # Validate each record
        for idx, row in df.iterrows():
            errors = []
            warnings = []
            
            # Validate vessel name
            if pd.isna(row.get('vessel_name')) or str(row.get('vessel_name')).strip() == '':
                errors.append({
                    'field': 'vessel_name',
                    'error': 'Missing vessel name'
                })
            
            # Validate IMO
            if 'imo' in row and pd.notna(row['imo']):
                is_valid, result = self.validate_imo(row['imo'])
                if not is_valid:
                    warnings.append({
                        'field': 'imo',
                        'error': result
                    })
                else:
                    df.at[idx, 'imo_validated'] = result
                    
            # Validate flag
            if 'flag_code' in row:
                is_valid, uuid, code = self.validate_flag_code(row.get('flag_code'))
                if is_valid:
                    df.at[idx, 'flag_uuid'] = uuid
                elif pd.notna(row.get('flag_code')):
                    errors.append({
                        'field': 'flag_code',
                        'error': f'Unknown flag code: {code}'
                    })
                    
            # Validate gear type
            if 'gear_type' in row:
                is_valid, uuid, code = self.validate_gear_type(row.get('gear_type'))
                if is_valid:
                    df.at[idx, 'gear_type_uuid'] = uuid
                elif pd.notna(row.get('gear_type')):
                    warnings.append({
                        'field': 'gear_type',
                        'warning': f'Unknown gear type: {code}'
                    })
                    
            # Validate vessel type
            if 'vessel_type' in row:
                is_valid, uuid, code = self.validate_vessel_type(row.get('vessel_type'))
                if is_valid:
                    df.at[idx, 'vessel_type_uuid'] = uuid
                elif pd.notna(row.get('vessel_type')):
                    warnings.append({
                        'field': 'vessel_type',
                        'warning': f'Unknown vessel type: {code}'
                    })
                    
            # Update validation status
            if errors:
                df.at[idx, 'validation_status'] = 'ERROR'
                df.at[idx, 'validation_errors'] = json.dumps(errors)
                self.invalid_records += 1
            elif warnings:
                df.at[idx, 'validation_status'] = 'WARNING'
                df.at[idx, 'validation_warnings'] = json.dumps(warnings)
                self.valid_records += 1
            else:
                self.valid_records += 1
                
        return df
        
    def generate_report(self, df: pd.DataFrame, output_file: str):
        """Generate comprehensive validation report"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'valid_records': self.valid_records,
            'invalid_records': self.invalid_records,
            'warning_records': len(df[df['validation_status'] == 'WARNING']),
            'validation_rate': (self.valid_records / len(df) * 100) if len(df) > 0 else 0,
            'errors_by_type': {},
            'warnings_by_type': {},
            'sample_errors': [],
            'sample_warnings': []
        }
        
        # Aggregate errors
        error_df = df[df['validation_status'] == 'ERROR']
        if len(error_df) > 0:
            report['sample_errors'] = error_df.head(10).to_dict('records')
            
        # Aggregate warnings  
        warning_df = df[df['validation_status'] == 'WARNING']
        if len(warning_df) > 0:
            report['sample_warnings'] = warning_df.head(10).to_dict('records')
            
        # Write report
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
            
        print(f"\nValidation Report:")
        print(f"Total Records: {report['total_records']}")
        print(f"Valid: {report['valid_records']} ({report['validation_rate']:.1f}%)")
        print(f"Errors: {report['invalid_records']}")
        print(f"Warnings: {report['warning_records']}")
        print(f"\nReport saved to: {output_file}")
        
        return report
        
    def save_to_staging(self, df: pd.DataFrame, staging_table: str = 'vessel_staging_validated'):
        """Save validated data to staging table"""
        # Only save records that passed validation or have warnings
        valid_df = df[df['validation_status'].isin(['VALID', 'WARNING'])]
        
        print(f"Saving {len(valid_df)} validated records to staging...")
        
        # Use copy_expert for efficient bulk insert
        columns = [
            'vessel_name', 'imo_validated', 'flag_uuid', 'gear_type_uuid',
            'vessel_type_uuid', 'validation_status', 'validation_errors',
            'validation_warnings'
        ]
        
        # Create CSV in memory
        from io import StringIO
        csv_buffer = StringIO()
        valid_df[columns].to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)
        
        with self.conn.cursor() as cur:
            cur.copy_expert(f"""
                COPY {staging_table} ({','.join(columns)})
                FROM STDIN WITH CSV
            """, csv_buffer)
            
        self.conn.commit()
        print(f"Successfully saved {len(valid_df)} records to staging")

def main():
    # Database configuration
    db_config = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT'),
        'database': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD')
    }
    
    # Validate required environment variables
    required_env_vars = ['POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    
    if len(sys.argv) < 2:
        print("Usage: validate_vessel_import.py <input_csv_file>")
        sys.exit(1)
        
    input_file = sys.argv[1]
    
    # Initialize validator
    validator = VesselImportValidator(db_config)
    validator.connect()
    
    # Read and validate data
    # Force all columns to be read as strings to prevent data corruption
    df = pd.read_csv(input_file, dtype=str)
    validated_df = validator.validate_dataframe(df, input_file)
    
    # Generate report
    report_file = f"{input_file}.validation_report.json"
    validator.generate_report(validated_df, report_file)
    
    # Save to staging if validation passed minimum threshold
    if validator.valid_records / len(df) >= 0.90:  # 90% threshold
        validator.save_to_staging(validated_df)
    else:
        print("ERROR: Validation rate below 95% threshold. Data not loaded to staging.")
        sys.exit(1)

if __name__ == '__main__':
    main()