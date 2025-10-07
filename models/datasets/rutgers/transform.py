"""
OW1 Bottom Trawl to Darwin Core Archive - Complete Pipeline
Extracts from ERDDAP → Transforms → Validates → Writes DwC-A
"""

import pandas as pd
from pathlib import Path
import zipfile
from typing import Dict, Tuple, Any
import requests
from io import StringIO
import yaml
import warnings


# ============================================================================
# CONFIGURATION
# ============================================================================

ERDDAP_SERVER = "https://rowlrs-data.marine.rutgers.edu/erddap"

# ERDDAP dataset IDs
DATASET_IDS = {
    'tows': 'bottom_trawl_survey_ow1_tows',
    'catch': 'bottom_trawl_survey_ow1_catch',
    'species': 'species_id_codes'
}

OUTPUT_DIR = Path("dwc_archive_output")

# LinkML mapping schema path
MAPPING_SCHEMA = "ow1-to-dwc-mappings.yaml"

# Meta.xml template path
META_XML_TEMPLATE = "meta.xml"

# ============================================================================
# GENERIC MAPPING ENGINE
# ============================================================================

class MappingEngine:
    """
    Generic transformation engine that reads LinkML mapping schemas and applies
    transformations to DataFrames based on exact_mappings annotations.
    """
    
    def __init__(self, mapping_schema_path: str):
        """
        Initialize the mapping engine with a LinkML mapping schema.
        
        Args:
            mapping_schema_path: Path to the LinkML mapping schema YAML file
        """
        self.schema_path = Path(mapping_schema_path)
        self.schema = self._load_schema()
        self.classes = self.schema.get('classes', {})
        self.slots = self.schema.get('slots', {})
        
    def _load_schema(self) -> Dict:
        """Load and parse the LinkML schema YAML file."""
        with open(self.schema_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _extract_source_field(self, mapping: str) -> str:
        """
        Extract the source field name from a mapping string.
        
        Args:
            mapping: String like "ow1_catch:time" or "ow1_catch:latitude"
            
        Returns:
            The field name after the colon (e.g., "time", "latitude")
        """
        if ':' in mapping:
            return mapping.split(':', 1)[1]
        return mapping
    
    def _get_slot_mappings(self, class_name: str) -> Dict[str, Dict]:
        """
        Get all slot mappings for a given class, including inherited slots.
        
        Args:
            class_name: Name of the target class (e.g., "Event", "Occurrence")
            
        Returns:
            Dictionary mapping target field names to their mapping specifications
        """
        if class_name not in self.classes:
            raise ValueError(f"Class '{class_name}' not found in schema")
        
        class_def = self.classes[class_name]
        slot_names = class_def.get('slots', [])
        
        mappings = {}
        for slot_name in slot_names:
            if slot_name in self.slots:
                slot_def = self.slots[slot_name]
                mappings[slot_name] = {
                    'definition': slot_def,
                    'exact_mappings': slot_def.get('exact_mappings', []),
                    'range': slot_def.get('range', 'string'),
                    'required': slot_def.get('required', False)
                }
        
        return mappings
    
    def _convert_type(self, value: Any, target_range: str) -> Any:
        """
        Convert a value to the target type specified in the LinkML range.
        
        Args:
            value: The value to convert
            target_range: LinkML range (e.g., "string", "float", "integer")
            
        Returns:
            Converted value, or None if conversion fails
        """
        if pd.isna(value):
            return None
        
        try:
            if target_range == 'integer':
                return int(value)
            elif target_range == 'float' or target_range == 'double':
                return float(value)
            elif target_range == 'string':
                return str(value)
            else:
                # Unknown type, return as-is
                return value
        except (ValueError, TypeError):
            warnings.warn(f"Could not convert '{value}' to {target_range}")
            return None
    
    def transform_dataframe(self, 
                          source_df: pd.DataFrame, 
                          target_class: str,
                          strict: bool = True) -> pd.DataFrame:
        """
        Transform a source DataFrame to match a target LinkML class structure
        using exact_mappings only.
        
        Args:
            source_df: Input DataFrame with source field names
            target_class: Name of target class in the mapping schema
            strict: If True, only process fields with exact_mappings (recommended)
            
        Returns:
            Transformed DataFrame with target field names
        """
        mappings = self._get_slot_mappings(target_class)
        result = pd.DataFrame()
        
        for target_field, mapping_spec in mappings.items():
            exact_mappings = mapping_spec['exact_mappings']
            
            # Only process if there's exactly one exact_mapping (strict 1:1 rename)
            if len(exact_mappings) != 1:
                if strict:
                    # Skip fields without exactly one exact mapping
                    continue
                else:
                    warnings.warn(
                        f"Skipping '{target_field}': requires exactly one exact_mapping, "
                        f"found {len(exact_mappings)}"
                    )
                    continue
            
            # Extract source field name
            source_field = self._extract_source_field(exact_mappings[0])
            
            # Check if source field exists
            if source_field not in source_df.columns:
                if mapping_spec['required']:
                    warnings.warn(
                        f"Required field '{target_field}' cannot be mapped: "
                        f"source field '{source_field}' not found in DataFrame"
                    )
                continue
            
            # Copy and convert the column
            target_range = mapping_spec['range']
            result[target_field] = source_df[source_field].apply(
                lambda x: self._convert_type(x, target_range)
            )
        
        return result


# ============================================================================
# STEP 1: EXTRACT FROM ERDDAP
# ============================================================================

class ERDDAPExtractor:
    """Extract data from ERDDAP datasets using direct CSV requests."""
    
    def __init__(self, server_url: str):
        self.server_url = server_url
    
    def fetch_dataset(self, dataset_id: str, constraints: Dict = None) -> pd.DataFrame:
        """
        Fetch a complete dataset from ERDDAP as CSV.
        
        Args:
            dataset_id: ERDDAP dataset identifier
            constraints: Optional dict of constraints (e.g., {'time>=': '2023-01-01'})
        
        Returns:
            DataFrame with the dataset
        """
        # Build the ERDDAP CSV URL
        url = f"{self.server_url}/tabledap/{dataset_id}.csv"
        
        # Add constraints if provided
        if constraints:
            constraint_str = '&'.join([f"{k}{v}" for k, v in constraints.items()])
            url += f"?{constraint_str}"
        
        # Fetch the CSV
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse CSV - ERDDAP CSV has 2 header rows (names, units), skip the units row
        df = pd.read_csv(StringIO(response.text), skiprows=[1])
        
        return df
    
    def fetch_all_ow1_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Fetch all three OW1 datasets.
        
        Returns:
            Tuple of (tow_df, catch_df, species_df)
        """
        print("Fetching data from ERDDAP...")
        
        print(f"  - Fetching tow records from {DATASET_IDS['tows']}...")
        tow_df = self.fetch_dataset(DATASET_IDS['tows'])
        print(f"    Got {len(tow_df)} tow records")
        print(f"  - Tow columns: {list(tow_df.columns)}")
        
        print(f"  - Fetching catch records from {DATASET_IDS['catch']}...")
        catch_df = self.fetch_dataset(DATASET_IDS['catch'])
        print(f"    Got {len(catch_df)} catch records")
        
        print(f"  - Fetching species codes from {DATASET_IDS['species']}...")
        species_df = self.fetch_dataset(DATASET_IDS['species'])
        print(f"    Got {len(species_df)} species entries")
        
        return tow_df, catch_df, species_df


# ============================================================================
# STEP 2: TRANSFORM TO DARWIN CORE
# ============================================================================

class DwCTransformer:
    """Transform OW1 data to Darwin Core format."""
    
    def __init__(self, mapping_engine: MappingEngine = None):
        """
        Initialize transformer with optional mapping engine.
        
        Args:
            mapping_engine: MappingEngine instance for auto-renaming fields
        """
        self.mapping_engine = mapping_engine
    
    @staticmethod
    def calculate_midpoint(start_lat: float, start_lon: float, 
                          end_lat: float, end_lon: float) -> Tuple[float, float]:
        """Calculate geographic midpoint."""
        return (start_lat + end_lat) / 2, (start_lon + end_lon) / 2
    
    @staticmethod
    def create_event_id(cruise: str, station: str) -> str:
        """Generate DwC eventID."""
        return f"{cruise}:{station}"
    
    @staticmethod
    def create_occurrence_id(cruise: str, station: str, species: str, size_class: str = None) -> str:
        """Generate DwC occurrenceID, including size_class if present."""
        species_code = str(species).replace(' ', '_').replace('/', '_')
        base_id = f"{cruise}:{station}:{species_code}"
        if size_class and pd.notna(size_class) and str(size_class).strip():
            size_code = str(size_class).replace(' ', '_').upper()
            return f"{base_id}:{size_code}"
        return base_id
    
    @staticmethod
    def format_itis_lsid(itis_tsn) -> str:
        """Format ITIS TSN as LSID."""
        if pd.isna(itis_tsn):
            return None
        return f"urn:lsid:itis.gov:itis_tsn:{int(itis_tsn)}"
    
    def transform_to_event(self, tow_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform TowRecords to DwC Event core.
        Creates TWO types of events: cruise-level parents and tow-level children.
        """
        events = []
        
        # 1. Create cruise-level parent events
        cruises = tow_df.groupby('cruise').agg({
            'time': 'min',  # Use earliest tow time for cruise
        }).reset_index()
        
        for _, cruise_row in cruises.iterrows():
            cruise_event = {
                'eventID': cruise_row['cruise'],
                'parentEventID': None,
                'eventType': 'cruise',
                'datasetID': 'bottom_trawl_survey_ow1_tows',
                'eventDate': cruise_row['time'],
                'locationID': None,
                'decimalLatitude': None,
                'decimalLongitude': None,
                'geodeticDatum': 'EPSG:4326',
                'footprintWKT': None,  # Could aggregate all tow lines if desired
                'footprintSRS': 'EPSG:4326',
                'coordinateUncertaintyInMeters': None,
                'minimumDepthInMeters': None,
                'maximumDepthInMeters': None,
                'waterBody': None,
                'samplingProtocol': 'Bottom otter trawl',
                'samplingEffort': None,
                'sampleSizeValue': None,
                'sampleSizeUnit': None,
                'eventRemarks': 'Survey cruise - parent event for multiple tows'
            }
            events.append(cruise_event)
        
        # 2. Create tow-level child events
        for _, row in tow_df.iterrows():
            # Calculate midpoint coordinates
            mid_lat, mid_lon = self.calculate_midpoint(
                row['latitude'], row['longitude'],
                row['end_latitude'], row['end_longitude']
            )
            
            # Create WKT LINESTRING for tow track
            footprint_wkt = f"LINESTRING ({row['longitude']} {row['latitude']}, {row['end_longitude']} {row['end_latitude']})"
            
            tow_event = {
                'eventID': self.create_event_id(row['cruise'], row['station']),
                'parentEventID': row['cruise'],
                'eventType': 'tow',
                'datasetID': 'bottom_trawl_survey_ow1_tows',
                'eventDate': row['time'],
                'locationID': row['station'],
                'decimalLatitude': round(mid_lat, 6),
                'decimalLongitude': round(mid_lon, 6),
                'geodeticDatum': 'EPSG:4326',
                'footprintWKT': footprint_wkt,
                'footprintSRS': 'EPSG:4326',
                'coordinateUncertaintyInMeters': None,
                'minimumDepthInMeters': row.get('depth_min'),
                'maximumDepthInMeters': row.get('depth_max'),
                'waterBody': None,
                'samplingProtocol': 'Bottom otter trawl',
                'samplingEffort': '20 minutes at 3 knots (~1 nautical mile)',
                'sampleSizeValue': 20,
                'sampleSizeUnit': 'minutes',
                'eventRemarks': f"Tow from {row['latitude']:.4f},{row['longitude']:.4f} to {row['end_latitude']:.4f},{row['end_longitude']:.4f}"
            }
            events.append(tow_event)
        
        return pd.DataFrame(events)
    
    def transform_to_occurrence(self, catch_df: pd.DataFrame, 
                                species_df: pd.DataFrame) -> pd.DataFrame:
        """Transform CatchRecords to DwC Occurrence extension."""
        
        # Join with species lookup on species_common_name
        merged = catch_df.merge(
            species_df,
            on='species_common_name',
            how='left'
        )
        
        # First, use mapping engine to auto-rename fields with exact mappings
        if self.mapping_engine:
            print("  - Auto-renaming Occurrence fields from LinkML mappings...")
            auto_renamed = self.mapping_engine.transform_dataframe(merged, "Occurrence")
            print(f"    Auto-renamed {len(auto_renamed.columns)} fields")
        else:
            auto_renamed = pd.DataFrame()
        
        # Then handle complex fields that require custom logic
        occurrences = []
        
        for _, row in merged.iterrows():
            # Build occurrence remarks with size class if present
            remarks = None
            if pd.notna(row.get('size_class')) and str(row['size_class']).strip():
                remarks = f"Size class: {row['size_class']}"
            
            occurrence = {
                'occurrenceID': self.create_occurrence_id(
                    row['cruise'], row['station'], row['species_common_name'], row.get('size_class')
                ),
                'eventID': self.create_event_id(row['cruise'], row['station']),
                'basisOfRecord': 'HumanObservation',
                'occurrenceStatus': 'present',
                'scientificNameID': self.format_itis_lsid(row.get('ITIS_tsn')),
                'taxonRank': 'species',
                'kingdom': 'Animalia',
                'individualCount': int(row['total_count']) if pd.notna(row['total_count']) else None,
                'organismQuantity': None,
                'organismQuantityType': None,
                'occurrenceRemarks': remarks
            }
            occurrences.append(occurrence)
        
        result_df = pd.DataFrame(occurrences)
        
        # Merge auto-renamed fields with custom fields
        # Auto-renamed fields that were manually handled should use manual version
        for col in auto_renamed.columns:
            if col not in result_df.columns:
                result_df[col] = auto_renamed[col]
        
        return result_df
    
    def transform_to_emof(self, catch_df: pd.DataFrame) -> pd.DataFrame:
        """Transform CatchRecords to DwC Extended Measurement or Fact (eMoF)."""
        emof_records = []
        
        for _, row in catch_df.iterrows():
            occurrence_id = self.create_occurrence_id(
                row['cruise'], row['station'], row['species_common_name'], row.get('size_class')
            )
            event_id = self.create_event_id(row['cruise'], row['station'])
            
            # Size class as a measurement (if present)
            if pd.notna(row.get('size_class')) and str(row['size_class']).strip():
                emof_records.append({
                    'measurementID': f"{occurrence_id}_size_class",
                    'occurrenceID': occurrence_id,
                    'eventID': event_id,
                    'measurementType': 'size class',
                    'measurementValue': str(row['size_class']),
                    'measurementUnit': None,
                    'measurementTypeID': None,  # TODO: add controlled vocab URI if available
                    'measurementValueID': None,
                    'measurementAccuracy': None,
                    'measurementDeterminedDate': None,
                    'measurementDeterminedBy': None,
                    'measurementMethod': 'Visual assessment during sorting',
                    'measurementRemarks': 'Categorical size class designation'
                })
            
            # Total weight
            if pd.notna(row.get('total_weight')):
                emof_records.append({
                    'measurementID': f"{occurrence_id}_weight",
                    'occurrenceID': occurrence_id,
                    'eventID': event_id,
                    'measurementType': 'total biomass',
                    'measurementValue': row['total_weight'],
                    'measurementUnit': 'kg',
                    'measurementTypeID': 'http://vocab.nerc.ac.uk/collection/P01/current/OWETXX01/',
                    'measurementValueID': None,
                    'measurementAccuracy': None,
                    'measurementDeterminedDate': None,
                    'measurementDeterminedBy': None,
                    'measurementMethod': 'Bottom otter trawl',
                    'measurementRemarks': None
                })
            
            # Total count
            if pd.notna(row.get('total_count')):
                emof_records.append({
                    'measurementID': f"{occurrence_id}_count",
                    'occurrenceID': occurrence_id,
                    'eventID': event_id,
                    'measurementType': 'abundance',
                    'measurementValue': int(row['total_count']),
                    'measurementUnit': 'individuals',
                    'measurementTypeID': 'http://vocab.nerc.ac.uk/collection/P01/current/OCOUNT01/',
                    'measurementValueID': None,
                    'measurementAccuracy': None,
                    'measurementDeterminedDate': None,
                    'measurementDeterminedBy': None,
                    'measurementMethod': 'Bottom otter trawl',
                    'measurementRemarks': None
                })
            
            # Mean length
            if pd.notna(row.get('mean_length')):
                length_type = row.get('length_type', 'TL')
                emof_records.append({
                    'measurementID': f"{occurrence_id}_mean_length",
                    'occurrenceID': occurrence_id,
                    'eventID': event_id,
                    'measurementType': f'mean {length_type} length',
                    'measurementValue': row['mean_length'],
                    'measurementUnit': 'mm',
                    'measurementTypeID': 'http://vocab.nerc.ac.uk/collection/P01/current/FL01XX01/',
                    'measurementValueID': None,
                    'measurementAccuracy': None,
                    'measurementDeterminedDate': None,
                    'measurementDeterminedBy': None,
                    'measurementMethod': 'Caliper measurement',
                    'measurementRemarks': f'Length type: {length_type}'
                })
            
            # Standard deviation of length
            if pd.notna(row.get('std_length')):
                length_type = row.get('length_type', 'TL')
                emof_records.append({
                    'measurementID': f"{occurrence_id}_std_length",
                    'occurrenceID': occurrence_id,
                    'eventID': event_id,
                    'measurementType': f'std dev {length_type} length',
                    'measurementValue': row['std_length'],
                    'measurementUnit': 'mm',
                    'measurementTypeID': 'http://vocab.nerc.ac.uk/collection/S06/current/S0600138/',
                    'measurementValueID': None,
                    'measurementAccuracy': None,
                    'measurementDeterminedDate': None,
                    'measurementDeterminedBy': None,
                    'measurementMethod': 'Caliper measurement',
                    'measurementRemarks': f'Length type: {length_type}'
                })
        
        df = pd.DataFrame(emof_records)
        
        # Reorder columns - coreid (occurrenceID) MUST be first
        column_order = [
            'eventID',
            'occurrenceID',
            'measurementID',
            'measurementType',
            'measurementValue',
            'measurementUnit',
            'measurementTypeID',
            'measurementValueID',
            'measurementAccuracy',
            'measurementDeterminedDate',
            'measurementDeterminedBy',
            'measurementMethod',
            'measurementRemarks'
        ]
        
        return df[column_order]


# ============================================================================
# EML METADATA GENERATION
# ============================================================================

class EMLGenerator:
    """Generate EML metadata from ERDDAP dataset metadata."""
    
    def __init__(self, erddap_server: str):
        self.erddap_server = erddap_server
    
    def fetch_metadata(self, dataset_id: str) -> Dict:
        """Fetch NC_GLOBAL metadata from ERDDAP info endpoint."""
        url = f"{self.erddap_server}/info/{dataset_id}/index.json"
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        metadata = {}
        
        # Extract NC_GLOBAL attributes
        for row in data['table']['rows']:
            if row[0] == 'attribute' and row[1] == 'NC_GLOBAL':
                attr_name = row[2]
                value = row[4]
                metadata[attr_name] = value
        
        return metadata
    
    def parse_contributors(self, names_str: str, roles_str: str) -> list:
        """Parse comma-separated contributor names and roles into list of dicts."""
        if not names_str or not roles_str:
            return []
        
        names = [n.strip() for n in names_str.split(',')]
        roles = [r.strip() for r in roles_str.split(',')]
        
        # Pair them up (assume equal length)
        contributors = []
        for i in range(min(len(names), len(roles))):
            contributors.append({
                'name': names[i],
                'role': roles[i]
            })
        return contributors
    
    def generate_eml_xml(self, metadata: Dict, package_id: str = None) -> str:
        """Generate EML XML from ERDDAP metadata."""
        
        # Use dataset id as package id if not provided
        pkg_id = package_id or metadata.get('id', 'unknown')
        
        # Parse contributors
        contributors = self.parse_contributors(
            metadata.get('contributor_name', ''),
            metadata.get('contributor_role', '')
        )
        
        # Parse keywords
        keywords = [kw.strip() for kw in metadata.get('keywords', '').split(',')]
        
        # Build EML XML
        eml = f"""<?xml version="1.0" encoding="UTF-8"?>
<eml:eml xmlns:eml="https://eml.ecoinformatics.org/eml-2.2.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xmlns:stmml="http://www.xml-cml.org/schema/stmml-1.2"
         xsi:schemaLocation="https://eml.ecoinformatics.org/eml-2.2.0 https://eml.ecoinformatics.org/eml-2.2.0/eml.xsd"
         packageId="{pkg_id}"
         system="{self.erddap_server}">
  
  <dataset>
    <title>{metadata.get('title', '')}</title>
    
    <creator>
      <organizationName>{metadata.get('creator_institution', '')}</organizationName>
      <individualName>
        <surName>{metadata.get('creator_name', '')}</surName>
      </individualName>
      <electronicMailAddress>{metadata.get('creator_email', '')}</electronicMailAddress>
      <onlineUrl>{metadata.get('creator_url', '')}</onlineUrl>
    </creator>
    
    <contact>
      <organizationName>{metadata.get('publisher_institution', '')}</organizationName>
      <individualName>
        <surName>{metadata.get('publisher_name', '')}</surName>
      </individualName>
      <electronicMailAddress>{metadata.get('publisher_email', '')}</electronicMailAddress>
    </contact>
    
    <publisher>
      <organizationName>{metadata.get('publisher_institution', '')}</organizationName>
      <individualName>
        <surName>{metadata.get('publisher_name', '')}</surName>
      </individualName>
      <electronicMailAddress>{metadata.get('publisher_email', '')}</electronicMailAddress>
    </publisher>
    
    <abstract>
      <para>{metadata.get('summary', '')}</para>
    </abstract>
    
    <keywordSet>"""
        
        for keyword in keywords:
            eml += f"\n      <keyword>{keyword}</keyword>"
        
        eml += f"""
    </keywordSet>
    
    <intellectualRights>
      <para>{metadata.get('license', '')}</para>
    </intellectualRights>
    
    <distribution>
      <online>
        <url>{metadata.get('infoUrl', '')}</url>
      </online>
    </distribution>
    
    <project>
      <title>{metadata.get('project', '')}</title>
      <funding>
        <para>{metadata.get('acknowledgement', '')}</para>
      </funding>"""
        
        if contributors:
            for contributor in contributors:
                eml += f"""
      <personnel>
        <individualName>
          <surName>{contributor['name']}</surName>
        </individualName>
        <role>{contributor['role']}</role>
      </personnel>"""
        
        eml += """
    </project>
    
    <methods>
      <methodStep>
        <description>
          <para>""" + metadata.get('comment', '') + """</para>
          <para>Platform: """ + metadata.get('platform_name', '') + """</para>
          <para>Program: """ + metadata.get('program', '') + """</para>
        </description>
      </methodStep>
    </methods>
  </dataset>
</eml:eml>"""
        
        return eml


# ============================================================================
# STEP 3: WRITE DARWIN CORE ARCHIVE
# ============================================================================

class DwCArchiveWriter:
    """Write Darwin Core Archive files."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def write_core_file(self, df: pd.DataFrame, filename: str):
        """Write a core or extension file as tab-delimited."""
        filepath = self.output_dir / filename
        df.to_csv(filepath, sep='\t', index=False, encoding='utf-8')
        print(f"  Wrote {filename} ({len(df)} records)")
    
    def write_eml(self, eml_xml: str):
        """Write EML metadata file."""
        eml_path = self.output_dir / "eml.xml"
        with open(eml_path, 'w', encoding='utf-8') as f:
            f.write(eml_xml)
        print(f"  Wrote eml.xml")
    
    def create_meta_xml(self):
        """Copy meta.xml template to output directory."""
        if not Path(META_XML_TEMPLATE).exists():
            raise FileNotFoundError(
                f"meta.xml template not found at '{META_XML_TEMPLATE}'. "
                f"Please ensure meta.xml exists in the working directory."
            )
        
        import shutil
        shutil.copy(META_XML_TEMPLATE, self.output_dir / "meta.xml")
        print(f"  Copied meta.xml from template")
        
    
    def create_zip_archive(self, archive_name: str = "ow1_dwca.zip"):
        """Create a zipped Darwin Core Archive."""
        archive_path = self.output_dir.parent / archive_name
        
        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in ['event.txt', 'occurrence.txt', 'extendedmeasurementorfact.txt', 'meta.xml', 'eml.xml']:
                zipf.write(self.output_dir / file, arcname=file)
        
        print(f"\nCreated Darwin Core Archive: {archive_path}")

        import shutil; shutil.rmtree(OUTPUT_DIR)
        return archive_path


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    """Execute the complete transformation pipeline."""
    
    print("=" * 80)
    print("OW1 BOTTOM TRAWL TO DARWIN CORE ARCHIVE PIPELINE")
    print("=" * 80)
    print()
    
    # Step 1: Extract from ERDDAP
    extractor = ERDDAPExtractor(ERDDAP_SERVER)
    tow_df, catch_df, species_df = extractor.fetch_all_ow1_data()
    print()
    
    # Step 2: Transform to Darwin Core
    print("Transforming to Darwin Core...")
    
    # Initialize mapping engine
    print(f"Loading LinkML mapping schema from {MAPPING_SCHEMA}...")
    mapping_engine = MappingEngine(MAPPING_SCHEMA)
    
    transformer = DwCTransformer(mapping_engine)
    
    events = transformer.transform_to_event(tow_df)
    print(f"  Created {len(events)} Event records")
    
    occurrences = transformer.transform_to_occurrence(catch_df, species_df)
    print(f"  Created {len(occurrences)} Occurrence records")
    
    emof = transformer.transform_to_emof(catch_df)
    print(f"  Created {len(emof)} extendedMeasurementOrFact records")
    print()
    
    # Step 3: Write Darwin Core Archive
    print("Writing Darwin Core Archive...")
    writer = DwCArchiveWriter(OUTPUT_DIR)
    writer.write_core_file(events, 'event.txt')
    writer.write_core_file(occurrences, 'occurrence.txt')
    writer.write_core_file(emof, 'extendedmeasurementorfact.txt')
    writer.create_meta_xml()
    
    # Generate and write EML
    print("\nGenerating EML metadata...")
    eml_generator = EMLGenerator(ERDDAP_SERVER)
    metadata = eml_generator.fetch_metadata(DATASET_IDS['catch'])
    eml_xml = eml_generator.generate_eml_xml(metadata)
    writer.write_eml(eml_xml)
    print()
    
    # Step 4: Create ZIP archive
    archive_path = writer.create_zip_archive()
    
    print()
    print("=" * 80)
    print("PIPELINE COMPLETE")
    print("=" * 80)
    print(f"Darwin Core Archive ready: {archive_path}")
    print()


if __name__ == "__main__":
    main()