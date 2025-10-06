"""
OW1 Bottom Trawl to Darwin Core Archive - Complete Pipeline
Extracts from ERDDAP → Transforms → Validates → Writes DwC-A
"""

import pandas as pd
from pathlib import Path
import zipfile
from typing import Dict, Tuple
import requests
from io import StringIO


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
                'vernacularName': row['species_common_name'],
                'scientificName': row.get('species_scientific_name'),
                'scientificNameID': self.format_itis_lsid(row.get('ITIS_tsn')),
                'taxonRank': 'species',
                'kingdom': 'Animalia',
                'individualCount': int(row['total_count']) if pd.notna(row['total_count']) else None,
                'organismQuantity': None,
                'organismQuantityType': None,
                'occurrenceRemarks': remarks
            }
            occurrences.append(occurrence)
        
        return pd.DataFrame(occurrences)
    
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
    
    def create_meta_xml(self):
        """Create meta.xml descriptor for the archive."""
        meta_xml = """<?xml version="1.0" encoding="UTF-8"?>
<archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
  <core encoding="UTF-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n" 
        ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Event">
    <files>
      <location>event.txt</location>
    </files>
    <id index="0"/>
    <field index="0" term="http://rs.tdwg.org/dwc/terms/eventID"/>
    <field index="1" term="http://rs.tdwg.org/dwc/terms/parentEventID"/>
    <field index="2" term="http://rs.tdwg.org/dwc/terms/eventDate"/>
    <field index="3" term="http://rs.tdwg.org/dwc/terms/decimalLatitude"/>
    <field index="4" term="http://rs.tdwg.org/dwc/terms/decimalLongitude"/>
    <field index="5" term="http://rs.tdwg.org/dwc/terms/geodeticDatum"/>
    <field index="6" term="http://rs.tdwg.org/dwc/terms/coordinateUncertaintyInMeters"/>
    <field index="7" term="http://rs.tdwg.org/dwc/terms/minimumDepthInMeters"/>
    <field index="8" term="http://rs.tdwg.org/dwc/terms/maximumDepthInMeters"/>
    <field index="9" term="http://rs.tdwg.org/dwc/terms/samplingProtocol"/>
    <field index="10" term="http://rs.tdwg.org/dwc/terms/sampleSizeValue"/>
    <field index="11" term="http://rs.tdwg.org/dwc/terms/sampleSizeUnit"/>
    <field index="12" term="http://rs.tdwg.org/dwc/terms/eventRemarks"/>
    <field index="13" term="http://rs.tdwg.org/dwc/terms/eventType"/>
    <field index="14" term="http://rs.tdwg.org/dwc/terms/datasetID"/>
    <field index="15" term="http://rs.tdwg.org/dwc/terms/locationID"/>
    <field index="16" term="http://rs.tdwg.org/dwc/terms/waterBody"/>
    <field index="17" term="http://rs.tdwg.org/dwc/terms/footprintWKT"/>
    <field index="18" term="http://rs.tdwg.org/dwc/terms/footprintSRS"/>
    <field index="19" term="http://rs.tdwg.org/dwc/terms/samplingEffort"/>
  </core>
  
  <extension encoding="UTF-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n" 
            ignoreHeaderLines="1" 
            rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
    <files>
      <location>occurrence.txt</location>
    </files>
    <coreid index="0"/>
    <field index="0" term="http://rs.tdwg.org/dwc/terms/eventID"/>
    <field index="1" term="http://rs.tdwg.org/dwc/terms/occurrenceID"/>
    <field index="2" term="http://rs.tdwg.org/dwc/terms/basisOfRecord"/>
    <field index="3" term="http://rs.tdwg.org/dwc/terms/occurrenceStatus"/>
    <field index="4" term="http://rs.tdwg.org/dwc/terms/vernacularName"/>
    <field index="5" term="http://rs.tdwg.org/dwc/terms/scientificName"/>
    <field index="6" term="http://rs.tdwg.org/dwc/terms/scientificNameID"/>
    <field index="7" term="http://rs.tdwg.org/dwc/terms/taxonRank"/>
    <field index="8" term="http://rs.tdwg.org/dwc/terms/kingdom"/>
    <field index="9" term="http://rs.tdwg.org/dwc/terms/individualCount"/>
    <field index="10" term="http://rs.tdwg.org/dwc/terms/organismQuantity"/>
    <field index="11" term="http://rs.tdwg.org/dwc/terms/organismQuantityType"/>
    <field index="12" term="http://rs.tdwg.org/dwc/terms/occurrenceRemarks"/>
  </extension>
  
  <extension encoding="UTF-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n" 
            ignoreHeaderLines="1" 
            rowType="http://rs.iobis.org/obis/terms/ExtendedMeasurementOrFact">
    <files>
        <location>extendedmeasurementorfact.txt</location>
    </files>
    <coreid index="0"/>
    <field index="0" term="http://rs.tdwg.org/dwc/terms/eventID"/>
    <field index="1" term="http://rs.tdwg.org/dwc/terms/occurrenceID"/>
    <field index="2" term="http://rs.tdwg.org/dwc/terms/measurementID"/>
    <field index="3" term="http://rs.tdwg.org/dwc/terms/measurementType"/>
    <field index="4" term="http://rs.tdwg.org/dwc/terms/measurementValue"/>
    <field index="5" term="http://rs.tdwg.org/dwc/terms/measurementUnit"/>
    <field index="6" term="http://rs.tdwg.org/dwc/terms/measurementTypeID"/>
    <field index="7" term="http://rs.tdwg.org/dwc/terms/measurementValueID"/>
    <field index="8" term="http://rs.tdwg.org/dwc/terms/measurementAccuracy"/>
    <field index="9" term="http://rs.tdwg.org/dwc/terms/measurementDeterminedDate"/>
    <field index="10" term="http://rs.tdwg.org/dwc/terms/measurementDeterminedBy"/>
    <field index="11" term="http://rs.tdwg.org/dwc/terms/measurementMethod"/>
    <field index="12" term="http://rs.tdwg.org/dwc/terms/measurementRemarks"/>
   </extension>
</archive>"""
        
        meta_path = self.output_dir / "meta.xml"
        with open(meta_path, 'w', encoding='utf-8') as f:
            f.write(meta_xml)
        print(f"  Wrote meta.xml")
    
    def create_zip_archive(self, archive_name: str = "ow1_dwca.zip"):
        """Create a zipped Darwin Core Archive."""
        archive_path = self.output_dir.parent / archive_name
        
        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in ['event.txt', 'occurrence.txt', 'extendedmeasurementorfact.txt', 'meta.xml']:
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
    transformer = DwCTransformer()
    
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