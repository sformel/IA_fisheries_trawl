#!/usr/bin/env python3
"""
Generate custom, readable schema documentation from LinkML YAML files.
Creates slot-focused docs with clean tables and better visual flow.
"""
import yaml
from pathlib import Path
from typing import Dict, List, Any
from collections import defaultdict

# Get the repository root
REPO_ROOT = Path(__file__).parent.parent
SCHEMA_DIR = REPO_ROOT / "models" / "datasets" / "rutgers"
DOCS_DIR = REPO_ROOT / "docs" / "schemas"
DOCS_DIR.mkdir(parents=True, exist_ok=True)

# Schema configurations
SCHEMAS = {
    'ow1-catch-schema.yaml': {
        'output': 'source-data.md',
        'title': 'Source Data Schema',
        'description': 'OW1 Bottom Trawl Survey source data structure',
        'type': 'source'
    },
    'ow1-to-dwc-mappings.yaml': {
        'output': 'dwc-mappings.md',
        'title': 'Darwin Core Mappings',
        'description': 'How OW1 source data maps to Darwin Core terms',
        'type': 'mappings'
    },
    'ow1-to-eml-mappings.yaml': {
        'output': 'eml-mappings.md',
        'title': 'EML Metadata Mappings',
        'description': 'How OW1 metadata maps to Ecological Metadata Language',
        'type': 'eml'
    }
}


class SchemaDocGenerator:
    """Generate readable documentation from LinkML schemas."""
    
    def __init__(self, schema_path: Path):
        self.schema_path = schema_path
        with open(schema_path, 'r') as f:
            self.schema = yaml.safe_load(f)
        
        self.classes = self.schema.get('classes', {})
        self.slots = self.schema.get('slots', {})
        self.enums = self.schema.get('enums', {})
        self.id = self.schema.get('id', '')
        self.name = self.schema.get('name', '')
        self.title = self.schema.get('title', '')
        self.description = self.schema.get('description', '')
    
    def generate_source_schema_doc(self) -> str:
        """Generate documentation for a source data schema."""
        
        doc = f"""# {self.title}

{self.description}

**Schema file**: `{self.schema_path.relative_to(REPO_ROOT)}`

---

## Data Fields (Slots)

"""
        
        # Group slots by class for better organization
        class_slots = self._group_slots_by_class()
        
        for class_name, slots_list in class_slots.items():
            if not slots_list:
                continue
            
            class_def = self.classes.get(class_name, {})
            class_desc = class_def.get('description', '')
            
            doc += f"### {class_name} Fields\n\n"
            if class_desc:
                doc += f"{class_desc}\n\n"
            
            doc += "| Field | Type | Units | Description | ERDDAP Source |\n"
            doc += "|-------|------|-------|-------------|---------------|\n"
            
            for slot_name in slots_list:
                slot_def = self.slots.get(slot_name, {})
                field_type = slot_def.get('range', 'string')
                description = slot_def.get('description', '').replace('\n', ' ')
                
                # Get units if available
                unit_info = slot_def.get('unit', {})
                units = unit_info.get('ucum_code', '-') if isinstance(unit_info, dict) else '-'
                
                # Get ERDDAP source from annotations
                annotations = slot_def.get('annotations', {})
                erddap_source = annotations.get('erddap_source', slot_name) if isinstance(annotations, dict) else slot_name
                
                doc += f"| **{slot_name}** | {field_type} | {units} | {description} | `{erddap_source}` |\n"
            
            doc += "\n"
        
        # Add enumerations section
        if self.enums:
            doc += "---\n\n## Enumerations\n\n"
            for enum_name, enum_def in self.enums.items():
                enum_desc = enum_def.get('description', '')
                doc += f"### {enum_name}\n\n"
                if enum_desc:
                    doc += f"{enum_desc}\n\n"
                
                doc += "| Code | Meaning | Description |\n"
                doc += "|------|---------|-------------|\n"
                
                for value_name, value_def in enum_def.get('permissible_values', {}).items():
                    value_desc = value_def.get('description', '') if isinstance(value_def, dict) else ''
                    doc += f"| **{value_name}** | {value_name} | {value_desc} |\n"
                
                doc += "\n"
        
        # Add classes overview
        doc += "---\n\n## Data Classes\n\n"
        doc += "The schema organizes fields into classes:\n\n"
        
        for class_name, class_def in self.classes.items():
            class_desc = class_def.get('description', '')
            class_slots = class_def.get('slots', [])
            
            doc += f"### {class_name}\n"
            if class_desc:
                doc += f"{class_desc}\n\n"
            
            if class_slots:
                key_fields = ', '.join(class_slots[:5])
                doc += f"**Key fields**: {key_fields}\n\n"
        
        return doc
    
    def generate_mappings_doc(self) -> str:
        """Generate documentation for Darwin Core mappings."""
        
        doc = f"""# {self.title}

{self.description}

**Schema file**: `{self.schema_path.relative_to(REPO_ROOT)}`

---

## Mapping Overview

```mermaid
flowchart LR
    A[Source Fields] -->|exact_mappings| B[Target Terms]
    A -->|related_mappings| C[Custom Transform]
    C --> B
    
    style A fill:#e1f5ff
    style B fill:#d4edda
    style C fill:#fff4e1
```

**Mapping types**:

- **exact_mappings**: 1:1 field renames (auto-transformed)
- **related_mappings**: Complex transformations requiring custom logic
- **close_mappings**: Conceptually similar fields

---

"""
        
        # Group slots by class
        class_slots = self._group_slots_by_class()
        
        for class_name, slots_list in class_slots.items():
            if not slots_list:
                continue
            
            class_def = self.classes.get(class_name, {})
            class_desc = class_def.get('description', '')
            
            doc += f"## {class_name} Mappings\n\n"
            if class_desc:
                doc += f"{class_desc}\n\n"
            
            # Separate into auto-mapped and custom-mapped
            auto_mapped = []
            custom_mapped = []
            
            for slot_name in slots_list:
                slot_def = self.slots.get(slot_name, {})
                exact_mappings = slot_def.get('exact_mappings', [])
                related_mappings = slot_def.get('related_mappings', [])
                
                if exact_mappings and len(exact_mappings) == 1:
                    auto_mapped.append(slot_name)
                elif exact_mappings or related_mappings:
                    custom_mapped.append(slot_name)
            
            # Auto-mapped fields table
            if auto_mapped:
                doc += "### Auto-Mapped Fields (1:1)\n\n"
                doc += "| Target Term | Source Field | Transformation |\n"
                doc += "|-------------|--------------|----------------|\n"
                
                for slot_name in auto_mapped:
                    slot_def = self.slots.get(slot_name, {})
                    exact_mappings = slot_def.get('exact_mappings', [])
                    source = self._extract_field_name(exact_mappings[0]) if exact_mappings else ''
                    doc += f"| **{slot_name}** | `{source}` | Direct copy |\n"
                
                doc += "\n"
            
            # Custom-mapped fields table
            if custom_mapped:
                doc += "### Custom-Mapped Fields\n\n"
                doc += "| Target Term | Source Fields | Transformation |\n"
                doc += "|-------------|---------------|----------------|\n"
                
                for slot_name in custom_mapped:
                    slot_def = self.slots.get(slot_name, {})
                    description = slot_def.get('description', '')
                    
                    # Get all mapping sources
                    exact_mappings = slot_def.get('exact_mappings', [])
                    related_mappings = slot_def.get('related_mappings', [])
                    close_mappings = slot_def.get('close_mappings', [])
                    
                    all_mappings = exact_mappings + related_mappings + close_mappings
                    sources = [self._extract_field_name(m) for m in all_mappings]
                    source_str = ', '.join(f"`{s}`" for s in sources) if sources else '-'
                    
                    # Get comments for transformation notes
                    comments = slot_def.get('comments', [])
                    transform_note = comments[0] if comments else description[:50]
                    
                    doc += f"| **{slot_name}** | {source_str} | {transform_note} |\n"
                
                doc += "\n"
            
            doc += "---\n\n"
        
        return doc
    
    def generate_eml_doc(self) -> str:
        """Generate documentation for EML mappings."""
        
        doc = f"""# {self.title}

{self.description}

**Schema file**: `{self.schema_path.relative_to(REPO_ROOT)}`

---

## Purpose

EML provides standardized dataset-level metadata for the Darwin Core Archive.

---

## Metadata Mappings

"""
        
        # Group slots by class
        class_slots = self._group_slots_by_class()
        
        for class_name, slots_list in class_slots.items():
            if not slots_list or class_name in ['EMLDocument', 'Dataset']:
                continue
            
            class_def = self.classes.get(class_name, {})
            class_desc = class_def.get('description', '')
            
            doc += f"### {class_name}\n\n"
            if class_desc:
                doc += f"{class_desc}\n\n"
            
            doc += "| EML Element | Source Field | Notes |\n"
            doc += "|-------------|--------------|-------|\n"
            
            for slot_name in slots_list:
                slot_def = self.slots.get(slot_name, {})
                exact_mappings = slot_def.get('exact_mappings', [])
                
                sources = [self._extract_field_name(m) for m in exact_mappings]
                source_str = ', '.join(f"`{s}`" for s in sources) if sources else 'Generated'
                
                comments = slot_def.get('comments', [])
                note = comments[0] if comments else ''
                
                doc += f"| **{slot_name}** | {source_str} | {note} |\n"
            
            doc += "\n"
        
        return doc
    
    def _group_slots_by_class(self) -> Dict[str, List[str]]:
        """Group slots by their parent class."""
        class_slots = defaultdict(list)
        
        for class_name, class_def in self.classes.items():
            slots_list = class_def.get('slots', [])
            class_slots[class_name] = slots_list
        
        return dict(class_slots)
    
    def _extract_field_name(self, mapping: str) -> str:
        """Extract field name from mapping string (e.g., 'ow1_catch:time' -> 'time')."""
        if ':' in mapping:
            return mapping.split(':', 1)[1]
        return mapping


def generate_docs():
    """Generate documentation for all schemas."""
    
    print("Generating custom schema documentation...")
    print("=" * 60)
    print(f"Schema directory: {SCHEMA_DIR}")
    print(f"Output directory: {DOCS_DIR}")
    print("=" * 60)
    
    for schema_file, config in SCHEMAS.items():
        schema_path = SCHEMA_DIR / schema_file
        output_path = DOCS_DIR / config['output']
        
        if not schema_path.exists():
            print(f"\n⚠️  Schema not found: {schema_path}")
            continue
        
        print(f"\n📄 Processing {schema_file}...")
        print(f"   Output: {output_path}")
        
        try:
            generator = SchemaDocGenerator(schema_path)
            
            # Generate appropriate doc based on schema type
            if config['type'] == 'source':
                content = generator.generate_source_schema_doc()
            elif config['type'] == 'mappings':
                content = generator.generate_mappings_doc()
            elif config['type'] == 'eml':
                content = generator.generate_eml_doc()
            else:
                print(f"   ⚠️  Unknown schema type: {config['type']}")
                continue
            
            # Write the documentation
            output_path.write_text(content)
            
            print(f"   ✅ Generated successfully ({len(content):,} characters)")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("✅ Schema documentation generation complete!")
    print(f"📁 Documentation written to: {DOCS_DIR}")


if __name__ == '__main__':
    generate_docs()