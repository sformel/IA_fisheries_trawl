#!/usr/bin/env python3
"""
Generate schema documentation from LinkML YAML files using gen-doc.
"""
import subprocess
from pathlib import Path

# Paths
SCHEMA_DIR = Path(".")
DOCS_DIR = Path("docs/schemas")
DOCS_DIR.mkdir(parents=True, exist_ok=True)

# Schema files to process
SCHEMAS = {
    'ow1-catch-schema.yaml': {
        'output': 'source-data.md',
        'title': 'Source Data Schema'
    },
    'ow1-to-dwc-mappings.yaml': {
        'output': 'dwc-mappings.md',
        'title': 'Darwin Core Mappings'
    },
    'ow1-to-eml-mappings.yaml': {
        'output': 'eml-mappings.md',
        'title': 'EML Mappings'
    }
}

def generate_docs():
    """Generate markdown documentation for each schema using gen-doc."""
    
    print("Generating LinkML schema documentation...")
    print("=" * 60)
    
    for schema_file, config in SCHEMAS.items():
        schema_path = SCHEMA_DIR / schema_file
        output_path = DOCS_DIR / config['output']
        
        if not schema_path.exists():
            print(f"⚠️  Schema not found: {schema_path}")
            continue
        
        print(f"\n📄 Processing {schema_file}...")
        print(f"   Output: {output_path}")
        
        # Run gen-doc command
        cmd = [
            'gen-doc',
            str(schema_path),
            '-d', str(DOCS_DIR),
            '--format', 'markdown'
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # gen-doc creates index.md by default, rename to our target
            generated_file = DOCS_DIR / 'index.md'
            if generated_file.exists():
                generated_file.rename(output_path)
            
            # Add frontmatter for MkDocs
            add_frontmatter(output_path, config['title'])
            
            print(f"   ✅ Generated successfully")
            
        except subprocess.CalledProcessError as e:
            print(f"   ❌ Error generating docs:")
            print(f"   {e.stderr}")
        except FileNotFoundError:
            print(f"   ❌ gen-doc not found. Install with: pip install linkml")
    
    print("\n" + "=" * 60)
    print("✅ Schema documentation generation complete!")
    print(f"📁 Documentation written to: {DOCS_DIR}")

def add_frontmatter(file_path: Path, title: str):
    """Add MkDocs frontmatter to generated markdown file."""
    
    if not file_path.exists():
        return
    
    content = file_path.read_text()
    
    # Add explanatory header before auto-generated content
    frontmatter = f"""# {title}

!!! info "Auto-Generated Documentation"
    This page is automatically generated from LinkML schema files using `gen-doc`.
    
    **Source file**: `{file_path.stem.replace('-mappings', '').replace('-', '_')}.yaml`
    
    To update this documentation, modify the source YAML file and run:
    ```bash
    python scripts/generate_schema_docs.py
    ```

---

{content}
"""
    
    file_path.write_text(frontmatter)

if __name__ == '__main__':
    generate_docs()