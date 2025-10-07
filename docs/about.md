# About This Project

## Overview

This project demonstrates how LinkML (Linked Data Modeling Language) can standardize fisheries survey data for publication to biodiversity repositories. It serves as a proof of concept for mobilizing marine survey data to Darwin Core Archives.

## Goals

1. **Demonstrate reusability**: Show how LinkML enables a generic transformation pattern applicable across different fisheries surveys
2. **Ensure traceability**: Document every field transformation explicitly through machine-readable mappings
3. **Support interoperability**: Make fisheries data discoverable alongside other marine biodiversity datasets
4. **Preserve knowledge**: Create maintainable documentation that survives personnel changes

## Project Context

### Ocean Wind 1 (OW1) Fisheries Monitoring

This work uses data from pre-construction fisheries surveys conducted for the Ocean Wind 1 offshore wind farm project. The surveys establish baseline conditions for fish and invertebrate communities within and around the proposed lease area.

**Survey design**:

- Two seasonal surveys per year (spring and fall)
- 20 tows in lease area (Impact), 20 in reference area (Control)
- Bottom otter trawl, 20 minutes at 3 knots
- Full species identification and measurement

**Data collection**:

- Rutgers University Marine Field Station
- Platform: R/V Petrel
- ERDDAP data server: https://rowlrs-data.marine.rutgers.edu/erddap

### Why Darwin Core?

Darwin Core is the international standard for biodiversity data, used by:

- **OBIS** (Ocean Biodiversity Information System)
- **GBIF** (Global Biodiversity Information Facility)
- Research institutions worldwide

Publishing fisheries data to Darwin Core:

- Increases data discoverability
- Enables large-scale synthesis
- Supports evidence-based management
- Facilitates data reuse

## Technical Approach

### LinkML as the Foundation

LinkML provides:

- **Machine-readable schemas**: Data models parseable by software
- **Semantic mappings**: Explicit relationships between source and target terms
- **Validation**: Type checking and constraint enforcement
- **Multiple outputs**: Generate documentation, code, and validation tools from one source

### Two-Stage Transformation

1. **Auto-rename**: Generic MappingEngine handles simple 1:1 field mappings
2. **Custom logic**: Domain-specific DwCTransformer handles complex transformations

This separation maximizes reusability while allowing flexibility.

## Repository Structure

```
IA_fisheries_trawl/
├── docs/                           # Documentation source (MkDocs)
│   ├── index.md
│   ├── workflow.md
│   ├── architecture/
│   ├── schemas/                    # Auto-generated from LinkML
│   └── ...
├── models/                         # LinkML schemas and datasets
│   ├── datasets/
│   │   └── rutgers/               # OW1 specific implementation
│   │       ├── ow1-catch-schema.yaml
│   │       ├── ow1-to-dwc-mappings.yaml
│   │       ├── ow1-to-eml-mappings.yaml
│   │       ├── transform.py
│   │       ├── meta.xml
│   │       └── ow1_dwca/          # Output files
│   └── example_ideas/             # Template schemas
├── scripts/                        # Utility scripts
│   └── generate_schema_docs.py    # Auto-generate docs
├── mkdocs.yml                      # Documentation config
└── .github/workflows/              # CI/CD automation
    └── deploy-docs.yml
```

## Contributing

This is a proof of concept. Feedback and suggestions are welcome!

### How to Provide Feedback

- **GitHub Issues**: https://github.com/sformel/IA_fisheries_trawl/issues