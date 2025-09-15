# CDS Refactoring Project

A data processing pipeline for handling classified defense and intelligence objects with proper security markings and standardization.

## Overview

This project processes raw source objects from an object management system and transforms them into standardized format while maintaining proper classification markings and security controls. The system handles various object types including facilities, equipment, targets, and locations.

## Key Features

- **Classification Handling**: Processes objects with various security classifications (UNCLASSIFIED, SECRET, etc.)
- **ISM (Information Security Marking)**: Maintains proper security banners and dissemination controls
- **Object Standardization**: Converts source objects to standardized schema format
- **Multi-Domain Support**: Handles AIR, SEA, GROUND and other domain objects
- **Validation**: Ensures all processed objects conform to the standard schema

## Project Structure


```
├── configs/
│   ├── attribute_mapping.yaml                # Maps source attributes to standard schema
│   ├── classifications_config.yaml           # Classification level configurations
│   ├── valid_attributes_6_19_25.yaml        # Valid attribute definitions
│   └── schemas/
│       └── standard_object_schema_v1.4.json  # JSON schema for standardized objects
├── data/
│   ├── 1_raw/
│   │   ├── input/                            # Individual raw source data files (UUID-named)
│   │   └── objects/
│   │       ├── facility_objects.json        # Facility-specific objects
│   │       ├── maritime_objects.json        # Maritime/sea domain objects
│   │       └── source_objects.json          # Combined source objects in a single JSON
│   └── 2_processed/
│       └── output/                           # Processed objects with timestamps
├── notebooks/                                # Jupyter notebooks for development/analysis
│   ├── oms_pipeline_flow.ipynb              # Cell-by-Cell pipeline flow
│   ├── oms_pipeline_main.ipynb              # Main pipeline flow
└── src/                                      # Source code modules
    ├── __init__.py
    ├── extract/
    │   ├── __init__.py
    │   └── object_extractor.py               # Extracts objects from raw data
    ├── load/
    │   ├── __init__.py
    │   └── configs_loader.py                 # Loads configuration files
    ├── pipelines/
    │   ├── __init__.py
    │   └── oms_data_pipeline.py              # Main OMS data processing pipeline
    ├── transform/
    │   ├── __init__.py
    │   ├── classif_restrictor.py             # Handles classification restrictions
    │   ├── object_parser.py                  # Parses and transforms objects
    │   └── preprocessor.py                   # Preprocesses raw data
    └── utils/
        ├── __init__.py
        └── validater.py                      # Validation utilities
```

## Data Flow

1. **Input**: Raw source objects with ACM (Access Control Metadata) classifications
2. **Processing**: 
   - Extract and standardize ISM markings
   - Parse location data
   - Transform object attributes to standard format
   - Validate against schema
3. **Output**: Standardized objects with proper security markings

## Security Classifications

The system handles multiple classification levels with strict hierarchical controls:

### Classification Levels (Hierarchical)
- `U` (0) - UNCLASSIFIED
- `C` (1) - CONFIDENTIAL  
- `S` (2) - SECRET
- `TS` (3) - TOP SECRET

### Dissemination Controls
- Standard dissemination controls (REL TO USA, AUS, CAN, GBR, NZL)
- Special access groups: FVEY, NATO, ACGU
- Banner markings for proper document handling

### Security Restrictions
The system enforces strict controls on:

**Forbidden SCI (Sensitive Compartmented Information):**
- SI
- TK
- G
- HCS

**Forbidden Dissemination Controls:**
- IMCON
- RSEN

**Blocked Terms:**
- Various TOP SECRET markings and SCI caveats are automatically filtered
- Ensures compliance with security policies and prevents unauthorized access

### Classification Processing
The `classif_restrictor.py` module enforces these restrictions during processing, ensuring that:
- Objects maintain appropriate classification levels
- Forbidden SCI markings are identified and handled
- Dissemination controls comply with policy
- Classification hierarchies are respected

## Object Types

Supports multiple object categories:
- **Facilities**: Buildings, command centers, installations
- **Equipment**: Military hardware, vehicles, weapons systems
- **Targets**: Objects of intelligence interest
- **Locations**: Geographic positions and areas
- **Air/Sea/Ground/Space**: Domain-specific metadata

## Getting Started

### Prerequisites

- Python 3.13+
- Jupyter Notebook environment

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd CDS-Refact

# Install dependencies
pip install -e .
```

### Dependencies

- `loguru` - Logging
- `rich` - Rich text and pretty printing
- `pyyaml` - YAML processing
- `jsonschema` - Schema validation
- `notebook` - Jupyter notebook support
- `pyarrow` - For data processing and Databricks connection
- `python-dotenv` - Environment variable management
- `databricks-sql-connector` - Databricks SQL connection

### Running the Pipeline

1. **Start with the main pipeline notebook**:
   ```
    notebooks/oms_pipeline_flow.ipynb
   ```

2. **Process source objects**:
   - Load raw objects from [`data/1_raw/input`](data/1_raw/input)
   - Run transformation and validation
   - Output standardized objects

3. **Run end-2-end processing**:
   - Use [`oms_pipeline_main.ipynb`](notebooks/oms_pipeline_main.ipynb) to run end-to-end processing

## Schema

The project uses [`standard_object_schema_v1.4.json`](configs/schemas/standard_object_schema_v1.4.json) which defines:

- Object structure and required fields
- ISM (Information Security Marking) format
- Data types and validation rules
- Domain-specific metadata schemas

## Development

Key functions for developers:

- `process_objects()` - Main function to process raw objects through the pipeline
- `extract_ism(acm)` - Converts ACM to ISM format
- `parse_location()` - Extracts location data
- `fetch_all_objects()` - Loads source objects
- Schema validation against the standard object schema

## Security Notes

⚠️ **Important**: This system processes classified information. Ensure proper:
- Secure development environment
- Proper handling of classification markings
- Compliance with information security policies

## Contributing

When contributing:
1. Maintain classification marking integrity
2. Follow the established schema structure
3. Add appropriate validation for new features
4. Update documentation for any schema changes

## License

[Include appropriate license information for your organization]