# Cleanup Analysis for CDC DBT Codegen

## Files to Remove (Obsolete/Deprecated)

### 1. Legacy Python Scripts
These are superseded by either `code_gen.py` CLI or `generate_staging_modern.py`:

- **py/generate_dbt_files_by_source.py** 
  - Has comment: "Bernie thinks this is obsolete and can be safely deleted"
  - Functionality replaced by `generate_staging_modern.py` with better auth support
  - Uses old confuse library and hardcoded values
  
- **py/generate_non_stg_yml_files.py**
  - Functionality integrated into new `code_gen.py` CLI as `dimensional` command
  - Still has hardcoded 'dev' references
  
- **py/generate_stg_yml_files.py**
  - Functionality integrated into new `code_gen.py` CLI as `stage` command
  - Redundant with modern alternatives

### 2. Duplicate/Similar SQL Models
After review, these are NOT duplicates but serve different purposes:
- `gen_stage_files.sql` - Generates source YAML configuration 
- `gen_stg_src_name_yml.sql` - Generates source-specific YAML (different template)
- Both are needed for different parts of the generation process

## Files to Keep

### Modern Python Scripts
- **py/code_gen.py** - New unified CLI (keep and continue improving)
- **py/generate_staging_modern.py** - Modern script with key pair auth support
- **cdc_dbt_codegen/** - New package structure with core/ and utils/

### SQL Models
All SQL models in models/dw_util/ should be kept as they serve distinct purposes

## Recommended Actions

1. **Remove obsolete Python scripts** that are commented as obsolete or superseded
2. **Update README** to clearly indicate which tools to use
3. **Add deprecation notices** to CHANGELOG for removed scripts
4. **Clean up any test artifacts** that may have been left behind

## Migration Path for Users

Users currently using the old scripts should migrate to:
- `python py/generate_staging_modern.py` for staging generation with auth support
- `codegen` CLI for unified interface to all generation tasks