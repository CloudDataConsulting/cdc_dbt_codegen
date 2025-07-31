# CDC DBT Codegen Enhancement To-Do List

## Overview
This document outlines the enhancement opportunities for the CDC DBT Codegen package based on a comprehensive analysis of the codebase. The package automates the generation of staging layer SQL and YAML files for dbt projects following Cloud Data Consulting's patterns.

## Current State Summary
- **Purpose**: Automates staging layer code generation from Snowflake metadata
- **Core Features**: 
  - Generates staging SQL with standardized transformations (trimming, renaming)
  - Creates YAML files with documentation and tests
  - Detects foreign keys for relationship tests
- **Main Issues**: Incomplete CLI, redundant code, outdated dependencies, missing modern features

## Enhancement To-Do List

### 1. Fix and Modernize the CLI ⚡ Priority: High
- [ ] Complete the implementation of `code_gen.py`
- [ ] Fix argument parsing and command structure
- [ ] Add clear command hierarchy:
  ```bash
  codegen stage --source <name> [--table <name>]
  codegen dimensional --schema <name> [--table <name>]
  codegen list-sources
  ```
- [ ] Add `--help` documentation for each command
- [ ] Add progress indicators and better output formatting

### 2. Consolidate Redundant Code 🔧 Priority: High
- [ ] Merge `gen_stage_files.sql` and `gen_stg_src_name_yml.sql` (they generate identical output)
- [ ] Create a single Python module for shared functionality:
  - [ ] Connection management
  - [ ] Configuration parsing
  - [ ] File writing utilities
- [ ] Remove or consolidate overlapping Python scripts
- [ ] Create a proper package structure:
  ```
  cdc_dbt_codegen/
  ├── __init__.py
  ├── cli.py
  ├── core/
  │   ├── __init__.py
  │   ├── connection.py
  │   ├── config.py
  │   └── generators.py
  └── templates/
  ```

### 3. Modernize Configuration and Authentication 🔐 Priority: High
- [ ] Remove hardcoded 'dev' environment
- [ ] Support dbt profile resolution (use `dbt.config.Profile`)
- [ ] Add support for modern authentication:
  - [ ] OAuth/SSO
  - [ ] Key-pair authentication
  - [ ] Environment variables
  - [ ] 1Password integration (as noted in CLAUDE.md)
- [ ] Replace deprecated `confuse` library with modern config management
- [ ] Add `.env` file support for local development

### 4. Add Safety and Quality Features 🛡️ Priority: Medium
- [ ] Add `--dry-run` mode to preview changes without writing files
- [ ] Add `--backup` option to preserve existing files
- [ ] Implement file diff comparison before overwriting
- [ ] Add validation for configuration files
- [ ] Add pre-generation checks:
  - [ ] Verify source database/schema exists
  - [ ] Check user permissions
  - [ ] Validate naming conventions
- [ ] Add post-generation validation:
  - [ ] Verify generated SQL is valid
  - [ ] Check YAML syntax

### 5. Enhance Generation Capabilities 🚀 Priority: Medium
- [ ] Support incremental generation (only new/changed tables)
- [ ] Add custom transformation templates
- [ ] Support for different staging patterns:
  - [ ] Type 2 SCD
  - [ ] Snapshot tables
  - [ ] Append-only patterns
- [ ] Add column-level customization:
  - [ ] Custom data type mappings
  - [ ] Transformation rules
  - [ ] Test specifications
- [ ] Support for multiple source systems in one run
- [ ] Generate documentation markdown files

### 6. Improve Testing Capabilities 🧪 Priority: Medium
- [ ] Expand beyond basic unique/not_null tests
- [ ] Add data quality tests:
  - [ ] Accepted values
  - [ ] Relationships
  - [ ] Expression tests
  - [ ] Custom SQL tests
- [ ] Generate test documentation
- [ ] Add test templates for common patterns

### 7. Add Modern dbt Features 📦 Priority: Low
- [ ] Support dbt 1.5+ features:
  - [ ] Python models
  - [ ] Semantic layer
  - [ ] Model contracts
- [ ] Generate macro files for reusable logic
- [ ] Support for dbt metrics
- [ ] Integration with dbt Cloud API

### 8. Improve Documentation 📚 Priority: High
- [ ] Create comprehensive README with:
  - [ ] Installation instructions
  - [ ] Configuration guide
  - [ ] Usage examples
  - [ ] Troubleshooting section
- [ ] Add inline code documentation
- [ ] Create example project structure
- [ ] Add architecture diagrams
- [ ] Document CDC naming conventions and patterns

### 9. Package and Distribution 📤 Priority: Low
- [ ] Set up proper Python packaging (setup.py/pyproject.toml)
- [ ] Create GitHub Actions for:
  - [ ] Testing
  - [ ] Linting
  - [ ] Release automation
- [ ] Publish to PyPI
- [ ] Create Docker image for containerized usage
- [ ] Add version management

### 10. Developer Experience Improvements 💻 Priority: Medium
- [ ] Add logging with configurable levels
- [ ] Improve error messages with actionable solutions
- [ ] Add interactive mode for configuration
- [ ] Create VS Code extension for integration
- [ ] Add shell completion support
- [ ] Create web UI for configuration management

## Quick Wins (Can be done immediately)
1. Fix the typo in readme.md: "multi scheams" → "multi schemas"
2. Add `.gitignore` entries for Python virtual environments
3. Update copyright year in license
4. Remove commented-out code in Python files
5. Add type hints to Python functions
6. Fix the `gen_non_stg_yml.sql` bug where it uses `target.user` instead of `target.schema`

## Implementation Phases

### Phase 1: Foundation (1-2 weeks)
- Fix CLI implementation
- Consolidate redundant code
- Modernize configuration
- Update documentation

### Phase 2: Safety and Quality (1 week)
- Add dry-run and backup features
- Implement validation
- Improve error handling

### Phase 3: Enhanced Features (2-3 weeks)
- Add incremental generation
- Expand testing capabilities
- Support custom templates

### Phase 4: Modernization (2-3 weeks)
- Add modern dbt features
- Improve developer experience
- Package for distribution

## Success Metrics
- Reduced code generation time by 50%
- Zero file overwrites without user confirmation
- Support for 100% of dbt test types
- Full compatibility with dbt 1.5+
- Comprehensive documentation coverage

## Notes
- All enhancements should maintain backward compatibility where possible
- Follow CDC's established patterns and naming conventions
- Prioritize user safety (no accidental overwrites)
- Focus on developer experience and clear error messages