# Pacific ETL Pipeline

## Overview
The Pacific ETL Pipeline automates data processing for the Pacific mVAM project. It's designed for handling survey data across multiple Pacific islands with high efficiency and standardization.

### Features
- **Modular Design**: Tailored modules for different islands and survey rounds.
- **Environment Switching**: Easily switch between development and production environments.
- **Comprehensive Processing**: Includes extraction, transformation (with cleaning and analysis), and loading stages.

### Components
1. **Extraction (`extract.py`)**: Retrieves survey data from APIs, converting JSON to CSV.
2. **Transformation (`transform.py`)**: Cleans and analyzes data using country and round-specific modules.
3. **Loading (`load.py`)**: Loads processed data into databases, with environment-specific configurations.
4. **Statistical Engine (`stat_engine.py`)**: Prepares data for statistical analysis and visualization.
5. **Supporting Modules**: Includes cleaning inputs, utility functions, and configuration management.

### Schematic
![alt text](https://github.com/aaronbwise/Pacific-ETL-pipeline/blob/main/ETL_overview.jpg)
