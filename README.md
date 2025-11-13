# mb-matric-test

A geospatial analysis tool for determining property orientation (facing direction) by analyzing the relationship between property locations and nearby road geometry.

## Overview

This project analyzes property data (GNAF) and road networks to determine which direction each property faces (North, South, East, West, etc.) by:
1. Loading property point data and road line geometries
2. Finding the nearest road segment for each property
3. Calculating the road bearing and determining the perpendicular house facing direction
4. Generating a CSV report with property addresses, PIDs, and orientations

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Data Requirements

Place the following files in the `data/` directory:
- `gnaf_prop.parquet` - Property data with Latitude, Longitude, Address, and PID columns
- `roads.gpkg` - Road network line geometries

## Running the Application

### Option 1: Run the Facing Analyzer directly

```bash
python house_faces.py
```

This will run the analysis pipeline and display a sample of the generated CSV report.

### Option 2: Run the Flask web application

```bash
python app.py
```

Then open your browser and navigate to `http://localhost:5000`. The application will:
- Run the orientation analysis pipeline
- Generate a CSV report
- Automatically download the report as `orientation_report.csv`

## Project Structure

- `house_faces.py` - Core `FacingAnalyzer` class that performs the geospatial analysis
- `app.py` - Flask web application that serves the analysis results as a downloadable CSV
- `data/` - Directory containing input data files (GNAF properties and road networks)