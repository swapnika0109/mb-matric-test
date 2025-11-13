import json
from flask import Flask, render_template, Response
# Make sure to import the updated class name
from data_processor import FacingAnalyzer

# --- Configuration ---
app = Flask(__name__)
DATA_DIR = 'data/' 

@app.route('/')
def report_generation():
    try:
        # Initialize and run the orientation analysis pipeline using the updated class name
        analyzer = FacingAnalyzer(data_path=DATA_DIR)
        
        # This runs the full pipeline and returns the CSV content as a string
        csv_results = analyzer.run_orientation_pipeline()

        # Serve the CSV string as a downloadable file
        return Response(
            csv_results,
            mimetype="text/csv",
            headers={"Content-disposition": "attachment; filename=orientation_report.csv"}
        )

    except FileNotFoundError as e:
        # Simple error handling for missing files
        return f"<h1>Data Error: Missing Input File</h1><p>Whoops! I couldn't find a required file: {e.filename}. "
        f"Please make sure 'gnaf.parquet' and 'roads.gpkg' are correctly placed in the '{DATA_DIR}' folder.</p>", 500
    
    except Exception as e:
        # General error handling
        return f"<h1>Analysis Failed</h1><p>Something went wrong during the orientation analysis: {e}</p>", 500

if __name__ == '__main__':
    # Flask app will run in debug mode for development
    app.run(debug=True)