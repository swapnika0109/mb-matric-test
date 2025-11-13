import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
import os
from math import degrees, atan2

class FacingAnalyzer:
    """
    Core engine for determining property orientation (N, S, E, W) 
    by analyzing the nearest road geometry for each property point.
    """
    def __init__(self, data_path='data/'):
        self.data_path = data_path
        self.gnaf_points = None
        self.roads_lines = None

    def _load_raw_data(self):
        """Grabs the raw GNAF (properties) and road network data."""
        print("Starting data load...")
        
        try:
            # 1. Property Points: We need the lat/lon and address info
            gnaf_file = f"{self.data_path}gnaf.parquet"
            self.gnaf = pd.read_parquet(gnaf_file)
            print(f"Loaded property points from: {os.path.basename(gnaf_file)}")

            # 2. Road Network: Line geometries for direction calculation
            roads_file = f"{self.data_path}roads.gpkg"
            # Read roads into a GeoDataFrame
            self.roads = gpd.read_file(roads_file)
            print(f"Loaded road network from: {os.path.basename(roads_file)}")
        
        except FileNotFoundError as e:
            # Better error message for the end user if files are missing
            raise FileNotFoundError(
                f"Couldn't find a critical data file: {e.filename}. "
                "Make sure 'gnaf.parquet' and 'roads.gpkg' are in the 'data/' folder."
            )
        except Exception as e:
            print(f"Error loading data: {e}")
            raise

    def _prepare_data_and_convert(self):
        """Converts the GNAF DataFrame into a GeoDataFrame and ensures CRS alignment."""
        print("Cleaning up data structures for spatial analysis...")
        
        # 1. Convert Pandas DataFrame to GeoDataFrame (using WGS84, which is standard for raw lat/lon)
        # We need to manually set the CRS since the original file is just a Pandas DataFrame.
        self.gnaf_points = gpd.GeoDataFrame(
            self.gnaf.copy(),
            geometry=gpd.points_from_xy(self.gnaf.Longitude, self.gnaf.Latitude),
            crs="EPSG:4326"
        )

        # 2. Make sure the road data uses the exact same CRS as our property points
        self.roads_lines = self.roads.to_crs(self.gnaf_points.crs)

        # 3. Strip unnecessary road columns (only keep geometry) and assign a clean ID
        self.roads_lines = self.roads_lines[['geometry']].reset_index(names=['ROAD_ID'])
        
        print("Roads and properties are now clean GeoDataFrames with matching CRS.")

    @staticmethod
    def _calculate_segment_bearing(line, point):
        """
        Determines the compass bearing (0-360 degrees) of the road segment 
        that is closest to the given property point.
        """
        # Finds the exact point on the road closest to the house's location
        closest_point_on_line = line.interpolate(line.project(point))
        
        # We need the two vertices that define the *segment* closest to that point
        coords = list(line.coords)
        
        min_dist = np.inf
        segment_points = None
        
        for i in range(len(coords) - 1):
            p1 = Point(coords[i])
            p2 = Point(coords[i+1])
            # Create a line segment between the two consecutive vertices
            segment = gpd.GeoSeries([p1, p2]).unary_union 
            
            # Simple check to find which segment contains the interpolated point
            if closest_point_on_line.distance(segment) < min_dist:
                min_dist = closest_point_on_line.distance(segment)
                segment_points = (p1, p2)

        if not segment_points:
             return np.nan # Can't find the direction

        # Calculate bearing (angle from North, clockwise) using the segment's two endpoints
        p1, p2 = segment_points
        dx = p2.x - p1.x
        dy = p2.y - p1.y
        bearing = (degrees(atan2(dx, dy)) + 360) % 360
        
        return bearing

    @staticmethod
    def _bearing_to_cardinal(bearing):
        """Converts a 0-360 degree bearing into a simple cardinal direction (N, NE, E, etc.)."""
        # Standard 8-point compass partitioning
        if 337.5 <= bearing < 360 or 0 <= bearing < 22.5:
            return 'N'
        elif 22.5 <= bearing < 67.5:
            return 'NE'
        elif 67.5 <= bearing < 112.5:
            return 'E'
        elif 112.5 <= bearing < 157.5:
            return 'SE'
        elif 157.5 <= bearing < 202.5:
            return 'S'
        elif 202.5 <= bearing < 247.5:
            return 'SW'
        elif 247.5 <= bearing < 292.5:
            return 'W'
        elif 292.5 <= bearing < 337.5:
            return 'NW'
        else:
            return 'Unknown'

    def _determine_orientation(self):
        """
        Matches properties to the closest road segment and calculates the house facing direction.
        """
        print("Finding nearest road for each property and calculating orientation...")

        # 3.1 Geospatial join to find the closest road line for every GNAF point (max 200m away)
        try:
            matched_df = self.gnaf_points.sjoin_nearest(
                self.roads_lines, 
                how='left', 
                # This max_distance should ideally be run in a projected CRS (meters), but using 200 units as a placeholder
                max_distance=200 
            ).drop(columns=['index_right'])
        except Exception as e:
            # This is a critical fallback/error handler if the primary spatial join fails
            print(f"Warning: sjoin_nearest hit an error ({e}). This often means incompatible GeoPandas/Shapely versions.")
            raise RuntimeError("CRITICAL: Spatial join failed. Check GeoPandas installation and CRS settings.")


        # 4. Process matches and calculate orientation for valid properties
        results = []
        
        for idx, row in matched_df.dropna(subset=['ROAD_ID']).iterrows():
            property_point = row['geometry']
            # Retrieve the matched road line geometry using the ROAD_ID
            road_line = self.roads_lines.loc[row['ROAD_ID'], 'geometry']
            
            # Get the road's bearing (direction) at the property's frontage
            road_bearing = self._calculate_segment_bearing(road_line, property_point)
            
            if not np.isnan(road_bearing):
                # House faces perpendicular (90 degrees offset) to the street
                house_bearing = (road_bearing + 90) % 360 
                orientation = self._bearing_to_cardinal(house_bearing)
            else:
                orientation = 'Unknown'

            # Build the report row
            results.append({
                # Safely pull data from the original GNAF columns
                'Address': row.get('Address', 'N/A'),
                'PID': row.get('PID', 'N/A'),
                'Orientation': orientation
            })

        print("Orientation calculation finished.")
        return pd.DataFrame(results)

    def run_orientation_pipeline(self):
        """Runs the full analysis pipeline from raw data to final CSV string output."""
        
        self._load_raw_data()
        self._prepare_data_and_convert()

        # Run the core analysis
        orientation_df = self._determine_orientation()
        
        if orientation_df.empty:
            raise ValueError(
                "The analysis didn't yield any results. "
                "Check data files to ensure properties are within 200m of a road."
            )
        
        # Final Step: Format the output as a CSV string
        print("Analysis complete. Preparing the final CSV report.")
        report_df = orientation_df[['Address', 'PID', 'Orientation']]
        return report_df.to_csv(index=False)


if __name__ == '__main__':
    # Simple test execution block.
    try:
        analyzer = FacingAnalyzer(data_path='./data/') 
        csv_report = analyzer.run_orientation_pipeline()
        print("\n--- Example Report Snip (Success if data files were present) ---")
        print(csv_report[:200] + "...")
    except FileNotFoundError as e:
        print(f"\n[ERROR] Can't find the data: {e}")
        print("ACTION: You must provide 'gnaf.parquet' and 'roads.gpkg' in the './data/' folder to run this.")
    except Exception as e:
        print(f"\n[FATAL ERROR] Something went wrong: {e}")