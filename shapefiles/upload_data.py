import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from shapely.geometry import Point
import os

# CONFIGURATION
DB_USER = 'postgres'
DB_PASS = 'admin'        
DB_NAME = 'street_guide' 
DB_HOST = 'localhost'

# FILE CONFIGURATION
ROAD_FILE = 'ROAD.shp'
BOUNDARY_FILE = 'BOUNDARY.shp'  # 🛠️ New: Specific Boundary File

POINT_FILES = {
    'BANK.shp': 'Bank',
    'CHURCH.shp': 'Church',
    'EVENT_CENTRE.shp': 'Event Center',
    'HOSPITAL.shp': 'Hospital',
    'POLICE_STATION.shp': 'Police Station',
    'HOTEL.shp': 'Hotel',
    'MARKET.shp': 'Market',
    'MOSQUE.shp': 'Mosque',
    'PRY_SCHOOL.shp': 'Primary School',
    'SEC_SCHOOL.shp': 'Secondary School'
}

LCDA_FILES = {
    'SURULERE LCDA BOUNDARY.shp': 'Surulere',
    'ITIRE_IKATE LCDA BOUNDARY.shp': 'Itire-Ikate',
    'COOKER_AGUDA LCDA BOUNDARY.shp': 'Coker-Aguda'
    # Removed 'BOUNDARY.shp' from here to process it separately
}

def build_topology_in_python(gdf):
    """
    Manually calculates Source/Target nodes for a LineString GeoDataFrame.
    """
    print("      ...Calculating network nodes in Python...")
    geo_col_name = gdf.geometry.name 
    nodes = {} 
    node_counter = 1
    sources = []
    targets = []
    costs = []
    
    for idx, row in gdf.iterrows():
        geom = row[geo_col_name]
        if geom is None or geom.is_empty:
            sources.append(None)
            targets.append(None)
            costs.append(0)
            continue

        try:
            start_coord = (round(geom.coords[0][0], 6), round(geom.coords[0][1], 6))
            end_coord = (round(geom.coords[-1][0], 6), round(geom.coords[-1][1], 6))
        except NotImplementedError:
             if geom.geom_type == 'MultiLineString':
                 first_line = geom.geoms[0]
                 last_line = geom.geoms[-1]
                 start_coord = (round(first_line.coords[0][0], 6), round(first_line.coords[0][1], 6))
                 end_coord = (round(last_line.coords[-1][0], 6), round(last_line.coords[-1][1], 6))
             else:
                 continue

        if start_coord not in nodes:
            nodes[start_coord] = node_counter
            node_counter += 1
        if end_coord not in nodes:
            nodes[end_coord] = node_counter
            node_counter += 1
            
        sources.append(nodes[start_coord])
        targets.append(nodes[end_coord])
        costs.append(geom.length)

    gdf['source'] = sources
    gdf['target'] = targets
    gdf['cost'] = costs
    gdf['reverse_cost'] = costs 
    
    node_data = [{'id': v, 'geometry': Point(k)} for k, v in nodes.items()]
    nodes_gdf = gpd.GeoDataFrame(node_data, crs=gdf.crs)
    
    return gdf, nodes_gdf

def setup_database():
    db_url = "postgresql://postgres:yiPamHXnavQmKE61@db.xyz.supabase.co:5432/postgres"
    engine = create_engine(db_url)
    print(f"🔌 Connected to database '{DB_NAME}'...")

    # ---------------------------------------------------------
    # A. UPLOAD POINTS
    # ---------------------------------------------------------
    print("\n📦 Processing Point Features...")
    first_point = True
    for filename, category in POINT_FILES.items():
        if os.path.exists(filename):
            print(f"   Processing {category} ({filename})...")
            gdf = gpd.read_file(filename)
            
            if gdf.crs != "EPSG:4326": 
                gdf = gdf.to_crs("EPSG:4326")
            
            gdf.columns = [c.lower() for c in gdf.columns] 
            
            if 'actual_nam' in gdf.columns:
                gdf['name'] = gdf['actual_nam']
            elif 'actual_name' in gdf.columns:
                gdf['name'] = gdf['actual_name']
            elif 'name' not in gdf.columns:
                gdf['name'] = f"{category} (Unknown)"
            
            gdf['category'] = category
            gdf = gdf[['name', 'category', 'geometry']]
            gdf = gdf.rename_geometry('geom')

            mode = 'replace' if first_point else 'append'
            gdf.to_postgis('point_features', engine, if_exists=mode, index=True)
            print(f"   ✅ Added {len(gdf)} {category}s.")
            first_point = False
        else:
            print(f"   ⚠️ File missing: {filename}")

    # ---------------------------------------------------------
    # B. UPLOAD LCDA (Sub-districts)
    # ---------------------------------------------------------
    print("\n🏙️ Processing LCDA Boundaries...")
    first_lcda = True
    for filename, lcda_name in LCDA_FILES.items():
        if os.path.exists(filename):
            print(f"   Processing {lcda_name}...")
            gdf = gpd.read_file(filename)
            if gdf.crs != "EPSG:4326": gdf = gdf.to_crs("EPSG:4326")
            
            # Ensure name column exists
            gdf['name'] = lcda_name
            
            # Keep only necessary columns
            gdf = gdf[['name', 'geometry']].rename_geometry('geom')
            
            mode = 'replace' if first_lcda else 'append'
            gdf.to_postgis('lcda_polygons', engine, if_exists=mode, index=True)
            first_lcda = False
            print(f"   ✅ {lcda_name} Done.")
        else:
            print(f"   ⚠️ LCDA File missing: {filename}")

    # ---------------------------------------------------------
    # C. UPLOAD PROJECT BOUNDARY (New Section)
    # ---------------------------------------------------------
    print("\n🚧 Processing Main Boundary...")
    if os.path.exists(BOUNDARY_FILE):
        gdf = gpd.read_file(BOUNDARY_FILE)
        if gdf.crs != "EPSG:4326": gdf = gdf.to_crs("EPSG:4326")
        
        gdf = gdf.rename_geometry('geom')
        # We assume the boundary file might have minimal attributes, so we just upload geom
        gdf.to_postgis('boundary', engine, if_exists='replace', index=False)
        print("   ✅ Project Boundary Uploaded to table 'boundary'.")
    else:
        print(f"   ⚠️ Boundary File missing: {BOUNDARY_FILE}")

    # ---------------------------------------------------------
    # D. PROCESS ROADS
    # ---------------------------------------------------------
    print("\n🛣️ Processing Road Network...")
    if os.path.exists(ROAD_FILE):
        gdf = gpd.read_file(ROAD_FILE)
        if gdf.crs != "EPSG:4326": gdf = gdf.to_crs("EPSG:4326")
        gdf.columns = [c.lower() for c in gdf.columns]
        
        gdf = gdf.rename_geometry('geom')
        
        roads_with_topo, nodes_gdf = build_topology_in_python(gdf)
        
        print("   🚀 Uploading Roads...")
        roads_with_topo.to_postgis('roads', engine, if_exists='replace', index=True)
        
        print("   🚀 Uploading Nodes...")
        nodes_gdf = nodes_gdf.rename_geometry('geom')
        nodes_gdf.to_postgis('roads_vertices_pgr', engine, if_exists='replace', index=False)
        
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE roads ADD COLUMN IF NOT EXISTS id SERIAL PRIMARY KEY;"))
            conn.execute(text("UPDATE roads SET cost = ST_Length(geom::geography);"))
            conn.execute(text("UPDATE roads SET reverse_cost = cost;"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_roads_source ON roads(source);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_roads_target ON roads(target);"))
            conn.commit()
            
        print("   ✅ Topology Built and Uploaded.")
    else:
        print(f"   ⚠️ Road file missing: {ROAD_FILE}")

    print("\n🎉 Database Setup Complete!")

if __name__ == "__main__":
    setup_database()