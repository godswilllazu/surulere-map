import os
from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import psycopg2
import json

app = Flask(__name__, template_folder='templates')
CORS(app) 

# ⚠️ CONFIGURATION
# Production: Uses Render's Environment Variable
# Local: Uses your hardcoded string
DB_URL = os.environ.get("DATABASE_URL")
LOCAL_DB_CONFIG = "dbname='street_guide' user='postgres' password='admin' host='localhost'"

def get_db_connection():
    try:
        if DB_URL:
            # PROD: Add SSL mode for security
            conn_str = f"{DB_URL}?sslmode=require" if "?" not in DB_URL else DB_URL
            return psycopg2.connect(conn_str)
        else:
            # LOCAL
            return psycopg2.connect(LOCAL_DB_CONFIG)
    except Exception as e:
        print(f"❌ Database Connection Failed: {e}")
        return None

@app.route('/')
def home():
    return render_template('index.html')

# ---------------------------------------------------------
# 1. ATTRIBUTE QUERY
# ---------------------------------------------------------
@app.route('/api/features/<category>', methods=['GET'])
def get_features(category):
    conn = get_db_connection()
    if not conn: return jsonify([]), 500
    cur = conn.cursor()
    
    search_term = f"%{category}%"
    
    query = """
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(t.*)::json)
        )
        FROM (
            SELECT name, category, geom 
            FROM point_features 
            WHERE category ILIKE %s
        ) AS t;
    """
    cur.execute(query, (search_term,))
    result = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    if result is None or result.get('features') is None:
        return jsonify({"type": "FeatureCollection", "features": []})
        
    return jsonify(result)

# ---------------------------------------------------------
# 2. SHORTEST PATH / ROUTING (Fixed Column Name)
# ---------------------------------------------------------
@app.route('/api/route', methods=['POST'])
def get_route():
    data = request.json
    start_lat, start_lng = data['start_lat'], data['start_lng']
    end_lat, end_lng = data['end_lat'], data['end_lng']

    conn = get_db_connection()
    cur = conn.cursor()

    # Find nearest nodes
    sql_start = """
    SELECT id FROM roads_vertices_pgr 
    ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326) LIMIT 1;
    """
    cur.execute(sql_start, (start_lng, start_lat))
    start_node = cur.fetchone()[0]

    cur.execute(sql_start, (end_lng, end_lat))
    end_node = cur.fetchone()[0]

    # Dijkstra Routing
    # 🛠️ FIX: Used "ROADNAME" and COALESCE
    sql_route = f"""
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(row.*)::json)
        ) FROM (
            SELECT r.id, COALESCE(r."ROADNAME", 'Road') AS name, r.geom 
            FROM pgr_dijkstra(
                'SELECT id, source, target, cost, reverse_cost FROM roads',
                {start_node}, {end_node}, directed := false
            ) AS d
            JOIN roads r ON d.edge = r.id
        ) row;
    """
    cur.execute(sql_route)
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return jsonify(result if result else {"type": "FeatureCollection", "features": []})

# ---------------------------------------------------------
# 3. BUFFER ANALYSIS
# ---------------------------------------------------------
@app.route('/api/buffer', methods=['POST'])
def get_buffer():
    data = request.json
    lat = data['lat']
    lng = data['lng']
    dist = data['distance']
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(t.*)::json)
        )
        FROM (
            SELECT name, category, geom 
            FROM point_features 
            WHERE ST_DWithin(
                geom::geography, 
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 
                %s
            )
        ) AS t;
    """
    cur.execute(query, (lng, lat, dist))
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return jsonify(result if result else {"type": "FeatureCollection", "features": []})

# ---------------------------------------------------------
# 4. GET ALL LCDA BOUNDARIES
# ---------------------------------------------------------
@app.route('/api/lcdas', methods=['GET'])
def get_lcdas():
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(t.*)::json)
        )
        FROM (SELECT name, geom FROM lcda_polygons) AS t;
    """
    cur.execute(query)
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return jsonify(result if result else {"type": "FeatureCollection", "features": []})

# ---------------------------------------------------------
# 5. GET ROAD NETWORK (OPTIMIZED for Speed & Memory)
# ---------------------------------------------------------
@app.route('/api/roads_layer', methods=['GET'])
def get_roads_layer():
    conn = get_db_connection()
    if not conn: return jsonify({"features": []})
    cur = conn.cursor()
    
    # 🛠️ OPTIMIZATION:
    # 1. Simplify geometry (0.0001) reduces file size significantly.
    # 2. WHERE ST_Length > 50 filters out tiny segments to prevent server crashes.
    query = """
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(
                json_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(ST_Simplify(geom, 0.0001), 5)::json,
                    'properties', json_build_object('name', COALESCE("ROADNAME", 'Road'))
                )
            )
        )
        FROM roads
        WHERE ST_Length(geom::geography) > 50; 
    """
    try:
        cur.execute(query)
        result = cur.fetchone()[0]
    except Exception as e:
        print(f"Road Layer Error: {e}")
        conn.rollback()
        result = None

    cur.close()
    conn.close()
    return jsonify(result if result else {"type": "FeatureCollection", "features": []})

# ---------------------------------------------------------
# 6. GET PROJECT BOUNDARY
# ---------------------------------------------------------
@app.route('/api/boundary', methods=['GET'])
def get_boundary():
    conn = get_db_connection()
    cur = conn.cursor()
    query = """
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(t.*)::json)
        )
        FROM (SELECT * FROM boundary) AS t;
    """
    cur.execute(query)
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return jsonify(result if result else {"type": "FeatureCollection", "features": []})

# ---------------------------------------------------------
# 7. GLOBAL SEARCH (Fixed Column Name)
# ---------------------------------------------------------
@app.route('/api/search', methods=['GET'])
def search_all():
    query_text = request.args.get('q', '')
    if not query_text or len(query_text) < 2:
        return jsonify([])

    conn = get_db_connection()
    cur = conn.cursor()
    
    # 🛠️ FIX: Changed roadname to "ROADNAME"
    sql = """
        SELECT name, category, ST_X(ST_Centroid(geom)) as lng, ST_Y(ST_Centroid(geom)) as lat
        FROM (
            SELECT name, category, geom FROM point_features WHERE name ILIKE %s
            UNION ALL
            SELECT "ROADNAME" as name, 'Road' as category, geom FROM roads WHERE "ROADNAME" ILIKE %s
            UNION ALL
            SELECT name, 'District' as category, geom FROM lcda_polygons WHERE name ILIKE %s
        ) as combined_results
        LIMIT 10;
    """
    wildcard_query = f"%{query_text}%"
    cur.execute(sql, (wildcard_query, wildcard_query, wildcard_query))
    
    results = []
    for row in cur.fetchall():
        results.append({"name": row[0], "category": row[1], "lng": row[2], "lat": row[3]})
        
    cur.close()
    conn.close()
    return jsonify(results)

# ---------------------------------------------------------
# 8. NEAREST NEIGHBOR
# ---------------------------------------------------------
@app.route('/api/nearest', methods=['POST'])
def get_nearest():
    data = request.json
    lat = data['lat']
    lng = data['lng']
    category = data['category']

    conn = get_db_connection()
    cur = conn.cursor()

    search_term = f"%{category}%"

    # 1. Find the Geometrically Nearest Item
    sql_find_closest = """
        SELECT name, category, ST_X(geom), ST_Y(geom)
        FROM point_features
        WHERE category ILIKE %s
        ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        LIMIT 1;
    """
    cur.execute(sql_find_closest, (search_term, lng, lat))
    target = cur.fetchone()

    if not target:
        cur.close()
        conn.close()
        return jsonify({"type": "FeatureCollection", "features": []})

    target_name, target_cat, target_lng, target_lat = target

    # 2. Try to Route to it (Dijkstra)
    cur.execute("""
        SELECT id FROM roads_vertices_pgr 
        ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326) LIMIT 1;
    """, (lng, lat))
    start_node_row = cur.fetchone()
    
    cur.execute("""
        SELECT id FROM roads_vertices_pgr 
        ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326) LIMIT 1;
    """, (target_lng, target_lat))
    end_node_row = cur.fetchone()

    route_res = None
    if start_node_row and end_node_row:
        start_node = start_node_row[0]
        end_node = end_node_row[0]

        sql_route = f"""
            SELECT ST_AsGeoJSON(ST_Union(r.geom)), SUM(r.cost)
            FROM pgr_dijkstra(
                'SELECT id, source, target, cost, reverse_cost FROM roads',
                {start_node}, {end_node}, directed := false
            ) AS d
            JOIN roads r ON d.edge = r.id;
        """
        cur.execute(sql_route)
        route_res = cur.fetchone()
    
    features = []
    
    # Destination Point
    features.append({
        "type": "Feature",
        "geometry": { "type": "Point", "coordinates": [target_lng, target_lat] },
        "properties": { "name": target_name, "category": target_cat, "is_target": True }
    })

    if route_res and route_res[0]:
        route_geom = json.loads(route_res[0])
        total_dist = round(route_res[1]) 
        
        features.append({
            "type": "Feature",
            "geometry": route_geom,
            "properties": { 
                "type": "route", 
                "distance_msg": f"Road Distance: {total_dist}m" 
            }
        })
    else:
        # Fallback: Straight Line
        cur.execute("""
            SELECT ST_Distance(
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
            )
        """, (lng, lat, target_lng, target_lat))
        straight_dist = round(cur.fetchone()[0])

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[lng, lat], [target_lng, target_lat]]
            },
            "properties": { 
                "type": "route", 
                "style": "dashed",
                "distance_msg": f"Straight Distance: {straight_dist}m (No road path)" 
            }
        })

    cur.close()
    conn.close()
    return jsonify({"type": "FeatureCollection", "features": features})

# ---------------------------------------------------------
# 9. SPATIAL IDENTIFY
# ---------------------------------------------------------
@app.route('/api/identify', methods=['POST'])
def identify_location():
    data = request.json
    lat = data['lat']
    lng = data['lng']

    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(t.*)::json)
        )
        FROM (
            SELECT name, geom 
            FROM lcda_polygons 
            WHERE ST_Intersects(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        ) AS t;
    """
    cur.execute(query, (lng, lat))
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return jsonify(result if result else {"type": "FeatureCollection", "features": []})

# ---------------------------------------------------------
# 10. DESCRIPTIVE STATISTICS
# ---------------------------------------------------------
@app.route('/api/stats', methods=['GET'])
def get_stats():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT category, COUNT(*) as count 
        FROM point_features 
        GROUP BY category 
        ORDER BY count DESC
    """)
    poi_rows = cur.fetchall()
    
    cur.execute("SELECT SUM(ST_Length(geom::geography)) / 1000 FROM roads")
    road_len = cur.fetchone()[0]

    cur.execute("SELECT SUM(ST_Area(geom::geography)) / 1000000 FROM lcda_polygons")
    area_sqkm = cur.fetchone()[0]

    cur.close()
    conn.close()

    return jsonify({
        "poi_stats": [{"label": r[0], "value": r[1]} for r in poi_rows],
        "total_road_km": round(road_len, 2) if road_len else 0,
        "total_area_sqkm": round(area_sqkm, 2) if area_sqkm else 0
    })

# ---------------------------------------------------------
# 11. LCDA SPECIFIC STATS (Fixed Column Name)
# ---------------------------------------------------------
@app.route('/api/stats/<lcda_name>', methods=['GET'])
def get_lcda_stats(lcda_name):
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Area
    cur.execute("SELECT ST_Area(geom::geography) / 1000000 FROM lcda_polygons WHERE name = %s", (lcda_name,))
    res_area = cur.fetchone()
    area = res_area[0] if res_area else 0

    # 2. Roads
    sql_road = """
        SELECT COUNT(*), MAX(ST_Length(r.geom::geography))
        FROM roads r, lcda_polygons l
        WHERE l.name = %s AND ST_Intersects(r.geom, l.geom)
    """
    cur.execute(sql_road, (lcda_name,))
    road_data = cur.fetchone()
    road_count = road_data[0]
    longest_road_len = round(road_data[1]) if road_data[1] else 0

    # 3. Longest Road Name (FIXED: "ROADNAME")
    sql_longest_name = """
        SELECT r."ROADNAME" 
        FROM roads r, lcda_polygons l 
        WHERE l.name = %s AND ST_Intersects(r.geom, l.geom) 
        ORDER BY ST_Length(r.geom::geography) DESC LIMIT 1
    """
    cur.execute(sql_longest_name, (lcda_name,))
    res_name = cur.fetchone()
    longest_road_name = res_name[0] if res_name else "None"

    # 4. POI Stats
    sql_poi = """
        SELECT 
            p.category, 
            COUNT(*), 
            json_agg(json_build_object(
                'name', p.name, 
                'lat', ST_Y(p.geom::geometry), 
                'lng', ST_X(p.geom::geometry)
            )) 
        FROM point_features p, lcda_polygons l
        WHERE l.name = %s AND ST_Intersects(p.geom, l.geom)
        GROUP BY p.category
    """
    cur.execute(sql_poi, (lcda_name,))
    poi_rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({
        "lcda_name": lcda_name,
        "area_sqkm": round(area, 2),
        "road_count": road_count,
        "longest_road": f"{longest_road_name} ({longest_road_len}m)",
        "poi_stats": [{"label": r[0], "value": r[1], "items": r[2]} for r in poi_rows]
    })

if __name__ == '__main__':
    # Use PORT from Render env, fallback to 5000 for local
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
