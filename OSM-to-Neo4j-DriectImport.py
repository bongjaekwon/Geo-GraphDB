import neo4j
import osmnx as ox

# Neo4j 설정
NEO4J_URI = "neo4j://localhost:"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""

# Neo4j 드라이버 생성
driver = neo4j.GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# 부산진구의 도로 네트워크 그래프 생성
G = ox.graph_from_place("Busanjin-gu", network_type="drive")
fig, ax = ox.plot_graph(G)

# 그래프에서 노드와 관계 데이터프레임 생성
gdf_nodes, gdf_relationships = ox.graph_to_gdfs(G)
gdf_nodes.reset_index(inplace=True)
gdf_relationships.reset_index(inplace=True)

# 노드와 관계 시각화
gdf_nodes.plot(markersize=0.1)
gdf_relationships.plot(markersize=0.01, linewidth=0.5)

# 제약 조건 및 인덱스 쿼리 정의
constraint_query = (
    "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Intersection) "
    "REQUIRE i.osmid IS UNIQUE"
)

rel_index_query = (
    "CREATE INDEX IF NOT EXISTS FOR ()-[r:ROAD_SEGMENT]-() "
    "ON r.osmids"
)

address_constraint_query = (
    "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) "
    "REQUIRE a.id IS UNIQUE"
)

point_index_query = (
    "CREATE POINT INDEX IF NOT EXISTS FOR (i:Intersection) "
    "ON i.location"
)

# 노드 삽입 쿼리
node_query = '''
    UNWIND $rows AS row
    WITH row WHERE row.osmid IS NOT NULL
    MERGE (i:Intersection {osmid: row.osmid})
        SET i.location = point({latitude: row.y, longitude: row.x}),
            i.ref = row.ref,
            i.highway = row.highway,
            i.street_count = toInteger(row.street_count)
    RETURN COUNT(*) as total
'''

# 관계 삽입 쿼리
rels_query = '''
    UNWIND $rows AS road
    MATCH (u:Intersection {osmid: road.u})
    MATCH (v:Intersection {osmid: road.v})
    MERGE (u)-[r:ROAD_SEGMENT {osmid: road.osmid}]->(v)
        SET r.oneway = road.oneway,
            r.lanes = road.lanes,
            r.ref = road.ref,
            r.name = road.name,
            r.highway = road.highway,
            r.max_speed = road.maxspeed,
            r.length = toFloat(road.length)
    RETURN COUNT(*) AS total
'''

# 제약 조건 생성 함수
def create_constraints(tx):
    tx.run(constraint_query)
    tx.run(rel_index_query)
    tx.run(address_constraint_query)
    tx.run(point_index_query)

# 데이터 삽입 함수
def insert_data(tx, query, rows, batch_size=10000):
    total = 0
    batch = 0

    while batch * batch_size < len(rows):
        results = tx.run(
            query, 
            parameters={'rows': rows[batch * batch_size:(batch + 1) * batch_size].to_dict('records')}
        ).data()
        
        print(results)
        total += results[0]['total']
        batch += 1

# 노드 데이터 삽입
with driver.session() as session:
    session.execute_write(create_constraints)
    session.execute_write(insert_data, node_query, gdf_nodes.drop(columns=['geometry']))  # FIXME: handle

# 관계 데이터 삽입
with driver.session() as session:
    session.execute_write(insert_data, rels_query, gdf_relationships.drop(columns=['geometry']))  # FIXME: handle geometry
