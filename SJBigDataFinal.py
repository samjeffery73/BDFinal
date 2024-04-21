'''
Sam Jeffery

4/17/2024

Big Data Tools

Final Project
=============================================================
=============================================================

- Choose any Distributed Database/Technology. It can be what we discussed in class or anything of your choice.
Example: Redis, Neo4J, MongoDB, Spark, ELK..

Definitely not a RDBMS (SQL Server, MySQL, Oracle, Teradata, ProstreSQL and so on...). If in doubt email me and get clarified before you start the work.

- Choose a authentic Public Dataset containing at least 100,000 rows (99,999 is okay) and 8 columns. If its JSON it should contain similar datasize.

- Dataset can be JSON, CSV, Parquet, Avro anything your like to use or supported by your database.

- Import the data into your database.

Using Visualization tools like Tableau or Power Bi query the data and create 3  Visualizations. Quality of work should be what you would like to present to your future employer during the interview.
If you are not into Visualization then build 3 ML models.
or combination of Viz & Models (should add upto 3)
Submit the following

Using Screen recorder, walk through the input data, how you imported it, what tool you used and explain the visualization or model. Total video should not be more than 6 minutes and upload it to Youtube. (No need to record your face, just audio is enough)
Also mention what you liked/didn't link in this course.
- Push your project to Github Repo and share the URL.

- UNLISTED Youtube URL of the video. (No Google Drives/Shares). Only YouTube.

'''



'''
RequestID,Boro,Yr,M,D,HH,MM,Vol,SegmentID,WktGeom,street,fromSt,toSt,Direction

'''


'''
Request

RequestID

'''

'''
Location

Boro
WktGeom

'''

'''
Date

Yr
M
D
'''

'''
Time

HH
MM

'''

'''
Segment

segmentID
street
fromSt
toSt
Direction

'''

'''
(Request)-[RECORDED_AT]->(Location)
(Request)-[ON_DATE]->(Date)
(Request)-[AT_TIME]->(Time)
(Request)-[ALONG_SEGMENT]->(Segment)
(Request)-[HAS_VOLUME](vol)->(Segment)

'''


from neo4j import GraphDatabase
import pandas as pd
import csv




uri = "bolt://localhost:7687"
username = "neo4j"
password = "Aolhotqc1."

csv_file_path = "file:///Automated_Traffic_Volume_Counts_20240417.csv"
driver = GraphDatabase.driver(uri, auth=(username, password))

def init_data(tx):
    query = (
        f"LOAD CSV WITH HEADERS FROM '{csv_file_path}' AS row "
        "WITH row SKIP 0 LIMIT 100000 " #limit 100k rows.
        "MERGE (req:Request {RequestID: row.RequestID}) "
        "MERGE (loc:Location {Boro: row.Boro, WktGeom: row.WktGeom}) "
        "MERGE (date:Date {Yr: row.Yr, M: row.M, D: row.D}) "
        "MERGE (time:Time {HH: row.HH, MM: row.MM}) "
        "MERGE (seg:Segment {segmentID: row.SegmentID, street: row.street, fromSt: row.fromSt, toSt: row.toSt, Direction: row.Direction}) "
        "MERGE (req)-[:RECORDED_AT]->(loc) "
        "MERGE (req)-[:ON_DATE]->(date) "
        "MERGE (req)-[:AT_TIME]->(time) "
        "MERGE (req)-[:ALONG_SEGMENT]->(seg) "
        "MERGE (req)-[:HAS_VOLUME {vol: toInteger(row.Vol)}]->(seg);"
    )

    result = tx.run(query)


# To send the data

#with driver.session() as session:
#    results = session.write_transaction(init_data)
 #   print("CSV Data loaded into Neo4J.")



# Run the first query and create a CSV File of results
def query_one_export():
    query = """
MATCH (loc:Location)<-[:RECORDED_AT]-(req:Request)-[:ON_DATE]->(date:Date), 
      (req)-[r:HAS_VOLUME]->(seg)
RETURN loc.Boro AS Borough, date.Yr AS Year, SUM(r.vol) AS TotalVolume
ORDER BY loc.Boro, date.Yr
"""
    with driver.session() as session:
        result = session.run(query)
        with open('output.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Borough', 'Year', 'TotalVolume'])
            for record in result:
                writer.writerow([record['Borough'], record['Year'], record['TotalVolume']])


# Run the first query and create a CSV File of results
def query_two_export():
    query = """
MATCH (time:Time)-[:AT_TIME]-(req:Request)-[r:HAS_VOLUME]->(seg)
RETURN time.HH AS Hour, AVG(toFloat(r.vol)) AS AvgVolume
ORDER BY Hour

"""
    with driver.session() as session:
        result = session.run(query)
        with open('output2.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Hour', 'AvgVolume'])
            for record in result:
                writer.writerow([record['Hour'], record['AvgVolume']])


def query_three_export():
    query = """
MATCH (seg:Segment)-[:ALONG_SEGMENT]-(req:Request)-[r:HAS_VOLUME]->()
RETURN seg.street AS Street, seg.Direction AS Direction, SUM(toFloat(r.vol)) AS TotalVolume
ORDER BY TotalVolume DESC
"""
    with driver.session() as session:
        result = session.run(query)
        with open('output3.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Street', 'Direction', 'TotalVolume'])
            for record in result:
                writer.writerow([record['Street'], record['Direction'], record['TotalVolume']])




#query_one_export()
#query_two_export()
query_three_export()

## Query & Tableau
driver.close()