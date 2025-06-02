from datetime import datetime
import psycopg2

file_name = 'posts.json'
db_name = "mydatabase"
db_user = "myuser"
db_password = "mypassword"
db_host = "localhost"
db_port = "5432"

# Connect to the database
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)

# Create a cursor object
cur = conn.cursor()

# Execute a query
cur.execute("SELECT created_utc,title,subreddit FROM posts order by created_utc desc")

# Fetch all results
results = cur.fetchall()

# Close the cursor and connection
cur.close()
conn.close()

# Convert results to a list
results_list = [list(row) for row in results]
# print(results_list)
# for row in results_list:
#     print(datetime.utcfromtimestamp(int(row[0])).strftime('%Y-%m-%d %H:%M:%S'), row[1])
import json

with open(file_name, 'w', encoding='utf-8') as f:
    json_data = {}
    for row in results_list:
        created_utc = datetime.utcfromtimestamp(int(row[0]))
        date = created_utc.strftime('%Y-%m-%d')

        if row[2] not in json_data:
            json_data[row[2]] = {}
        if date not in json_data[row[2]]:
            json_data[row[2]][date] = []

        json_data[row[2]][date].append({
            # 'created_utc': created_utc.strftime('%Y-%m-%d %H:%M:%S'),
            'title': row[1]
        })

    json.dump(json_data, f, ensure_ascii=False, indent=4)
