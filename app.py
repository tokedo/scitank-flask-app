from flask import Flask, request, abort, jsonify
import os
from functools import wraps

import pytz
import psycopg2
from psycopg2 import sql
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime

app = Flask(__name__)

API_KEY = os.environ['API_KEY']

# Configuration for connecting to your PostgreSQL database
DATABASE_CONFIG = {
    'dbname': os.environ['DB_NAME'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'host': os.environ['DB_HOST'],
    'port': os.environ['DB_PORT'],
    'sslmode': 'require'
}

# Create a connection pool
dbpool = SimpleConnectionPool(1, 20, **DATABASE_CONFIG)  # Adjust min and max conn as needed

VALID_KEYS = {
    'channel1', 
    'channel2',
    'channel3',
    'channel4',
    'rewifi',
    'test'
} 

def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('x-api-key')
        if api_key != API_KEY:
            abort(401)
        return f(*args, **kwargs)
    return decorated_function

@app.route('/data', methods=['POST'])
@require_api_key
def data():
    if request.method == 'POST':
        # Get the current server time in PST timezone
        pst = pytz.timezone('America/Los_Angeles')
        server_time = datetime.now(pst)

        # Get a connection from the pool
        conn = dbpool.getconn()
        cur = conn.cursor()

        try:
            # Iterate over the form data and save to the database
            for key, value in request.form.items():
                if key in VALID_KEYS:
                    # Construct the SQL query
                    query = sql.SQL(
                        "INSERT INTO scitank (timestamp, parameter, value) VALUES (%s, %s, %s)"
                    )
                    cur.execute(query, (server_time, key, float(value)))

            # Commit changes to the database
            conn.commit()

        except Exception as e:
            # In case of an error, rollback any changes
            conn.rollback()
            return f"An error occurred: {str(e)}", 500

        finally:
            # Always close the cursor and release the connection back to the pool
            cur.close()
            dbpool.putconn(conn)

        return 'Data saved', 201

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
