from flask import Flask, request, abort, jsonify
import os
from functools import wraps

import psycopg2
from psycopg2 import sql
from datetime import datetime, timezone, timedelta

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
def connect():
  return psycopg2.connect(**DATABASE_CONFIG)


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
    server_time = datetime.now(timezone.utc).astimezone(
        timezone(timedelta(hours=-8)))

    # Create a connection
    conn = connect()
    cur = conn.cursor()

    try:
      # Iterate over the form data and save to the database
      for key, value in request.form.items():
        if key not in ["timestamp"]:
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
      # Always close the cursor and the connection
      cur.close()
      conn.close()

    return 'Data saved', 201


@app.teardown_appcontext
def close_pool(error):
  dbpool.closeall()


if __name__ == '__main__':
  port = int(os.environ.get('PORT', 8080))
  app.run(host='0.0.0.0', port=port)
