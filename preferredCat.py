#!/usr/bin/python

import json
import pandas as pd
import sys
import mysql.connector
from datetime import date, timedelta, datetime
import http.client
import html
import psycopg2     # PostgreSQL/Redshift database connection
from psycopg2 import pool  # Connection pooling for Redshift

# ============================================================================
# CONFIGURATION
# ============================================================================

with open('def.json') as f:
    confData = json.load(f)

# MySQL configuration (for writes)
db_host = confData['db_host']
db_user = confData['db_user']
db_pass = confData['db_pass']
db_name = confData['db_name']

# Redshift configuration (for reads)
redshift_host = confData['redshift_host']
redshift_user = confData['redshift_user']
redshift_pass = confData['redshift_pass']
redshift_db = confData['redshift_db']
redshift_port = confData['redshift_port']

# Redshift connection configuration
redshift_config = {
    "host": redshift_host,
    "user": redshift_user,
    "password": redshift_pass,
    "database": redshift_db,
    "port": redshift_port
}

# Create Redshift connection pool
try:
    print(f"Connecting to Redshift at {redshift_config['host']}:{redshift_config['port']}")
    redshift_pool = pool.SimpleConnectionPool(1, 5, **redshift_config)
    print("Redshift connection pool created successfully.")
except Exception as e:
    print(f"Error connecting to Redshift: {str(e)}")
    # slack_updates(f"preferredCat.py: Error connecting to Redshift: {str(e)}")
    redshift_pool = None

# Configuration constants
LOOKBACK_MONTHS = 3          # How far back to analyze clicks
MAX_CATEGORIES_PER_USER = 4  # Top N categories to store

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def slack_updates(message):
    """
    Send notification to Slack channel
    """
    conn = http.client.HTTPSConnection("hooks.slack.com")
    payload = json.dumps({
        "text": html.escape(str(message))
    })
    headers = {
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/services/T0L4M9K43/B06H4E1M1FY/L5Nzrre5YDck5FCehfJtoJSv", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print("slack response: ", data.decode("utf-8"))

def GetRecords(query):
    """
    Execute SELECT query on MySQL and return results
    
    Args:
        query (str): SQL SELECT statement
        
    Returns:
        list: Array of tuples (rows), or empty list if no results
    """
    dbo = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        database=db_name
    )

    c = dbo.cursor()
    c.execute(query)
    rows = c.fetchall()
    
    if not c.rowcount:
        print("No results found")
        return []

    dbo.close()
    return rows

def GetRedshiftRecords(query):
    """
    Execute SELECT query on Redshift and return results
    
    Args:
        query (str): SQL SELECT statement (Redshift syntax)
        
    Returns:
        list: Array of tuples (rows), or empty list if no results
    """
    if redshift_pool is None:
        print("Redshift pool is not defined. Cannot fetch records.")
        slack_updates("preferredCat.py: Redshift pool is not defined. Cannot fetch records.")
        return []

    try:
        # Get connection from pool
        dbo = redshift_pool.getconn()
        
        # Create cursor object
        cursor = dbo.cursor()
        
        # Execute query
        cursor.execute(query)
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Close cursor
        cursor.close()
        
        # Return connection to pool
        redshift_pool.putconn(dbo)
        
        # Check if any rows returned
        if not rows:
            print("No results found in Redshift query")
            return []
        
        return rows
        
    except Exception as e:
        print(f"Error fetching records from Redshift: {str(e)}")
        slack_updates(f"preferredCat.py: Error fetching records from Redshift: {str(e)}")
        return []

def ExecCommand(query):
    """
    Execute UPDATE, INSERT, or DELETE query
    
    Args:
        query (str): SQL command to execute
    """
    dbo = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        database=db_name
    )

    c = dbo.cursor()
    c.execute(query)
    dbo.commit()
    dbo.close()

def InsertRecords(data):
    """
    Batch insert records into database
    
    Args:
        data (list): [values, length, table, columns]
            - values: List of tuples to insert
            - length: Number of records
            - table: Table name
            - columns: List of column names
    """
    if (data[1] == 0): 
        print("No records to insert")
        return

    dbo = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        database=db_name
    )

    cols = ', '.join(data[3])

    # Build placeholders
    placeholders = []
    for x in data[3]:
        placeholders.append('%s')
    p = ', '.join(placeholders)

    sql = f"INSERT INTO {data[2]} ({cols}) VALUES ({p})"

    mysql_limit_int = 990  # MySQL parameter limit

    if(data[1] < mysql_limit_int):
        c = dbo.cursor()
        try:
            c.executemany(sql, data[0])
            dbo.commit()
            print(c.rowcount, "records inserted.")
        except Exception as e:
            slack_updates(f"NewsUserPreferredCategory.py exception: {str(e)}")
            dbo.rollback()
            print(c.rowcount, "records not inserted, rolling back.")
    else:
        # Handle large batches
        temp_a = []
        i = 0  # record count
        x = 0  # iteration count
        for r in data[0]:
            i = i + 1
            temp_a.append(r)
            if(i % mysql_limit_int == 0 or i == data[1]):
                x = x + 1
                c = dbo.cursor()
                try:
                    c.executemany(sql, temp_a)
                    dbo.commit()
                    print(c.rowcount, "records inserted. Iteration: " + str(x))
                    temp_a = []
                except TypeError as e:
                    dbo.rollback()
                    print(c.rowcount, "incremental issue, rolling back. Iteration: " + str(x))
                    slack_updates(f"NewsUserPreferredCategory.py exception: {str(e)}")
                    break

    dbo.close()

# ============================================================================
# NEWS CATEGORY ANALYSIS FUNCTIONS
# ============================================================================

def GetNewsCategoryStats():
    """
    Get news category click statistics per user from last 3 months FROM REDSHIFT
    
    WHY REDSHIFT:
    - hp_clicks table contains millions of rows
    - Redshift is optimized for analytical queries on large datasets
    - Much faster than querying MySQL directly
    - MySQL is used only for writes (updating preferred_categories table)
    
    WHAT THIS DOES:
    - Joins hp_clicks with blogs table (Redshift equivalent of blogs)
    - Groups by user (e_hash) and category
    - Counts clicks per category per user
    - Orders categories by click count (most clicked first)
    - Uses LISTAGG for Redshift (equivalent to MySQL GROUP_CONCAT)
    - Uses DATEADD for Redshift (equivalent to MySQL INTERVAL)
    
    Returns:
        list: [(e_hash, comma_separated_category_ids), ...]
        Example: [('user_hash_1', '2,5,3,7'), ('user_hash_2', '1,4,6'), ...]
    """
    query = f"""
    SELECT 
        a.e_hash, 
        LISTAGG(a.category_id, ',') WITHIN GROUP (ORDER BY a.cat_count DESC) AS cats
    FROM (
        SELECT 
            hpc.e_hash, 
            q.category_id, 
            COUNT(hpc.blog_id) AS cat_count
        FROM news.hp_clicks hpc
        LEFT JOIN news.blogs q ON hpc.blog_id = q.id
        WHERE hpc.created_at >= DATEADD(month, -{LOOKBACK_MONTHS}, CURRENT_DATE)
        AND hpc.choice_selected = 1
        AND q.category_id IS NOT NULL
        GROUP BY hpc.e_hash, q.category_id
    ) a
    GROUP BY a.e_hash;
    """
    
    print(f"Fetching news category stats from Redshift for last {LOOKBACK_MONTHS} months...")
    return GetRedshiftRecords(query)

def GetCurrentUserPreferences():
    """
    Get current user preferences from database
    
    Returns:
        dict: {user_hash: cat_json_string, ...}
        Example: {'user_hash_1': '{"count": 3, "cats": {"1": "2", "2": "5"}}', ...}
    """
    query = f"SELECT user, cat_json FROM {db_name}.preferred_categories;"
    rows = GetRecords(query)

    u_pref = {}
    for row in rows:
        u_pref[row[0]] = row[1]

    print(f"Loaded {len(u_pref)} existing user preferences")
    return u_pref

def CreateNewsUserPreferences():
    """
    Build user category preferences based on news article click history
    
    PREFERENCE FORMAT:
    {
        "count": 3,
        "cats": {
            "1": "2",   # Position 1: Category ID 2
            "2": "5",   # Position 2: Category ID 5
            "3": "3"    # Position 3: Category ID 3
        }
    }
    
    Returns:
        dict: {user_hash: cat_json_string, ...}
    """
    categoryStats = GetNewsCategoryStats()

    processed_users = 0
    u_pref = {}
    
    # Create user category preferences
    for row in categoryStats:
        processed_users += 1
        
        # Skip if category data is None
        if row[1] is None:
            continue
            
        # Split the comma-separated category IDs
        cats = row[1].split(',')
        
        # Limit to top N categories
        cats = cats[:MAX_CATEGORIES_PER_USER]
        
        cat_count = len(cats)

        # Build category JSON structure
        cat_positions = {}
        for i, category_id in enumerate(cats, start=1):
            cat_positions[i] = category_id

        # Build final JSON structure
        preference_json = {
            "count": cat_count,
            "cats": cat_positions
        }

        u_pref[row[0]] = json.dumps(preference_json, indent=4)

    print(f"Created preferences for {processed_users} users")
    return u_pref

def GenerateDeltaUserPreferences(current, created):
    """
    Compare current and new preferences, return only changes
    
    WHY THIS EXISTS:
    - Avoid updating unchanged records
    - Reduce database write operations
    - Track actual preference changes
    
    Args:
        current (dict): Current preferences from database
        created (dict): Newly calculated preferences
        
    Returns:
        tuple: ([delta_records], count)
            delta_records: List of [user_hash, cat_json, timestamp] for changed preferences
            count: Number of changed records
    """
    delta = []
    new_users = 0
    changed_prefs = 0
    
    # Get current timestamp for modified column
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for user_hash in created:
        if user_hash not in current:
            # New user
            delta.append([user_hash, created[user_hash], current_time])
            new_users += 1
        elif json.loads(created[user_hash]) != json.loads(current[user_hash]):
            # Preference changed
            delta.append([user_hash, created[user_hash], current_time])
            changed_prefs += 1

    print(f"Delta: {new_users} new users, {changed_prefs} changed preferences")
    return [delta, len(delta)]

def UpdateCategoriesAndCleanTempTable():
    """
    Update main table from temp table and clean up
    
    USES UPSERT PATTERN:
    - INSERT new records
    - UPDATE existing records
    - Single atomic operation
    """
    # Upsert from temp table to main table
    upsert = f"""
    INSERT INTO {db_name}.preferred_categories (user, cat_json, modified_at)
    SELECT t.user, t.cat_json, t.modified
    FROM {db_name}.preferred_categories_temp t
    ON DUPLICATE KEY UPDATE
        cat_json = t.cat_json,
        modified_at = t.modified
    """

    # Clean temp table
    clean = f"TRUNCATE TABLE {db_name}.preferred_categories_temp;"

    ExecCommand(upsert)
    print("Updated preferred_categories table")
    
    ExecCommand(clean)
    print("Cleaned temp table")

# ============================================================================
# MAIN DRIVER
# ============================================================================

def Driver():
    """
    Main orchestration function
    
    EXECUTION FLOW:
    1. Get current user preferences from database
    2. Calculate new preferences based on news click history
    3. Find delta (changes only)
    4. Insert delta into temp table
    5. Upsert from temp table to main table
    6. Clean up temp table
    """
    print("=" * 70)
    print("Starting News User Preferred Category Analysis")
    print("=" * 70)
    
    try:
        # Step 1: Get current preferences
        currentPref = GetCurrentUserPreferences()
        
        # Step 2: Build new preferences from click data
        builtPref = CreateNewsUserPreferences()
        
        # Step 3: Calculate delta
        deltaValues = GenerateDeltaUserPreferences(currentPref, builtPref)
        
        # Step 4: Insert delta into temp table
        if deltaValues[1] > 0:
            InsertRecords([
                deltaValues[0], 
                deltaValues[1], 
                f'{db_name}.preferred_categories_temp', 
                ['user', 'cat_json', 'modified']
            ])
            
            # Step 5: Update main table from temp table
            UpdateCategoriesAndCleanTempTable()
            
            print(f"Successfully updated {deltaValues[1]} user preferences")
        else:
            print("No changes detected, skipping update")
        
        # Send success notification
        slack_updates(f"NewsUserPreferredCategory.py: Successfully processed {deltaValues[1]} preference updates")
        
    except Exception as e:
        error_msg = f"NewsUserPreferredCategory.py ERROR: {str(e)}"
        print(error_msg)
        slack_updates(error_msg)
        raise
    
    print("=" * 70)
    print("Ended News User Preferred Category Analysis")
    print("=" * 70)

# ============================================================================
# SCRIPT EXECUTION
# ============================================================================

if __name__ == "__main__":
    Driver()
