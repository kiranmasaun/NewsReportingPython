#!/usr/bin/python

import json          # For parsing JSON configuration file
import sys          # System-specific parameters
import mysql.connector  # MySQL database connection
from datetime import date, timedelta, datetime  # Date/time operations
import http.client  # HTTP connections for Slack notifications
import html         # HTML escaping for Slack messages
import psycopg2     # PostgreSQL/Redshift database connection
from psycopg2 import pool  # Connection pooling for Redshift
import csv          # For reading baseline stats CSV files
import os           # For checking if baseline files exist
import time         # For sleep between deadlock retries

# Load database credentials from def.json configuration file
# Expected format: {"db_host": "...", "db_user": "...", "db_pass": "...", "db_name": "...", "redshift_host": "...", ...}
with open('def.json') as f:
  confData = json.load(f)

# Extract MySQL connection parameters (for writes)
db_host = confData['db_host']  # MySQL server hostname/IP
db_user = confData['db_user']  # MySQL username
db_pass = confData['db_pass']  # MySQL password
db_name = confData['db_name']  # MySQL database name/schema

# Extract Redshift connection parameters (for reads)
redshift_host = confData['redshift_host']  # Redshift cluster endpoint
redshift_user = confData['redshift_user']  # Redshift username
redshift_pass = confData['redshift_pass']  # Redshift password
redshift_db = confData['redshift_db']      # Redshift database name
redshift_port = confData['redshift_port']  # Redshift port (usually 5439)

# Redshift connection configuration
redshift_config = {
    "host": redshift_host,
    "user": redshift_user,
    "password": redshift_pass,
    "database": redshift_db,
    "port": redshift_port
}

hp_configs = confData['hp_configs']

# Create Redshift connection pool
try:
    print(f"Connecting to Redshift at {redshift_config['host']}:{redshift_config['port']} with user {redshift_config['user']}")

    # Create connection pool with 1-5 connections
    redshift_pool = pool.SimpleConnectionPool(1, 5, **redshift_config)

    print("Redshift connection pool created successfully.")
except Exception as e:
    print(f"Error connecting to Redshift: {str(e)}")
    # slack_updates(f"DynamicPriority.py: Error connecting to Redshift: {str(e)}")
    redshift_pool = None

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def slack_updates(message):
    """
    Send notification messages to Slack channel for monitoring and alerts.

    HOW IT WORKS:
    1. Connects to Slack webhook URL via HTTPS
    2. Escapes HTML characters in message to prevent formatting issues
    3. Sends POST request with JSON payload
    4. Prints response for logging

    Args:
        message (str or list): The message to send to Slack (string or array)

    Use Cases:
        - Alert when script fails
        - Report summary of nightly run
        - Notify about specific article changes
        - Alert on Redshift connection issues
    """
    # Establish secure connection to Slack's webhook service
    conn = http.client.HTTPSConnection("hooks.slack.com")

    # Convert message to string if it's a list
    if isinstance(message, list):
        message = '\n'.join(str(m) for m in message)

    # Create JSON payload with escaped message text
    payload = json.dumps({
        "text": html.escape(str(message))  # Escape special characters like <, >, &
    })

    # Set HTTP headers to indicate JSON content
    headers = {
        'Content-Type': 'application/json'
    }

    # Send POST request to specific webhook URL
    # Note: This webhook URL is unique to your Slack workspace/channel
    conn.request("POST", "/services/T0L4M9K43/B06H4E1M1FY/L5Nzrre5YDck5FCehfJtoJSv", payload, headers)

    # Get response from Slack
    res = conn.getresponse()
    data = res.read()

    # Print response for logging (usually "ok" if successful)
    print("slack response: ", data.decode("utf-8"))

def GetRecords(query):
    """
    Execute a SELECT query on MySQL and return all results.

    HOW IT WORKS:
    1. Opens new database connection
    2. Executes the SELECT query
    3. Fetches all rows from result set
    4. Closes connection
    5. Returns rows as list of tuples

    Args:
        query (str): SQL SELECT statement to execute

    Returns:
        list: List of tuples, each tuple is a row. Empty list if no results.
        Example: [(1, 'article1', 100), (2, 'article2', 200)]

    Important Notes:
        - Each database call opens a NEW connection (not connection pooling)
        - Connection is closed after query completes
        - Returns empty list if no rows found
    """
    # Open new database connection using credentials from config
    dbo = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        database=db_name
    )

    # Create cursor object to execute queries
    c = dbo.cursor()

    # Execute the SELECT query
    c.execute(query)

    # Fetch all rows from the result set
    rows = c.fetchall()

    # Check if any rows were returned
    if not c.rowcount:
        print("No results found")
        return []  # Return empty list instead of None

    # Close database connection
    dbo.close()

    # Return results as list of tuples
    return rows

def GetRedshiftRecords(query):
    """
    Execute a SELECT query on Redshift and return all results.

    HOW IT WORKS:
    1. Gets connection from Redshift pool
    2. Executes the SELECT query
    3. Fetches all rows from result set
    4. Returns connection to pool
    5. Returns rows as list of tuples

    Args:
        query (str): SQL SELECT statement to execute (Redshift syntax)

    Returns:
        list: List of tuples, each tuple is a row. Empty list if no results.
        Example: [(1, 250, 100000), (2, 180, 75000)]

    Important Notes:
        - Uses connection pooling for efficiency
        - Connection is returned to pool (not closed)
        - Returns empty list if no rows found or pool unavailable
    """
    # Check if Redshift pool is available
    if redshift_pool is None:
        print("Redshift pool is not defined. Cannot fetch records.")
        slack_updates("DynamicPriority.py: Redshift pool is not defined. Cannot fetch records.")
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
        slack_updates(f"DynamicPriority.py: Error fetching records from Redshift: {str(e)}")
        return []

def ExecCommand(query):
    """
    Execute an UPDATE, INSERT, or DELETE query (commands that modify data).

    HOW IT WORKS:
    1. Opens new database connection
    2. Executes the query (UPDATE/INSERT/DELETE)
    3. Commits the transaction to save changes
    4. Closes connection

    Args:
        query (str): SQL command to execute (UPDATE, INSERT, DELETE, etc.)

    Returns:
        None

    Important Notes:
        - COMMITS the transaction, so changes are permanent
        - Does not return result rows (use GetRecords for SELECT)
        - Will raise exception if query fails
    """
    # Open new database connection
    dbo = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        database=db_name
    )

    # Create cursor
    c = dbo.cursor()

    # Execute the command
    c.execute(query)

    # Commit transaction to save changes to database
    dbo.commit()

    # Close connection
    dbo.close()

def RetryExecCommand(q, repeat = 10):
    """
    Execute a command with automatic retry on failure.

    WHY THIS EXISTS:
    Database operations can fail due to:
    - Temporary network issues
    - Database locks/deadlocks
    - Connection timeouts

    This function retries up to 10 times before giving up.

    HOW IT WORKS:
    1. Try to execute command
    2. If successful, return True immediately
    3. If fails, wait and try again
    4. After 10 failures, return False and alert Slack

    Args:
        q (str): SQL query to execute
        repeat (int): Number of retry attempts (default 10)

    Returns:
        bool: True if successful, False if all retries failed
    """
    # Loop through retry attempts
    for x in range(repeat):
        try:
            # Attempt to execute the command
            ExecCommand(q)
            return True  # Success! Exit immediately

        except Exception as e:
            # Command failed, log the failure
            print(f'query failed at iteration {x}: {str(e)}')
            print(f'Query was: {q}')

            # Send alert to Slack about the failure
            slack_updates(f"DynamicPriority.py exception: query failed at iteration {x}: {str(e)}" )

            # Wait before retrying — gives competing transactions time to commit
            # and avoids hammering the same deadlock repeatedly
            time.sleep(2)

    # All retries exhausted, command failed
    return False

# ============================================================================
# DATA INSERTION FUNCTIONS
# ============================================================================

def InsertRecords(data):
    """
    Insert multiple records into database efficiently.

    WHY THIS IS COMPLEX:
    MySQL has a limit on how many placeholders can be in a single query (~990).
    If we need to insert 5,000 records, we must batch them into chunks.

    HOW IT WORKS:
    1. Check if we have records to insert
    2. Build INSERT query with placeholders
    3. If < 990 records: Insert all at once
    4. If >= 990 records: Split into batches and insert iteratively

    Args:
        data (list): [values, length, table, columns]
            - values: List of tuples, each tuple is a row to insert
            - length: Number of records
            - table: Table name to insert into
            - columns: List of column names

    Example:
        data = [
            [(1, 5), (2, 3), (3, 7)],  # values: 3 rows
            3,                          # length: 3 records
            'mydb.priority_rank_temp',  # table name
            ['q_id', 'rank']            # column names
        ]
    """
    # Check if there are any records to insert
    if (data[1] == 0):
        return  # Nothing to do

    # Open database connection
    dbo = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        database=db_name
    )

    # Build column list for INSERT statement
    # Example: 'q_id, rank'
    cols = ', '.join(data[3])

    # Build placeholders for VALUES clause
    # For each column, add a %s placeholder
    # Example: '%s, %s' for 2 columns
    placeholders = []
    for x in data[3]:
        placeholders.append('%s')
    p = ', '.join(placeholders)

    # Build complete INSERT query
    # Example: "INSERT INTO mydb.priority_rank_temp (q_id, rank) VALUES (%s, %s)"
    sql = f"INSERT INTO {data[2]} ({cols}) VALUES ({p})"

    # MySQL parameter limit (max ~1000 placeholders per query)
    mysql_limit_int = 990

    # CASE 1: Small batch - insert all records at once
    if(data[1] < mysql_limit_int):
        c = dbo.cursor()

        try:
            # executemany runs the INSERT for each tuple in data[0]
            # Example: INSERT ... VALUES (1, 5), INSERT ... VALUES (2, 3), etc.
            c.executemany(sql, data[0])

            # Commit to save changes
            dbo.commit()

            print(c.rowcount, "records inserted.")

        except Exception:
            # If anything fails, rollback to undo partial changes
            dbo.rollback()
            print(c.rowcount, "records not inserted, rolling back.")
            slack_updates("DynamicPriority.py exception: " + str(Exception))

    # CASE 2: Large batch - split into chunks
    else:
        temp_a = []      # Temporary array for current batch
        i = 0            # Total record count
        x = 0            # Iteration/batch count

        # Loop through all records
        for r in data[0]:
            i = i+1
            temp_a.append(r)  # Add record to current batch

            # Check if we've reached batch limit OR this is the last record
            if(i % mysql_limit_int == 0 or i == data[1]):
                x = x+1  # Increment batch counter
                c = dbo.cursor()

                try:
                    # Insert current batch
                    c.executemany(sql, temp_a)
                    dbo.commit()

                    print(c.rowcount, "records inserted. Iteration: " + str(x))

                    # Clear batch for next iteration
                    temp_a = []

                except TypeError as e:
                    # If batch fails, rollback and stop
                    dbo.rollback()
                    print(c.rowcount, "incremental issue, rolling back. Iteration: " + str(x))
                    slack_updates("DynamicPriority.py exception: " + str(e))
                    break  # Stop processing remaining batches

    # Close database connection
    dbo.close()

# ============================================================================
# DATA RETRIEVAL FUNCTIONS
# ============================================================================

def GetQuestionCounts():
    """
    Get click and serve counts for ALL articles, merging day 0 baseline with Redshift data.

    WHAT THIS QUERY DOES:
    1. Starts with all articles from blogs table
    2. LEFT JOINs with clicks (last 3 months) - counts unique clicks
    3. LEFT JOINs with allocations (last 3 months) - counts serves
    4. Merges with baseline stats from CSV (day 0 data)
    5. Returns: article_id, click_count, served_count for each article

    WHY REDSHIFT:
    - hp_clicks and q_allocation tables contain millions of rows
    - Redshift is optimized for analytical queries on large datasets
    - Much faster than querying MySQL directly
    - MySQL is used only for writes (updating priority_rank table)

    BASELINE STATS (DAY 0):
    - Loaded from article_baseline_stats.csv if it exists
    - Provides initial clicks/serves for articles before Redshift tracking
    - Automatically merged with Redshift data when article exists in both
    - Articles ONLY in baseline CSV are also processed (baseline data only)
    - This ensures all articles with baseline stats get proper priority assignments

    TIME WINDOW:
    - Only looks at last 3 months of data from Redshift
    - Baseline data is added on top of Redshift data

    Returns:
        list: List of tuples (article_id, click_count, serve_count)
        Example: [(1, 250, 100000), (2, 180, 75000), ...]
    """

    # Load baseline stats from CSV (day 0 data)
    # baseline_file = 'article_baseline_stats.csv'
    # baseline_data = {}

    # if os.path.exists(baseline_file):
    #     print(f"Loading baseline stats from {baseline_file}")
    #     with open(baseline_file, 'r') as f:
    #         reader = csv.DictReader(f)
    #         for row in reader:
    #             baseline_data[row['blog_id']] = {
    #                 'clicks': int(row['Clicks']),
    #                 'serves': int(row['Serves'])
    #             }
    #     print(f"Loaded {len(baseline_data)} baseline article stats")
    # else:
    #     print(f"No baseline file found at {baseline_file}, using only Redshift data")

    # Build Redshift query (note: different syntax from MySQL)
    # DATEADD instead of INTERVAL, COALESCE instead of IFNULL, etc.
    print(f"CID: {confData['CID']}")
    query = f"""
    SELECT
        b.id,
        COALESCE(c.click_count, 0) AS click_count,      -- Use 0 if no clicks
        COALESCE(qa.serve_count, 0) AS served_count     -- Use 0 if no serves
    FROM news.blogs b

    -- LEFT JOIN for CLICKS (last 3 months only)
    LEFT JOIN (
        -- Count clicks per article (hp_clicks only stores actual clicks, not impressions)
        SELECT blog_id, COUNT(blog_id) AS click_count
        FROM news.hp_clicks
        WHERE created_at >= DATEADD(month, -3, CURRENT_DATE)  -- Last 3 months (Redshift syntax)
        AND q_position = 1
        GROUP BY blog_id
    ) c ON b.id = c.blog_id

    -- LEFT JOIN for SERVES (last 3 months only)
    LEFT JOIN (
        -- Count how many times each article was shown/served
        SELECT q_id, COUNT(*) AS serve_count
        FROM news.q_allocation
        WHERE timestamp >= DATEADD(month, -3, CURRENT_DATE)  -- Last 3 months (Redshift syntax)
        AND data_type = 'ret-clickers'  -- Only consider ret-clickers data
        AND q_position = 1
        GROUP BY q_id
    ) qa ON b.id = qa.q_id
    """

    # Execute query on Redshift
    redshift_results = GetRedshiftRecords(query)

    # Merge baseline data with Redshift data
    merged_results = []
    processed_articles = set()  # Track which articles we've processed

    # STEP 1: Process articles from Redshift (merge with baseline if available)
    for row in redshift_results:
        article_id = row[0]
        redshift_clicks = row[1]
        redshift_serves = row[2]

        processed_articles.add(article_id)  # Mark as processed

        # Add baseline stats if they exist for this article
        # if article_id in baseline_data:
        #     total_clicks = redshift_clicks + baseline_data[article_id]['clicks']
        #     total_serves = redshift_serves + baseline_data[article_id]['serves']
        #     print(f"Article {article_id}: Baseline({baseline_data[article_id]['clicks']}c, {baseline_data[article_id]['serves']}s) + Redshift({redshift_clicks}c, {redshift_serves}s) = Total({total_clicks}c, {total_serves}s)")
        # else:
        total_clicks = redshift_clicks
        total_serves = redshift_serves

        merged_results.append((article_id, total_clicks, total_serves))

    # STEP 2: Process articles that are ONLY in baseline CSV (not in Redshift)
    # for article_id, stats in baseline_data.items():
    #     if article_id not in processed_articles:
    #         # Article exists in baseline but not in Redshift - use baseline data only
    #         total_clicks = stats['clicks']
    #         total_serves = stats['serves']
    #         print(f"Article {article_id}: Baseline-only({total_clicks}c, {total_serves}s)")
    #     merged_results.append((article_id, total_clicks, total_serves))

    return merged_results

# ============================================================================
# PRIORITY CALCULATION FUNCTIONS
# ============================================================================

def CalculatePriority():
    """
    Calculate priority level (1-7) for each article based on performance.

    BUSINESS LOGIC:
    The priority system categorizes articles into 7 levels based on:
    1. CTR (Click-Through Rate) = clicks / serves
    2. Total serves (how mature/tested the article is)

    PRIORITY BREAKDOWN:

    Priority 1 - TOP HERO:
        - CTR >= 2.5% (excellent performance)
        - Serves >= 100K (well-tested)
        - These are your best performers

    Priority 2 - MIDTIER HERO:
        - CTR >= 2.2% AND CTR < 2.5% (very good performance)
        - Serves >= 100K
        - Still strong, just below top tier

    Priority 3 - STABLE HERO:
        - CTR >= 1.4% (good performance)
        - Serves >= 50K AND Serves < 100K (moderately tested)
        - Solid mid-tier content

    Priority 4 - EARLY FATIGUE:
        - CTR >= 1.4% AND CTR < 2.2% (decent CTR)
        - Serves >= 100K (heavily tested)
        - Good CTR but getting stale from overuse

    Priority 5 - ALT (Alternative):
        - CTR >= 1.2% AND CTR < 1.4% (marginal performance)
        - Serves >= 50K
        - Backup content, use when needed

    Priority 6 - NEW/IN-TESTING:
        - Serves < 50K (not enough data yet)
        - Give it more exposure to gather data

    Priority 7 - SUPPRESS (Mark Inactive):
        - CTR < 1.2% (poor performance)
        - Serves >= 50K (enough data to be sure)
        - Stop showing this content AND mark as inactive
        - Sets include_question_mail_TN = 0
        - Sets include_question_mail_NC = 0

    Returns:
        list: [priority_array, count]
            - priority_array: List of [article_id, priority] pairs
            - count: Total number of articles processed
    """
    # Get click and serve counts for all articles
    counts = GetQuestionCounts()

    priority_a = []  # Will hold [article_id, priority] pairs
    i = 0            # Counter for total articles processed

    # Loop through each article
    for r in counts:
        # Extract data from query result tuple
        q_id = r[0]    # Article ID
        clicks = r[1]  # Total clicks (last 3 months)
        serves = r[2]  # Total serves (last 3 months)

        # Calculate CTR (Click-Through Rate)
        # If serves = 0, CTR = 0 (avoid division by zero)
        ctr = 0 if serves == 0 else (clicks / serves)

        i = i + 1  # Increment counter

        # ===================================================================
        # PRIORITY DECISION TREE
        # Evaluated in order - first match wins
        # ===================================================================

        # Priority 1 - Top Hero: Best performers
        if (
            (ctr >= 0.04 and 130000 > serves >= 80000) 
            or 
            (ctr >= 0.035 and 500000 > serves >= 130000)
            or
            (ctr >= 0.03 and serves >= 500000)
        ): 
            priority_a.append([q_id, 1])

        # Priority 2 - MidTier Hero: Very good performers
        elif (
            (0.04 > ctr >= 0.035 and 130000 > serves >= 80000) 
            or 
            (0.035 > ctr >= 0.03 and 500000 > serves >= 130000)
            or
            (0.03 > ctr >= 0.025 and serves >= 500000)
        ):
            priority_a.append([q_id, 2])

        # Priority 3 - Stable Hero: Good performers with moderate exposure
        elif (
            (ctr >= 0.025 and 80000 > serves >= 20000) 
            or 
            (0.035 > ctr >= 0.025 and 130000 > serves >= 80000)
        ):
            priority_a.append([q_id, 3])

        # Priority 4 - Early Fatigue: Good CTR but overexposed
        elif (
            (0.03 > ctr >= 0.025 and 500000 > serves >= 130000) 
            or 
            (0.025 > ctr >= 0.02 and serves >= 500000)
        ):
            priority_a.append([q_id, 4])

        # Priority 5 - ALT: Marginal performers
        elif (
            (0.025 > ctr >= 0.005 and 500000 > serves >= 20000)
            or
            (0.02 > ctr >= 0.005 and serves >= 500000)
            or
            (0.1 > ctr >= 0.005 and serves < 20000 and not q_id.startswith("ag"))
        ):
            priority_a.append([q_id, 5])

        # Priority 6 - New/In-Testing: Not enough data yet
        elif (
            (serves < 20000 and q_id.startswith("ag")) # 20K serves, only ag new Articles 
            or
            (ctr >= 0.1 and serves < 20000 and not q_id.startswith("ag")) # 10% CTR, 20K serves, only VP Articles
        ):  # Less than 20K serves
            priority_a.append([q_id, 6])

        # Priority 7 - Suppress: Poor performers (Mark Inactive)
        elif (
            (ctr < 0.005 and serves >= 20000) # 0.5% CTR, 20K+ serves
            or
            (ctr < 0.005 and serves < 20000 and not q_id.startswith("ag")) # 0.5% CTR, 20K serves, only VP Articles
        ):  # < 0.05% CTR, 20K+ serves
            priority_a.append([q_id, 7])

        # Default fallback (should rarely happen)
        # This catches any edge cases not covered above
        else:
            priority_a.append([q_id, 0])  # Priority 0 = undefined

    # Return array of priorities and total count
    return [priority_a, i]

# ============================================================================
# DATABASE UPDATE FUNCTIONS
# ============================================================================

def UpdatePriorityAndCleanTempTable():
    """
    Update main priority table from temp table and clean up.

    WHY USE A TEMP TABLE PATTERN:
    This is a best practice for bulk updates:
    1. Calculate all new priorities
    2. Insert into temp table
    3. Update main table in single transaction
    4. Clean temp table

    This approach:
    - Minimizes lock time on main table
    - Allows rollback if something fails
    - Keeps main table consistent during update

    STEPS:
    1. Insert new articles from blogs table (not in priority_rank yet) with default priority 6
    1b. Insert baseline-only articles from temp table (calculated but not in blogs)
    2. Update existing articles' priorities from temp table
    3. Mark Priority 7 articles as inactive (include_question_mail_TN = 0, include_question_mail_NC = 0)
    4. (Optional) Reinforce manual priority overrides
    5. Truncate temp table for next run
    """

    # STEP 1: Insert new articles that don't exist in priority table yet
    # Default them to priority 6 (New/In-Testing)
    print(f"include_question_mail: {confData['include_question_mail']}")
    insertNonExistant = f"""
            INSERT INTO {db_name}.priority_rank (blog_id, priority, calculated_priority)
            SELECT id, 0, 6
            FROM {db_name}.blogs a
            LEFT JOIN {db_name}.priority_rank r ON a.id = r.blog_id
            WHERE r.blog_id IS NULL;
            """

    # STEP 1b: Insert articles from temp table that don't exist in priority_rank yet
    # This handles baseline-only articles that were calculated but not in blogs table
    insertFromTempTable = f"""
            INSERT INTO {db_name}.priority_rank (blog_id, priority, calculated_priority, modified_at)
            SELECT t.q_id, 0, t.rank, t.modified_date
            FROM {db_name}.priority_rank_temp t
            LEFT JOIN {db_name}.priority_rank r ON t.q_id = r.blog_id
            WHERE r.blog_id IS NULL;
            """

    # STEP 2: Update main priority table from temp table
    # This updates calculated_priority for all articles we just processed
    update = f"""
        UPDATE {db_name}.priority_rank p
        INNER JOIN {db_name}.priority_rank_temp t ON (t.q_id = p.blog_id)
        SET p.calculated_priority = t.rank, p.modified_at = t.modified_date;
        """

    # STEP 3: Mark Priority 7 articles as inactive
    # Articles with CTR < 1.2% and serves >= 50K should not be served
    markInactive = f"""
        UPDATE {db_name}.priority_rank
        SET {confData['include_question_mail']} = 0
        WHERE calculated_priority = 7
        AND JSON_CONTAINS(brand_assignment, JSON_QUOTE('{confData['brand']}'));
        """
    
    # STEP 4: Mark articles as active other than Priority 6 and 7
    # if calculated_priority is shifted from 6 or 7 to other value then it should active i.e. from 0 to 1
    # NOTE: 6 priority has many Articles which are currently disabled so we need to take care of those as well that's why I added 6 in this condition
    markActive = f"""
        UPDATE {db_name}.priority_rank
        SET {confData['include_question_mail']} = 1
        WHERE calculated_priority NOT IN (6,7)
        AND JSON_CONTAINS(brand_assignment, JSON_QUOTE('{confData['brand']}'));
        """

    # STEP 4 (COMMENTED OUT): Reinforce manual priority overrides
    # If admin manually set priority=1, keep it regardless of calculation
    # Currently disabled - uncomment if you want manual overrides
    updateReinforceOldPriority = f"""
        UPDATE {db_name}.priority_rank
        SET calculated_priority = 1
        WHERE priority = 1
    """

    # STEP 5: Clear temp table for next run
    clean = f"TRUNCATE table {db_name}.priority_rank_temp;"

    # Execute all steps with retry logic
    RetryExecCommand(insertNonExistant)
    print("inserted non existant questions from blogs table")

    RetryExecCommand(insertFromTempTable)
    print("inserted baseline-only articles from temp table")

    RetryExecCommand(update)
    print("updated priorities")

    RetryExecCommand(markInactive)
    print("marked priority 7 articles as inactive")

    RetryExecCommand(markActive)
    print("marked articles as active other than priority 6 and 7")

    # Uncomment if you want to enforce manual overrides:
    #RetryExecCommand(updateReinforceOldPriority)
    #print("updated reinforced priority")

    RetryExecCommand(clean)
    print("cleaned temp table")

# ============================================================================
# MAIN DRIVER
# ============================================================================

def Driver():
    """
    Main orchestration function - runs the entire priority calculation workflow.

    EXECUTION FLOW:
    1. Calculate priorities for all articles
    2. Insert calculated priorities into temp table
    3. Update main priority table from temp table
    4. Clean up temp table

    This function is called automatically when script runs.
    """
    print("starting Dynamic Priority")
    try:
        print(hp_configs)
        for x in hp_configs:
            print(f"working on campaign {x}")

            # Set Brands
            confData['brand'] = 'NC'
            if (x == 'toplinenews'):
                confData['brand'] = 'TN'

            print(f"working on Brand {confData['brand']}")

            # Get column name and CID project specific
            confData['include_question_mail'] = hp_configs[x]['include_question_mail']
            confData['CID'] = hp_configs[x]['hp_campaign_id']

            # Step 1: Calculate priorities
            # Returns: [[article_id, priority], [article_id, priority], ...], total_count
            p = CalculatePriority()

            # Step 2: Insert priorities into temp table
            # Format: [data_array, count, table_name, column_names]
            InsertRecords([
                p[0],                                    # Priority data: [[q_id, rank], ...]
                p[1],                                    # Count of records
                f'{db_name}.priority_rank_temp',        # Temp table name
                ['`q_id`', '`rank`']                    # Column names
            ])

            # Step 3: Update main table and clean up
            UpdatePriorityAndCleanTempTable()

    except Exception as e:
        print(f"DynamicPriority.py exception: {str(e)}")
        slack_updates(f"DynamicPriority.py exception: {str(e)}")
    finally:
        if redshift_pool:
            try:
                redshift_pool.closeall()
                print("Redshift connection pool closed")
            except Exception as e:
                print(f"Error closing Redshift pool: {str(e)}")

    print("Ended Dynamic Priority")

# ============================================================================
# SCRIPT EXECUTION
# ============================================================================

# Run the driver when script is executed
# This makes the script run automatically when called from command line
Driver()
