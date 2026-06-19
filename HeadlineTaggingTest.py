#!/usr/bin/python
"""
HeadlineTagging.py - Evaluate and rank article headlines based on performance.
Uses batch queries and connection pooling for efficient database access.
"""

import json
import sys
import mysql.connector
from mysql.connector import pooling
from datetime import date, timedelta, datetime
import http.client
import html
import psycopg2
from psycopg2 import pool
import csv
import os

# Load database configuration from def.json
with open('def.json') as f:
  confData = json.load(f)

# Extract connection parameters
db_host = confData['db_host']
db_user = confData['db_user']
db_pass = confData['db_pass']
db_name = confData['db_name']

redshift_host = confData['redshift_host']
redshift_user = confData['redshift_user']
redshift_pass = confData['redshift_pass']
redshift_db = confData['redshift_db']
redshift_port = confData['redshift_port']

mysql_config = {
    "host": db_host,
    "user": db_user,
    "password": db_pass,
    "database": db_name
}

redshift_config = {
    "host": redshift_host,
    "user": redshift_user,
    "password": redshift_pass,
    "database": redshift_db,
    "port": redshift_port
}

hp_configs = confData['hp_configs']

# Initialize MySQL connection pool (reuses connections for better performance)
try:
    print(f"Creating MySQL connection pool for {mysql_config['host']}")
    mysql_pool = pooling.MySQLConnectionPool(
        pool_name="mysql_pool",
        pool_size=10,
        pool_reset_session=True,
        **mysql_config
    )
    print("MySQL connection pool created successfully.")
except Exception as e:
    print(f"Error creating MySQL connection pool: {str(e)}")
    # slack_updates(f"HeadlineRanking.py: Error creating MySQL pool: {str(e)}")
    mysql_pool = None

# Initialize Redshift connection pool
try:
    print(f"Connecting to Redshift at {redshift_config['host']}:{redshift_config['port']} with user {redshift_config['user']}")
    redshift_pool = pool.SimpleConnectionPool(1, 5, **redshift_config)
    print("Redshift connection pool created successfully.")
except Exception as e:
    print(f"Error connecting to Redshift: {str(e)}")
    # slack_updates(f"HeadlineRanking.py: Error connecting to Redshift: {str(e)}")
    redshift_pool = None

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def slack_updates(message):
    """Send notification to Slack channel."""
    try:
        conn = http.client.HTTPSConnection("hooks.slack.com")
        if isinstance(message, list):
            message = '\n'.join(str(m) for m in message)
        
        payload = json.dumps({"text": html.escape(str(message))})
        headers = {'Content-Type': 'application/json'}
        conn.request("POST", "/services/T0L4M9K43/B06H4E1M1FY/L5Nzrre5YDck5FCehfJtoJSv", payload, headers)
        
        res = conn.getresponse()
        data = res.read()
        print("slack response: ", data.decode("utf-8"))
    except Exception as e:
        print(f"Error sending Slack notification: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

def GetRecords(query, params=None):
    """Execute SELECT query on MySQL using connection pool."""
    if mysql_pool is None:
        print("MySQL pool is not defined. Cannot fetch records.")
        return []
    
    dbo = None
    try:
        dbo = mysql_pool.get_connection()
        c = dbo.cursor()
        c.execute(query, params) if params else c.execute(query)
        rows = c.fetchall()
        c.close()
        return rows if rows else []
    except Exception as e:
        print(f"Error fetching MySQL records: {str(e)}")
        return []
    finally:
        if dbo:
            dbo.close()

def GetRecordsWithParams(query, params):
    """Backward compatibility wrapper for GetRecords()."""
    return GetRecords(query, params)

def GetRedshiftRecords(query, params=None):
    """Execute SELECT query on Redshift using connection pool."""
    if redshift_pool is None:
        print("Redshift pool is not defined. Cannot fetch records.")
        slack_updates("HeadlineRanking.py: Redshift pool is not defined.")
        return []

    dbo = None
    try:
        dbo = redshift_pool.getconn()
        cursor = dbo.cursor()
        cursor.execute(query, params) if params else cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows if rows else []
    except Exception as e:
        print(f"Error fetching records from Redshift: {str(e)}")
        slack_updates(f"HeadlineRanking.py: Error fetching from Redshift: {str(e)}")
        return []
    finally:
        if dbo:
            redshift_pool.putconn(dbo)

def GetRedshiftRecordsWithParams(query, params):
    """Backward compatibility wrapper for GetRedshiftRecords()."""
    return GetRedshiftRecords(query, params)

def ExecCommand(query):
    """Execute UPDATE/INSERT/DELETE on MySQL."""
    if mysql_pool is None:
        print("MySQL pool is not defined. Cannot execute command.")
        return
    
    dbo = None
    try:
        dbo = mysql_pool.get_connection()
        c = dbo.cursor()
        c.execute(query)
        dbo.commit()
        c.close()
    except Exception as e:
        print(f"Error executing MySQL command: {str(e)}")
        if dbo:
            dbo.rollback()
        raise
    finally:
        if dbo:
            dbo.close()

def RetryExecCommand(q, repeat = 10):
    """Execute command with automatic retry on failure (handles locks/timeouts)."""
    for x in range(repeat):
        try:
            ExecCommand(q)
            return True
        except Exception:
            print('query failed at iteration ', x, q)
            slack_updates(f"HeadlineRanking.py exception: query failed at iteration {x} {q}" )
    return False

# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================

def GetArticlesForHeadlineTesting():
    """Get articles in testing phase (calculated_priority = 6)."""
    query = f"""
    SELECT DISTINCT blog_id
    FROM {db_name}.priority_rank
    WHERE calculated_priority = 6
    AND {confData['include_question_mail']} = 1
    """
    results = GetRecords(query)
    return [r[0] for r in results]

def GetAllArticleStatsFromRedshift(article_ids):
    """
    Fetch article and headline statistics for all articles in a single query.
    Returns serves/clicks data aggregated by article and headline index.
    """
    if not article_ids:
        return {}
    
    article_ids_str = ','.join(f"'{aid}'" for aid in article_ids)
    print(f"Fetching stats for all {len(article_ids)} articles in ONE batch query...")
    batch_query = f"""
    SELECT 
        q_id,
        headline_index,
        COUNT(*) AS serves,
        SUM(CASE WHEN is_click = 1 THEN 1 ELSE 0 END) AS clicks
    FROM (
        -- Get all serves
        SELECT q_id, headline_index, 0 AS is_click
        FROM news.q_allocation
        WHERE q_id IN ({article_ids_str}) AND headline_index IS NOT NULL AND data_type = 'ret-clickers'
        
        UNION ALL
        
        -- Get all clicks
        SELECT blog_id, choice_selected AS headline_index, 1 AS is_click
        FROM news.hp_clicks
        WHERE blog_id IN ({article_ids_str}) AND choice_selected IS NOT NULL
    ) combined
    GROUP BY q_id, headline_index
    ORDER BY q_id, headline_index
    """
    
    results = GetRedshiftRecords(batch_query)
    
    if not results:
        print(f"Batch query complete: 0 articles have data in Redshift (this is normal if articles are new)")
        return {}
    
    stats_dict = {}
    for row in results:
        article_id, headline_index, serves, clicks = row
        if article_id not in stats_dict:
            stats_dict[article_id] = {'serves': 0, 'clicks': 0, 'headlines': {}}
        stats_dict[article_id]['headlines'][headline_index] = {'serves': serves, 'clicks': clicks}
        stats_dict[article_id]['serves'] += serves
        stats_dict[article_id]['clicks'] += clicks
    
    print(f"Batch query complete: {len(stats_dict)} articles have data in Redshift")
    return stats_dict

def GetAllHeadlineMetadata(article_ids):
    """
    Fetch headline metadata (text, IDs, active status) for all articles in a single query.
    Returns data grouped by article ID for efficient lookup.
    """
    if not article_ids:
        return {}
    
    article_ids_str = ','.join(f"'{aid}'" for aid in article_ids)
    print(f"Fetching headline metadata for all {len(article_ids)} articles in ONE batch query...")
    batch_query = f"""
    SELECT blog_id, headline_id, headline_index, headline, is_active
    FROM {db_name}.blog_headlines_test
    WHERE blog_id IN ({article_ids_str})
    ORDER BY blog_id, headline_index ASC
    """
    
    results = GetRecords(batch_query)
    
    if not results:
        print(f"Batch query complete: 0 articles have headline metadata (check if article IDs are correct)")
        return {}
    
    metadata_dict = {}
    for row in results:
        blog_id, headline_id, headline_index, headline_text, is_active = row
        if blog_id not in metadata_dict:
            metadata_dict[blog_id] = []
        metadata_dict[blog_id].append({
            'headline_id': headline_id,
            'headline_index': headline_index,
            'headline_text': headline_text,
            'is_active': is_active
        })
    
    print(f"Batch query complete: {len(metadata_dict)} articles have headline metadata")
    return metadata_dict

def GetHeadlineStats(article_id, redshift_stats=None, headlines_metadata=None):
    """
    Get comprehensive stats for an article and its headlines.
    Merges: Redshift data + MySQL metadata + baseline CSV data.
    Accepts pre-fetched batch data for optimal performance.
    """
    # if baseline_data is None:
    #     baseline_data = {}
    
    # Get Redshift stats (use batch data if available, else fallback to individual query)
    if redshift_stats and article_id in redshift_stats:
        # Use pre-fetched batch data (FAST - no query)
        article_redshift = redshift_stats[article_id]
        redshift_article_serves = article_redshift['serves']
        redshift_article_clicks = article_redshift['clicks']
        redshift_stats_dict = article_redshift['headlines']
    else:
        # Fallback: query individually (SLOW - backward compatible)
        article_query = f"""
        SELECT 
            %s AS article_id,
            COALESCE((SELECT COUNT(*) FROM news.q_allocation WHERE q_id = %s AND data_type = 'ret-clickers'), 0) AS total_serves,
            COALESCE((SELECT COUNT(blog_id) FROM news.hp_clicks WHERE blog_id = %s), 0) AS total_clicks
        """
        
        article_result = GetRedshiftRecordsWithParams(article_query, (article_id, article_id, article_id))
        
        if not article_result:
            return None  # Article not found
        
        redshift_article_serves = article_result[0][1]
        redshift_article_clicks = article_result[0][2]
        
        headline_stats_query = f"""
        SELECT 
            qa.cid,
            COALESCE(qa.served_count, 0) AS headline_serves,
            COALESCE(c.click_count, 0) AS headline_clicks
        FROM (
            SELECT cid, COUNT(*) AS served_count
            FROM news.q_allocation
            WHERE q_id = %s AND cid IS NOT NULL AND data_type = 'ret-clickers'
            AND q_position = 1
            GROUP BY cid
        ) qa
        LEFT JOIN (
            SELECT cid, choice_selected AS headline_index, COUNT(blog_id) AS click_count
            FROM news.hp_clicks
            WHERE blog_id = %s AND choice_selected IS NOT NULL
            AND q_position = 1
            GROUP BY choice_selected, cid
        ) c ON qa.cid = c.cid
        """
        headline_stats = GetRedshiftRecordsWithParams(headline_stats_query, (article_id, article_id))

        redshift_stats_dict = {row[0]: {'serves': row[1], 'clicks': row[2]} for row in headline_stats}
    
    # Get headline metadata (use batch data if available, else fallback to individual query)
    if headlines_metadata and article_id in headlines_metadata:
        # Use pre-fetched batch data (FAST - no query)
        headlines_meta = headlines_metadata[article_id]
    else:
        # Fallback: query individually (SLOW - backward compatible)
        headlines_meta_query = f"""
        SELECT headline_id, headline_index, headline, is_active
        FROM {db_name}.blog_headlines_test
        WHERE blog_id = %s
        ORDER BY headline_index ASC
        """
        results = GetRecordsWithParams(headlines_meta_query, (article_id,))
        headlines_meta = [
            {'headline_id': row[0], 'headline_index': row[1], 'headline_text': row[2], 'is_active': row[3]}
            for row in results
        ]
    
    if not headlines_meta:
        return {
            'article_id': article_id,
            'article_serves': redshift_article_serves,
            'article_clicks': redshift_article_clicks,
            'headlines': []
        }
    
    # Merge all data sources: metadata + Redshift + baseline CSV
    headlines = []
    # baseline_article_serves = 0
    # baseline_article_clicks = 0
    
    for meta in headlines_meta:
        headline_id = meta['headline_id']
        headline_index = meta['headline_index']
        headline_text = meta['headline_text']
        is_active = meta['is_active']
        
        # Get stats from each source
        redshift_stats_entry = redshift_stats_dict.get(str(headline_index), {'serves': 0, 'clicks': 0})
        print(f"redshift_stats_entry: {redshift_stats_entry}")
        # baseline_stats = baseline_data.get(headline_id, {'serves': 0, 'clicks': 0})
        
        # Merge baseline + Redshift
        # total_serves = redshift_stats_entry['serves'] + baseline_stats['serves']
        # total_clicks = redshift_stats_entry['clicks'] + baseline_stats['clicks']
        total_serves = redshift_stats_entry['serves'] 
        total_clicks = redshift_stats_entry['clicks']
        
        # baseline_article_serves += baseline_stats['serves']
        # baseline_article_clicks += baseline_stats['clicks']
        
        # if baseline_stats['serves'] > 0:
        #     print(f"Article {article_id}, Headline {headline_id} (index {headline_index}): Baseline({baseline_stats['clicks']}c, {baseline_stats['serves']}s) + Redshift({redshift_stats_entry['clicks']}c, {redshift_stats_entry['serves']}s) = Total({total_clicks}c, {total_serves}s)")
        
        headlines.append({
            'headline_id': headline_id,
            'headline_index': headline_index,
            'headline_text': headline_text,
            'is_active': is_active,
            'serves': total_serves,
            'clicks': total_clicks,
            'ctr': total_clicks / total_serves if total_serves > 0 else 0
        })
    
    # Calculate article totals (Redshift + baseline)
    # article_serves = redshift_article_serves + baseline_article_serves
    # article_clicks = redshift_article_clicks + baseline_article_clicks
    article_serves = redshift_article_serves
    article_clicks = redshift_article_clicks
    
    # if baseline_article_serves > 0:
    #     print(f"Article {article_id} totals: Baseline({baseline_article_clicks}c, {baseline_article_serves}s) + Redshift({redshift_article_clicks}c, {redshift_article_serves}s) = Total({article_clicks}c, {article_serves}s)")
    
    headlines.sort(key=lambda x: x['serves'], reverse=True)
    
    return {
        'article_id': article_id,
        'article_serves': article_serves,
        'article_clicks': article_clicks,
        'headlines': headlines
    }

# ============================================================================
# HEADLINE EVALUATION
# ============================================================================

def EvaluateHeadlines(article_stats):
    """
    Analyze headline performance and determine actions (WDS-1361).
    Thresholds: 10K serves per headline, 50K total, 1.2% CTR winner, 3% CTR early winner.
    """
    article_id = article_stats['article_id']
    article_serves = article_stats['article_serves']
    headlines = article_stats['headlines']
    
    actions = {
        'article_id': article_id,
        'set_inactive': [],
        'keep_active': [],
        'suppress_article': False
    }
    
    CTR_WINNER_THRESHOLD = 0.005
    # CTR_CLEAR_WINNER_THRESHOLD = 0.03
    HEADLINE_SERVES_THRESHOLD = 4500
    # ARTICLE_SERVES_THRESHOLD = 9000
    
    active_headlines = [h for h in headlines if h['is_active']]
    
    if len(active_headlines) <= 1:
        return actions
    
    sorted_headlines = sorted(active_headlines, key=lambda x: x['ctr'], reverse=True)
    best_headline = sorted_headlines[0]
    all_headlines_tested = all(h['serves'] >= HEADLINE_SERVES_THRESHOLD for h in active_headlines)
   
    # if not all_headlines_tested and article_serves < ARTICLE_SERVES_THRESHOLD:
    if not all_headlines_tested:
        return actions
    
    # clear_winners = [h for h in sorted_headlines if h['ctr'] >= CTR_CLEAR_WINNER_THRESHOLD]
    # viable_headlines = [h for h in sorted_headlines 
    #                    if h['ctr'] >= CTR_WINNER_THRESHOLD and h['ctr'] < CTR_CLEAR_WINNER_THRESHOLD]
    viable_headlines = [h for h in sorted_headlines 
                       if h['ctr'] >= CTR_WINNER_THRESHOLD]
    
    failed_headlines = [h for h in sorted_headlines if h['ctr'] < CTR_WINNER_THRESHOLD]
    
    # Scenario 1: Full test complete (article serves >= 50K)
    # if article_serves >= ARTICLE_SERVES_THRESHOLD:
    #     if len(clear_winners) > 0 or len(viable_headlines) > 0:
    #         actions['keep_active'] = [best_headline['headline_id']]
    #         actions['set_inactive'] = [h['headline_id'] for h in sorted_headlines 
    #                                    if h['headline_id'] != best_headline['headline_id']]
    #         return actions
    #     else:
    #         actions['set_inactive'] = [h['headline_id'] for h in active_headlines]
    #         actions['suppress_article'] = True
    #         return actions
    
    # Scenario 2: Clear winner (CTR >= 3%)
    # if len(clear_winners) > 0 and article_serves < ARTICLE_SERVES_THRESHOLD:
    #     actions['keep_active'] = [best_headline['headline_id']]
    #     actions['set_inactive'] = [h['headline_id'] for h in sorted_headlines 
    #                                if h['headline_id'] != best_headline['headline_id']]
    #     return actions
    
    # Scenario 3: Multiple viable options (2+ with 1.2% <= CTR < 3%)
    # if len(viable_headlines) >= 2 and article_serves < ARTICLE_SERVES_THRESHOLD:
    #     actions['set_inactive'] = [h['headline_id'] for h in failed_headlines]
    #     return actions
    
    # Scenario 4: Single winner (1 with 1.2% <= CTR < 3%)
    # if len(viable_headlines) == 1 and article_serves < ARTICLE_SERVES_THRESHOLD:
        # actions['keep_active'] = [viable_headlines[0]['headline_id']]
        # actions['set_inactive'] = [h['headline_id'] for h in sorted_headlines 
        #                            if h['headline_id'] != viable_headlines[0]['headline_id']]
        # return actions

    # if len(viable_headlines) > 0 and article_serves < ARTICLE_SERVES_THRESHOLD:
    if len(viable_headlines) > 0:
        actions['keep_active'] = [best_headline['headline_id']]
        actions['set_inactive'] = [h['headline_id'] for h in sorted_headlines 
                                    if h['headline_id'] != best_headline['headline_id']]
        return actions
    
    # Scenario 5: All failed (all CTR < 1.2%)
    # if len(failed_headlines) == len(active_headlines) and article_serves < ARTICLE_SERVES_THRESHOLD:
    if len(failed_headlines) == len(active_headlines):
        actions['set_inactive'] = [h['headline_id'] for h in active_headlines]
        actions['suppress_article'] = True
        return actions
    
    return actions

def ProcessHeadlineRanking():
    """
    Main processing loop: evaluates all articles in testing and applies actions.
    Returns: [processed_count, winner_count, suppressed_count]
    """
    article_ids = GetArticlesForHeadlineTesting()
    
    if len(article_ids) == 0:
        print("No articles with priority=6 found")
        return [0, 0, 0]
    
    print(f"Found {len(article_ids)} articles for headline testing")
    
    # Load baseline stats CSV once (contains day-0 data before Redshift tracking)
    # baseline_file = 'headline_baseline_stats.csv'
    # baseline_data = {}
    
    # if os.path.exists(baseline_file):
    #     print(f"Loading headline baseline stats from {baseline_file}")
    #     try:
    #         with open(baseline_file, 'r') as f:
    #             reader = csv.DictReader(f)
    #             baseline_data = {
    #                 row['headline_id']: {'clicks': int(row['Clicks']), 'serves': int(row['Serves'])}
    #                 for row in reader
    #             }
    #         print(f"Loaded {len(baseline_data)} baseline headline stats")
    #     except Exception as e:
    #         print(f"Error loading baseline file: {str(e)}")
    #         baseline_data = {}
    # else:
    #     print(f"No baseline file found at {baseline_file}, using only Redshift data")
    
    # Fetch all data upfront in batch queries
    redshift_stats = GetAllArticleStatsFromRedshift(article_ids)
    headlines_metadata = GetAllHeadlineMetadata(article_ids)
    
    processed_count = 0
    winner_count = 0
    suppressed_count = 0
    
    # Process each article using pre-fetched data
    for article_id in article_ids:
        try:
            article_stats = GetHeadlineStats(article_id, redshift_stats, headlines_metadata)
            
            if not article_stats:
                continue
            
            actions = EvaluateHeadlines(article_stats)
            
            # Apply actions to database
            if actions['set_inactive']:
                inactive_ids = ', '.join(f"'{id}'" for id in actions['set_inactive'])
                update_inactive_query = f"""
                UPDATE {db_name}.blog_headlines_test
                SET is_active = 0, updated_at = NOW()
                WHERE headline_id IN ({inactive_ids})
                """
                RetryExecCommand(update_inactive_query)
            
            if actions['keep_active']:
                active_ids = ', '.join(f"'{id}'" for id in actions['keep_active'])
                update_active_query = f"""
                UPDATE {db_name}.blog_headlines_test
                SET is_active = 1, updated_at = NOW()
                WHERE headline_id IN ({active_ids})
                """
                RetryExecCommand(update_active_query)
                winner_count += 1
            
            if actions['suppress_article']:
                suppress_query = f"""
                UPDATE {db_name}.priority_rank_test
                SET calculated_priority = 7, 
                    {confData['include_question_mail']} = 0,
                    modified_at = NOW()
                WHERE blog_id = {article_id}
                """
                RetryExecCommand(suppress_query)
                suppressed_count += 1
                slack_updates(f"HeadlineRanking: Article {article_id} suppressed (priority=7)")
            
            if actions['set_inactive'] or actions['keep_active'] or actions['suppress_article']:
                processed_count += 1
                
        except Exception as e:
            print(f"Error processing article {article_id}: {str(e)}")
            slack_updates(f"HeadlineRanking.py: Error processing article {article_id}: {str(e)}")
            continue
    
    return [processed_count, winner_count, suppressed_count]

# ============================================================================
# MAIN DRIVER
# ============================================================================

def Driver():
    """Main orchestration - runs entire headline ranking workflow."""
    print("Starting Headline Ranking")
    
    try:
        print(hp_configs)
        for x in hp_configs:
            print(f"working on campaign {x}")

            # Get column name project specific
            confData['include_question_mail'] = hp_configs[x]['include_question_mail']
        
            # Operation handling
            results = ProcessHeadlineRanking()
            print(f'results: {results}')
            processed_count, winner_count, suppressed_count = results
            
            print(f"Processed {processed_count} articles, {winner_count} winners, {suppressed_count} suppressed")
            
            if processed_count > 0:
                slack_updates(f"HeadlineRanking.py: Processed {processed_count} articles, {winner_count} winners, {suppressed_count} suppressed")
        
    except Exception as e:
        print(f"HeadlineRanking.py exception: {str(e)}")
        slack_updates(f"HeadlineRanking.py exception: {str(e)}")
    finally:
        if mysql_pool:
            print("MySQL connection pool cleanup complete")
        if redshift_pool:
            try:
                redshift_pool.closeall()
                print("Redshift connection pool closed")
            except Exception as e:
                print(f"Error closing Redshift pool: {str(e)}")
    
    print("Ended Headline Ranking")

Driver()
