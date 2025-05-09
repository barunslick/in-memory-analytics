import duckdb
import os
import time
from datetime import timedelta, datetime
import json


FILE_NAME="files/yellow_tripdata_2016-03.csv"

def load_csv_to_duckdb(csv_file, con, memory_limit):
    """
    Load a CSV file into a DuckDB table 
    
    Args:
        csv_file (str): Path to the CSV file to load
        con (duckdb.DuckDBPyConnection): DuckDB connection
        memory_limit (str): Memory limit configuration
        
    Returns:
        tuple: (success, duration, row_count)
    """
    start_time = time.time()
    
    try:
        print("Cleaning up any existing table...")
        con.execute("DROP TABLE IF EXISTS yellow_taxi")

        print(f"Starting to process CSV file with memory limit {memory_limit}...")
        con.execute("""
            CREATE TABLE yellow_taxi AS 
            SELECT * FROM read_csv_auto(?, 
                                        SAMPLE_SIZE=100000,
                                        PARALLEL=TRUE)
        """, [csv_file])
        
        # Get the number of rows loaded
        result = con.execute("SELECT COUNT(1) as row_count FROM yellow_taxi").fetchone()
        row_count = result[0]
        
        duration = time.time() - start_time
        return True, duration, row_count
    except Exception as e:
        duration = time.time() - start_time
        print(f"Error loading data: {e}")
        return False, duration, 0

def analyze_trips_by_hour(con):
    """
    Analyze trip patterns by hour of day
    
    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        
    Returns:
        dict: Hour-by-hour statistics
    """
    print("\nAnalyzing trips by hour of day...")
    start_time = time.time()
    
    # Query to analyze trips by hour of day
    result = con.execute("""
        SELECT 
            EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour_of_day,
            COUNT(*) AS trip_count,
            AVG(trip_distance) AS avg_distance,
            AVG(fare_amount) AS avg_fare,
            AVG(tip_amount) AS avg_tip,
            AVG(tip_amount / NULLIF(fare_amount, 0)) * 100 AS avg_tip_percentage
        FROM yellow_taxi
        GROUP BY hour_of_day
        ORDER BY hour_of_day
    """).fetchall()
    
    duration = time.time() - start_time
    print(f"Analysis completed in {duration:.2f} seconds")
    
    # Format and print results
    print("\nTrips by Hour of Day:")
    print(f"{'Hour':^5}|{'Trip Count':^12}|{'Avg Distance':^15}|{'Avg Fare':^12}|{'Avg Tip':^12}|{'Tip %':^8}")
    print("-" * 70)
    
    hour_data = {}
    for row in result:
        hour, count, distance, fare, tip, tip_pct = row
        hour_data[int(hour)] = {
            "trip_count": int(count),
            "avg_distance": float(distance),
            "avg_fare": float(fare),
            "avg_tip": float(tip),
            "tip_percentage": float(tip_pct) if tip_pct is not None else 0
        }
        print(f"{hour:^5}|{count:^12,}|{distance:^15.2f}|${fare:^11.2f}|${tip:^11.2f}|{tip_pct if tip_pct is not None else 0:^8.1f}")
    
    return hour_data

def analyze_popular_routes(con, limit=10):
    """
    Find the most popular routes by pickup and dropoff locations
    
    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        limit (int): Number of top routes to return
        
    Returns:
        list: Top routes data
    """
    print(f"\nAnalyzing top {limit} popular routes...")
    start_time = time.time()
    
    try:
        # First create a materialized view with rounded coordinates to reduce memory pressure
        print("Creating optimized view for route analysis...")
        con.execute("""
            CREATE OR REPLACE VIEW taxi_routes AS
            SELECT 
                ROUND(pickup_longitude, 2) AS pickup_long,
                ROUND(pickup_latitude, 2) AS pickup_lat,
                ROUND(dropoff_longitude, 2) AS dropoff_long,
                ROUND(dropoff_latitude, 2) AS dropoff_lat,
                trip_distance,
                fare_amount,
                tip_amount,
                total_amount
            FROM yellow_taxi
            WHERE pickup_longitude != 0 AND pickup_latitude != 0
              AND dropoff_longitude != 0 AND dropoff_latitude != 0
        """)
        
        # Now analyze the view with memory-efficient processing
        print("Processing routes in memory-efficient chunks...")
        result = con.execute(f"""
            SELECT 
                pickup_long, pickup_lat, 
                dropoff_long, dropoff_lat,
                COUNT(*) AS trip_count,
                AVG(trip_distance) AS avg_distance,
                AVG(fare_amount) AS avg_fare,
                AVG(total_amount) AS avg_total
            FROM taxi_routes
            GROUP BY pickup_long, pickup_lat, dropoff_long, dropoff_lat
            ORDER BY trip_count DESC
            LIMIT {limit}
        """).fetchall()
        
    except Exception as e:
        print(f"Route analysis error: {e}")
        print("Trying alternative approach with lower precision...")
        
        # If that fails, try with less precision (fewer decimal places)
        result = con.execute(f"""
            SELECT 
                ROUND(pickup_longitude, 1) AS pickup_long,
                ROUND(pickup_latitude, 1) AS pickup_lat,
                ROUND(dropoff_longitude, 1) AS dropoff_long,
                ROUND(dropoff_latitude, 1) AS dropoff_lat,
                COUNT(*) AS trip_count,
                AVG(trip_distance) AS avg_distance,
                AVG(fare_amount) AS avg_fare,
                AVG(total_amount) AS avg_total
            FROM yellow_taxi
            WHERE pickup_longitude != 0 AND pickup_latitude != 0
              AND dropoff_longitude != 0 AND dropoff_latitude != 0
            GROUP BY pickup_long, pickup_lat, dropoff_long, dropoff_lat
            ORDER BY trip_count DESC
            LIMIT {limit}
        """).fetchall()
    
    duration = time.time() - start_time
    print(f"Analysis completed in {duration:.2f} seconds")
    
    print(f"\nTop {limit} Popular Routes:")
    print(f"{'Pickup (long,lat)':^25}|{'Dropoff (long,lat)':^25}|{'Trip Count':^12}|{'Avg Dist':^10}|{'Avg Fare':^10}")
    print("-" * 90)
    
    routes_data = []
    for row in result:
        pickup_long, pickup_lat, dropoff_long, dropoff_lat, count, distance, fare, total = row
        route = {
            "pickup": {"longitude": float(pickup_long), "latitude": float(pickup_lat)},
            "dropoff": {"longitude": float(dropoff_long), "latitude": float(dropoff_lat)},
            "trip_count": int(count),
            "avg_distance": float(distance),
            "avg_fare": float(fare),
            "avg_total": float(total)
        }
        routes_data.append(route)
        
        pickup_str = f"({pickup_long:.3f},{pickup_lat:.3f})"
        dropoff_str = f"({dropoff_long:.3f},{dropoff_lat:.3f})"
        print(f"{pickup_str:^25}|{dropoff_str:^25}|{count:^12,}|{distance:^10.2f}|${fare:^9.2f}")
    
    return routes_data

def analyze_payment_methods(con):
    """
    Analyze usage and tipping behavior by payment method
    
    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        
    Returns:
        dict: Payment method statistics
    """
    print("\nAnalyzing payment methods...")
    start_time = time.time()
    
    # Query to analyze payment methods
    # payment_type: 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided
    result = con.execute("""
        SELECT 
            payment_type,
            CASE payment_type
                WHEN 1 THEN 'Credit card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided'
                ELSE 'Other'
            END AS payment_desc,
            COUNT(*) AS trip_count,
            AVG(fare_amount) AS avg_fare,
            AVG(tip_amount) AS avg_tip,
            SUM(tip_amount) AS total_tips,
            AVG(tip_amount / NULLIF(fare_amount, 0)) * 100 AS avg_tip_percentage
        FROM yellow_taxi
        GROUP BY payment_type, payment_desc
        ORDER BY trip_count DESC
    """).fetchall()
    
    duration = time.time() - start_time
    print(f"Analysis completed in {duration:.2f} seconds")
    
    # Format and print results
    print("\nPayment Method Analysis:")
    print(f"{'Method':^12}|{'Trip Count':^12}|{'Avg Fare':^12}|{'Avg Tip':^12}|{'Total Tips':^15}|{'Tip %':^8}")
    print("-" * 80)
    
    payment_data = {}
    for row in result:
        payment_id, payment_desc, count, fare, tip, total_tips, tip_pct = row
        payment_data[str(payment_id)] = {
            "description": payment_desc,
            "trip_count": int(count),
            "avg_fare": float(fare),
            "avg_tip": float(tip),
            "total_tips": float(total_tips),
            "tip_percentage": float(tip_pct) if tip_pct is not None else 0
        }
        
        print(f"{payment_desc:^12}|{count:^12,}|${fare:^11.2f}|${tip:^11.2f}|${total_tips:^14,.2f}|{tip_pct if tip_pct is not None else 0:^8.1f}")
    
    return payment_data

def analyze_busy_days_and_times(con):
    """
    Analyze busiest days of week and times
    
    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        
    Returns:
        tuple: (day_stats, hour_by_day_stats)
    """
    print("\nAnalyzing busy days and times...")
    start_time = time.time()
    
    day_result = con.execute("""
        SELECT 
            DAYNAME(tpep_pickup_datetime) AS day_name,
            COUNT(*) AS trip_count,
            AVG(trip_distance) AS avg_distance,
            AVG(fare_amount) AS avg_fare,
            AVG(tip_amount) AS avg_tip,
            AVG(trip_distance / 
                (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/3600)) 
                AS avg_speed_mph
        FROM yellow_taxi
        WHERE tpep_dropoff_datetime > tpep_pickup_datetime 
          AND trip_distance > 0
          AND EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 60
        GROUP BY day_name
        ORDER BY 
            CASE day_name
                WHEN 'Monday' THEN 1
                WHEN 'Tuesday' THEN 2
                WHEN 'Wednesday' THEN 3
                WHEN 'Thursday' THEN 4
                WHEN 'Friday' THEN 5
                WHEN 'Saturday' THEN 6
                WHEN 'Sunday' THEN 7
            END
    """).fetchall()
    
    # Heat map for hour of day by day of week
    hour_by_day_result = con.execute("""
        SELECT 
            DAYNAME(tpep_pickup_datetime) AS day_name,
            EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour_of_day,
            COUNT(*) AS trip_count
        FROM yellow_taxi
        GROUP BY day_name, hour_of_day
        ORDER BY 
            CASE day_name
                WHEN 'Monday' THEN 1
                WHEN 'Tuesday' THEN 2
                WHEN 'Wednesday' THEN 3
                WHEN 'Thursday' THEN 4
                WHEN 'Friday' THEN 5
                WHEN 'Saturday' THEN 6
                WHEN 'Sunday' THEN 7
            END,
            hour_of_day
    """).fetchall()
    
    duration = time.time() - start_time
    print(f"Analysis completed in {duration:.2f} seconds")
    print("\nBusy Days Analysis:")
    print(f"{'Day':^10}|{'Trip Count':^12}|{'Avg Distance':^15}|{'Avg Fare':^12}|{'Avg Tip':^12}|{'Avg Speed':^10}")
    print("-" * 80)
    
    day_data = {}
    for row in day_result:
        day, count, distance, fare, tip, speed = row
        day_data[day] = {
            "trip_count": int(count),
            "avg_distance": float(distance),
            "avg_fare": float(fare),
            "avg_tip": float(tip),
            "avg_speed_mph": float(speed) if speed is not None else 0
        }
        
        print(f"{day:^10}|{count:^12,}|{distance:^15.2f}|${fare:^11.2f}|${tip:^11.2f}|{speed if speed is not None else 0:^10.1f}")
    
    # Format hour by day data
    hour_by_day_data = {}
    for row in hour_by_day_result:
        day, hour, count = row
        if day not in hour_by_day_data:
            hour_by_day_data[day] = {}
        hour_by_day_data[day][int(hour)] = int(count)
    
    print("\nBusiest Hours by Day: (Trip counts)")
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    print(f"{'Hour':^6}|{'Mon':^8}|{'Tue':^8}|{'Wed':^8}|{'Thu':^8}|{'Fri':^8}|{'Sat':^8}|{'Sun':^8}")
    print("-" * 65)
    
    for hour in range(24):
        hour_data = [hour_by_day_data.get(day, {}).get(hour, 0) for day in days]
        print(f"{hour:^6}|" + "|".join(f"{count:^8,}" for count in hour_data))
    
    return day_data, hour_by_day_data

def test_large_memory_processing(con):
    """
    Specifically test DuckDB's ability to process data larger than memory
    by forcing complex windowing and joins that require disk spilling
    
    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        
    Returns:
        tuple: (success, duration, result_count)
    """
    print("\n" + "="*50)
    print("TESTING LARGER-THAN-MEMORY PROCESSING")
    print("="*50)
    
    start_time = time.time()
    
    try:
        print("Running complex window functions across the entire dataset...")
        
        # This query specifically tests larger-than-memory capabilities by:
        # 1. Creating a window function across the entire dataset
        # 2. Joining the result back to itself
        # 3. Calculating percentiles which requires sorting the entire dataset
        # 4. Using the ROWS BETWEEN construct which may not use indexes efficiently
        
        result = con.execute("""
            WITH trip_ranks AS (
                SELECT 
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    trip_distance,
                    fare_amount,
                    tip_amount,
                    pickup_longitude, pickup_latitude,
                    dropoff_longitude, dropoff_latitude,
                    -- Complex window function across entire dataset
                    RANK() OVER (
                        PARTITION BY EXTRACT(DAY FROM tpep_pickup_datetime)
                        ORDER BY fare_amount DESC
                    ) as day_fare_rank,
                    -- Moving average requiring buffer of many rows
                    AVG(fare_amount) OVER (
                        ORDER BY tpep_pickup_datetime 
                        ROWS BETWEEN 10000 PRECEDING AND 10000 FOLLOWING
                    ) as moving_avg_fare,
                    -- Percentile calculation requiring full sort
                    NTILE(100) OVER (ORDER BY trip_distance) as distance_percentile
                FROM yellow_taxi
            )
            SELECT 
                distance_percentile,
                ROUND(AVG(trip_distance), 2) as avg_distance,
                ROUND(AVG(fare_amount), 2) as avg_fare,
                ROUND(AVG(tip_amount), 2) as avg_tip,
                COUNT(*) as trip_count
            FROM trip_ranks
            WHERE day_fare_rank <= 1000  -- Still forces processing of all data
            GROUP BY distance_percentile
            ORDER BY distance_percentile
        """).fetchall()
        
        duration = time.time() - start_time
        
        # Print results
        print(f"\nLarger-than-memory processing completed in {timedelta(seconds=duration)} (HH:MM:SS)")
        print(f"Successfully processed {len(result)} percentile groups")
        
        # Print memory information after processing
        mem_info = con.execute("PRAGMA memory_info").fetchall()
        if mem_info:
            print("\nMemory Usage Information:")
            for info in mem_info:
                print(f"- {info[0]}: {info[1]}")
        
        return True, duration, len(result)
        
    except Exception as e:
        duration = time.time() - start_time
        print(f"Error during larger-than-memory test: {e}")
        return False, duration, 0

def run_analytics(con):
    """
    Run all analytics functions and return combined results
    
    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        
    Returns:
        dict: All analytics results
    """
    print("\n" + "="*50)
    print("RUNNING COMPREHENSIVE ANALYTICS")
    print("="*50)
    
    hourly_stats = analyze_trips_by_hour(con)
    popular_routes = analyze_popular_routes(con, limit=10)
    payment_stats = analyze_payment_methods(con)
    day_stats, hourly_day_stats = analyze_busy_days_and_times(con)
    
    success, duration, result_count = test_large_memory_processing(con)
    
    # Combine all analytics into a single result
    analytics_results = {
        "hourly_stats": hourly_stats,
        "popular_routes": popular_routes,
        "payment_stats": payment_stats,
        "day_stats": day_stats,
        "hourly_day_stats": hourly_day_stats,
        "large_memory_test": {
            "success": success,
            "duration_seconds": duration,
            "result_count": result_count
        }
    }
    
    # Create a timestamped filename to avoid overwriting previous results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_filename = f"db/taxi_analytics_{timestamp}.json"
    
    # Save analytics results to JSON file
    with open(json_filename, "w") as f:
        json.dump(analytics_results, f, indent=2)
    
    print(f"\nAnalytics complete. Results saved to {json_filename}")
    return analytics_results

def main(csv_file=None):
    """
    Main function to load and analyze NYC Yellow Taxi data
    
    Args:
        csv_file (str, optional): Path to the CSV file to analyze. 
                                  If not provided, uses the default file.
    """
    # Create directories for database and temp files if they don't exist
    os.makedirs('db', exist_ok=True)
    temp_dir = os.environ.get('DUCKDB_TEMP_DIRECTORY', 'db/temp')
    os.makedirs(temp_dir, exist_ok=True)
    
    # Default CSV file if none is provided
    if csv_file is None:
        print(f"No file specified, using default: {csv_file}")
        return
    
    # Validate file existence
    if not os.path.exists(csv_file):
        print(f"Error: Could not find the file at {csv_file}")
        print("Current working directory:", os.getcwd())
        return
    
    print(f"Loading data from {csv_file}...")
    
    # Create a database name based on the CSV file name
    csv_basename = os.path.basename(csv_file)
    db_name = f"db/taxi_data_{os.path.splitext(csv_basename)[0]}.duckdb"
    print(f"Using database: {db_name}")
    
    # Check if database file already exists
    db_exists = os.path.exists(db_name)
    if db_exists:
        print(f"Found existing database file at {db_name}")
    else:
        print(f"Creating new database file at {db_name}")
    
    con = duckdb.connect(database=db_name)
    
    temp_dir = os.environ.get('DUCKDB_TEMP_DIRECTORY', 'db/temp')
    memory_limit = os.environ.get('DUCKDB_MEMORY_LIMIT', '400MB')
    con.execute(f"SET temp_directory='{temp_dir}'")
    con.execute(f"SET memory_limit='{memory_limit}'")
    
    try:
        success, load_duration, row_count = load_csv_to_duckdb(csv_file, con, memory_limit)
        
        if success:
            print(f"Successfully loaded {row_count:,} rows into the yellow_taxi table")
            print(f"Loading time: {timedelta(seconds=load_duration)} (HH:MM:SS)")
            print(f"Average loading speed: {row_count / load_duration:,.2f} rows/second")
        
        # Print some stats
        result = con.execute("SELECT COUNT(1) as row_count FROM yellow_taxi").fetchone()
        print(f"Successfully loaded {result[0]} rows into the yellow_taxi table")
        
        print("\nTable Schema:")
        schema = con.execute("DESCRIBE yellow_taxi").fetchall()
        for col in schema:
            print(f"- {col[0]}: {col[1]}")
        
        print("\nSample data (5 rows):")
        sample = con.execute("SELECT * FROM yellow_taxi LIMIT 5").fetchall()
        for row in sample:
            print(row)
            
        print("\nDatabase Information:")
        print("Memory Limit:", con.execute("SELECT current_setting('memory_limit')").fetchone()[0])
        
        print("\nDatabase Statistics:")
        stats = con.execute("PRAGMA database_size").fetchall()
        for stat in stats:
            print(f"- {stat[0]}: {stat[1]}")
        
        if input("\nWould you like to run comprehensive analytics? (y/n): ").lower() == 'y':
            try:
                print("\nNote: Analytics may take some time and memory. Individual analyses will continue even if others fail.")
                hourly_stats = analyze_trips_by_hour(con)
                
                try:
                    popular_routes = analyze_popular_routes(con, limit=10)
                except Exception as e:
                    print(f"Route analysis failed: {e}")
                    popular_routes = []
                
                try:
                    payment_stats = analyze_payment_methods(con)
                except Exception as e:
                    print(f"Payment analysis failed: {e}")
                    payment_stats = {}
                
                try:
                    day_stats, hourly_day_stats = analyze_busy_days_and_times(con)
                except Exception as e:
                    print(f"Day/time analysis failed: {e}")
                    day_stats, hourly_day_stats = {}, {}
                
                # Combine all analytics into a single result
                analytics_results = {
                    "hourly_stats": hourly_stats,
                    "popular_routes": popular_routes,
                    "payment_stats": payment_stats,
                    "day_stats": day_stats,
                    "hourly_day_stats": hourly_day_stats
                }
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                json_filename = f"reports/duck_db_taxi_analytics_{timestamp}.json"
                
                with open(json_filename, "w") as f:
                    json.dump(analytics_results, f, indent=2)
                
                print(f"\nAnalytics complete. Results saved to {json_filename}")
            except Exception as e:
                print(f"Error during analytics: {e}")
            
    except Exception as e:
        print(f"Error loading data: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    main(FILE_NAME)

