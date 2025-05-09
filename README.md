# In-Memory Analytics Testing

This repository explores and compares different in-memory analytical processing options, with a focus on handling datasets that are larger than available memory.

## Approaches Compared

The repository includes implementations and benchmarks for:

- [DuckDB](https://duckdb.org/) for in-memory analytics
- [Polars](https://pola.rs/) (next to be tested)


## Dataset

The project uses NYC Yellow Taxi Trip 2016 data as a test dataset, which provides a realistic medium-sized dataset for testing analytical performance.
[Dataset available on Kaggle](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)

## Project Structure

- `src/` - Source code for different analytical implementations
- `db/` - Database files for persistent storage during analysis
- `files/` - Input/output data files, including the test datasets
- `reports/` - Runtime reports generated after running analytics

## Memory Limitations

This project uses Docker Compose to control memory limits for testing how different analytical tools perform under memory constraints:

- Container memory limit: 350MB (`mem_limit: 350m`)

You can update it as you see fit.

## Getting Started

1. Clone the repository
2. Run the setup script to create necessary directories:
   ```bash
   ./setup.sh
   ```
3. Download the [NYC Yellow Taxi 2016 dataset](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data) and extract it.
4. Place your extracted data files in the `files/` directory
5. See individual implementation examples in the `src/` directory

## Running Tests

To run tests with the memory constraints:

```bash
# Start the Docker container with memory limits
docker compose build 

# Execute a specific test
docker compose run analytics

# View the results
cat reports/<tool>_taxi_analytics_<run_datetime>_results.json
```

## Testing Your Own Data

To test with your own data:
1. Place your data files in the `files/` directory
2. Update the FILE_NAME in duck_db.py (Will come with something better later)
3. Run the benchmarks and compare results

---
References:
- [DuckDB Memory Management](https://duckdb.org/2024/07/09/memory-management.html)
