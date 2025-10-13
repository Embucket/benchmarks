# Snowplow Event Data Generator

This script generates synthetic Snowplow event data for testing purposes. It can generate data based on either the number of rows or target file size in GB.

## Features

- **Size-based generation**: Generate data to reach a specific file size in GB
- **Row-based generation**: Generate a specific number of rows (original behavior)
- **Automatic size estimation**: Calculates row size and generates data efficiently in batches
- **Progress monitoring**: Shows progress during large data generation
- **Two output files**:
  - `events_yesterday.csv`: Events for yesterday only
  - `events_today.csv`: Combined events for yesterday + today

## Usage

### Generate by size (GB)

```bash
# Generate 0.5 GB of data per day
python gen_events.py --gb 0.5

# Generate 2 GB of data per day
python gen_events.py --gb 2

# Generate 10 GB of data per day
python gen_events.py --gb 10
```

### Generate by row count

```bash
# Generate 10,000 rows per day
python gen_events.py --rows 10000

# Backward compatibility (same as --rows)
python gen_events.py 10000
```

### Help

```bash
python gen_events.py --help
```

## How it works

### Size-based generation (`--gb`)

1. Generates a small sample (100 rows) to calculate average row size
2. Estimates how many rows are needed to reach the target size
3. Generates events in batches of 10,000 rows
4. Shows progress every 50,000 rows
5. Reports final file size

**Example output:**
```
Mode: Size-based generation
Target size per day: 1 GB
Estimated row size: 1410.08 bytes
Estimated rows needed for 1 GB: 760,209
Generating events in batches of 10,000...
  Progress: 50,000 rows, 0.067 GB
  Progress: 100,000 rows, 0.134 GB
  ...
✓ Generated 760,209 events in events_yesterday.csv (1.000 GB)
```

### Row-based generation (`--rows` or just a number)

1. Generates the exact number of rows specified
2. Faster for small datasets
3. Maintains backward compatibility



## Output Files

The script generates two CSV files:

1. **events_yesterday.csv**: Contains only yesterday's events
   - Size: Target size (if using `--gb`) or calculated from rows
   
2. **events_today.csv**: Contains yesterday's + today's events combined
   - Size: 2x target size (if using `--gb`) or calculated from 2x rows

## Performance

The script uses batched generation for efficiency:
- Batch size: 10,000 rows
- Progress updates: Every 50,000 rows
- Memory efficient: Writes to file as it generates

**Approximate generation times:**
- 1 GB: ~2-3 minutes
- 10 GB: ~20-30 minutes
- 100 GB: ~3-4 hours

## Data Schema

The generated data includes all standard Snowplow event fields:
- Timestamps (collector, device created, ETL, derived)
- User identifiers (user_id, domain_userid, network_userid)
- Geo data (country, city, coordinates)
- Page data (URL, title, referrer)
- Device/browser data (user agent, screen size, etc.)
- Event contexts (web page, IAB, YAUAA, UA parser, web vitals)

## Examples

```bash
# Small test (100 rows)
python gen_events.py --rows 100

# Medium dataset (100 MB per day)
python gen_events.py --gb 0.1

# Large dataset (5 GB per day)
python gen_events.py --gb 5

# Very large dataset (50 GB per day)
python gen_events.py --gb 50
```

