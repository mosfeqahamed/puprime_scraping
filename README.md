# PU Prime Account Scraper

A comprehensive web scraper for PU Prime IB Portal that extracts account data and stores it in MongoDB with scheduled sync capabilities.

## Features

- ✅ **Account Report Scraping**: Extracts date, user ID, account number, name, and email from the Account Report page
- ✅ **Pagination Support**: Automatically handles multiple pages of data
- ✅ **MongoDB Integration**: Stores scraped data in MongoDB with proper indexing
- ✅ **Incremental Sync**: Only processes new records since the last sync
- ✅ **Scheduled Sync**: Automatically runs sync operations at configurable intervals
- ✅ **Anti-Detection**: Uses undetected-chromedriver and stealth techniques
- ✅ **Error Handling**: Comprehensive error handling and logging
- ✅ **Command Line Interface**: Easy-to-use CLI with multiple operation modes

## Installation

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Setup MongoDB**:
   - **Recommended**: Use MongoDB Atlas (Cloud) - see [MONGODB_ATLAS_SETUP.md](MONGODB_ATLAS_SETUP.md)
   - **Alternative**: Install MongoDB locally (default: `mongodb://localhost:27017/`)

3. **Configure Environment**:
   ```bash
   cp config.env.example .env
   # Edit .env with your MongoDB Atlas connection string
   ```
   
   For MongoDB Atlas, your `.env` file should look like:
   ```env
   MONGODB_URI=mongodb+srv://username:password@cluster0.abc123.mongodb.net/?retryWrites=true&w=majority
   ```

## Usage

### Command Line Interface

#### Full Sync (Scrape All Data)
```bash
python puprime.py --email your@email.com --password yourpassword --mode full
```

#### Incremental Sync (New Data Only)
```bash
python puprime.py --email your@email.com --password yourpassword --mode incremental
```

#### Scheduled Sync (Continuous)
```bash
python puprime.py --email your@email.com --password yourpassword --mode scheduled --interval 6
```

#### With Custom MongoDB URI
```bash
python puprime.py --email your@email.com --password yourpassword --mode full --mongodb-uri "mongodb+srv://username:password@cluster0.abc123.mongodb.net/?retryWrites=true&w=majority"
```

#### Headless Mode (No Browser Window)
```bash
python puprime.py --email your@email.com --password yourpassword --mode full --headless
```

### Command Line Options

- `--email`: Your PU Prime login email (required)
- `--password`: Your PU Prime login password (required)
- `--mongodb-uri`: MongoDB connection string (optional, default: mongodb://localhost:27017/)
- `--mode`: Sync mode - `full`, `incremental`, or `scheduled` (default: full)
- `--interval`: Sync interval in hours for scheduled mode (default: 6)
- `--headless`: Run browser in headless mode (no GUI)

### Legacy Mode

For backward compatibility, you can still run without arguments (uses hardcoded credentials):

```bash
python puprime.py
```

## Data Structure

The scraper extracts the following data from each account:

```json
{
  "date": "2025-01-08T00:00:00",
  "date_string": "08/01/2025",
  "user_id": "892776",
  "account_number": "2437312",
  "name": "Tommy Sebastian Johansson",
  "email": "zebb@kumrif.com",
  "campaign_source": "",
  "id_status": "Completed",
  "poa_status": "Completed",
  "scraped_at": "2025-01-13T10:30:00",
  "last_updated": "2025-01-13T10:30:00"
}
```

## MongoDB Collections

### `accounts`
Stores the scraped account data with the following indexes:
- `account_number` (unique)
- `user_id`
- `date`
- `scraped_at`

### `sync_logs`
Tracks sync operations:
- `sync_time`: When the sync occurred
- `status`: "success" or "failed"
- `records_processed`: Number of records processed
- `error_message`: Error details (if failed)

## How It Works

1. **Login**: Automatically logs into the PU Prime IB Portal
2. **Navigation**: Navigates to the Account Report page
3. **Data Extraction**: Scrapes all visible account data from the table
4. **Pagination**: Automatically handles multiple pages
5. **Data Processing**: Validates and processes the extracted data
6. **MongoDB Storage**: Stores data with duplicate detection
7. **Sync Logging**: Records sync operation details

## Incremental Sync Logic

The incremental sync works by:
1. Checking the timestamp of the last successful sync
2. Scraping all current data from the website
3. Filtering records that were created after the last sync
4. Only inserting/updating new records

## Scheduled Sync

The scheduled sync service:
- Runs continuously in the background
- Performs incremental syncs at specified intervals
- Handles errors gracefully and continues running
- Can be stopped with Ctrl+C

## Error Handling

The scraper includes comprehensive error handling:
- Login failures
- Network timeouts
- Element not found errors
- MongoDB connection issues
- Data validation errors

All errors are logged with timestamps and detailed messages.

## Screenshots

The scraper automatically takes screenshots for debugging:
- `account_report_page.png`: Shows the Account Report page
- Other debug screenshots are created as needed

## Requirements

- Python 3.7+
- Chrome browser
- MongoDB (local or cloud)
- Internet connection

## Dependencies

- `selenium`: Web scraping
- `pymongo`: MongoDB integration
- `schedule`: Task scheduling
- `python-dotenv`: Environment variables
- `undetected-chromedriver`: Anti-detection (optional)

## Troubleshooting

### Common Issues

1. **Chrome Driver Issues**:
   - Make sure Chrome browser is installed
   - The scraper will automatically download ChromeDriver if needed

2. **MongoDB Connection**:
   - **For MongoDB Atlas**: Check connection string format and network access
   - **For Local MongoDB**: Ensure MongoDB is running
   - Verify credentials and IP whitelist (for Atlas)

3. **Login Failures**:
   - Verify credentials are correct
   - Check if 2FA is enabled (not supported)
   - Ensure account is not locked

4. **No Data Found**:
   - Check if you have access to the Account Report page
   - Verify the page structure hasn't changed
   - Check screenshots for debugging

### Debug Mode

Run with `--headless false` to see the browser and debug issues visually.

## Security Notes

- Never commit credentials to version control
- Use environment variables for sensitive data
- Consider using MongoDB authentication
- Run in a secure environment

## License

This project is for educational and personal use only. Please respect the website's terms of service and rate limits.
