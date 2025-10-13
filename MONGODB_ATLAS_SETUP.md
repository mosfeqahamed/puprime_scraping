# MongoDB Atlas Setup Guide

This guide will help you set up MongoDB Atlas (MongoDB Cloud) for the PU Prime scraper.

## Step 1: Create MongoDB Atlas Account

1. Go to [MongoDB Atlas](https://www.mongodb.com/atlas)
2. Click "Try Free" or "Sign Up"
3. Create your account or sign in

## Step 2: Create a New Project

1. Once logged in, click "New Project"
2. Give your project a name (e.g., "PU Prime Scraper")
3. Click "Next" and then "Create Project"

## Step 3: Create a Database Cluster

1. Click "Build a Database"
2. Choose "M0 Sandbox" (Free tier) or higher tier based on your needs
3. Select your preferred cloud provider and region
4. Give your cluster a name (e.g., "puprime-cluster")
5. Click "Create Cluster"

## Step 4: Create Database User

1. In the "Database Access" section, click "Add New Database User"
2. Choose "Password" authentication
3. Create a username and strong password
4. Under "Database User Privileges", select "Read and write to any database"
5. Click "Add User"

**Important**: Save the username and password - you'll need them for the connection string.

## Step 5: Configure Network Access

1. In the "Network Access" section, click "Add IP Address"
2. For development, you can click "Allow Access from Anywhere" (0.0.0.0/0)
3. For production, add only your specific IP addresses
4. Click "Confirm"

## Step 6: Get Connection String

1. Go to "Database" section
2. Click "Connect" on your cluster
3. Choose "Connect your application"
4. Select "Python" and version "3.6 or later"
5. Copy the connection string

The connection string will look like:
```
mongodb+srv://<username>:<password>@<cluster-name>.mongodb.net/?retryWrites=true&w=majority
```

## Step 7: Configure Your Scraper

1. Create a `.env` file in your project directory:
   ```bash
   cp config.env.example .env
   ```

2. Edit the `.env` file and replace the connection string:
   ```env
   MONGODB_URI=mongodb+srv://yourusername:yourpassword@yourcluster.abc123.mongodb.net/?retryWrites=true&w=majority
   ```

3. Replace the placeholders:
   - `yourusername`: Your database username
   - `yourpassword`: Your database password
   - `yourcluster.abc123`: Your actual cluster name

## Step 8: Test the Connection

Run the test script to verify your connection:

```bash
python test_installation.py
```

You should see:
```
✅ MongoDB connection successful
✅ Database access successful: puprime_data
```

## Step 9: Run the Scraper

Now you can run the scraper with your MongoDB Atlas connection:

```bash
# Full sync
python puprime.py --email your@email.com --password yourpassword --mode full

# Or use the .env file (no need to specify --mongodb-uri)
python puprime.py --email your@email.com --password yourpassword --mode full
```

## Security Best Practices

### For Production Use:

1. **Restrict IP Access**:
   - Remove "Allow Access from Anywhere" (0.0.0.0/0)
   - Add only your server's IP addresses

2. **Use Strong Passwords**:
   - Use a strong, unique password for your database user
   - Consider using a password manager

3. **Environment Variables**:
   - Never commit your `.env` file to version control
   - Use environment variables in production

4. **Database User Permissions**:
   - Create a user with minimal required permissions
   - Use separate users for different applications

## Troubleshooting

### Common Issues:

1. **Connection Timeout**:
   - Check your internet connection
   - Verify the connection string is correct
   - Ensure your IP is whitelisted

2. **Authentication Failed**:
   - Double-check username and password
   - Make sure special characters in password are URL-encoded

3. **Network Access Denied**:
   - Add your IP address to the whitelist
   - Check if you're behind a corporate firewall

4. **SSL/TLS Issues**:
   - MongoDB Atlas requires SSL by default
   - The connection string should include SSL parameters

### URL Encoding for Special Characters:

If your password contains special characters, encode them:
- `@` becomes `%40`
- `:` becomes `%3A`
- `/` becomes `%2F`
- `?` becomes `%3F`
- `#` becomes `%23`
- `[` becomes `%5B`
- `]` becomes `%5D`

Example:
```
Original password: myP@ssw0rd!
Encoded: myP%40ssw0rd%21
```

## MongoDB Atlas Free Tier Limits

- **Storage**: 512 MB
- **RAM**: Shared
- **Connections**: 500 concurrent connections
- **Data Transfer**: 1 GB/month

For the PU Prime scraper, the free tier should be sufficient for most use cases.

## Monitoring Your Database

1. **Atlas Dashboard**: Monitor your cluster performance
2. **Database Collections**: View your scraped data
3. **Metrics**: Track storage usage and performance
4. **Alerts**: Set up alerts for unusual activity

## Backup and Recovery

MongoDB Atlas provides:
- **Automated backups** (for paid tiers)
- **Point-in-time recovery**
- **Export/import functionality**

For the free tier, consider implementing your own backup strategy.

## Support

- **MongoDB Atlas Documentation**: [docs.atlas.mongodb.com](https://docs.atlas.mongodb.com)
- **Community Forum**: [community.mongodb.com](https://community.mongodb.com)
- **Support Tickets**: Available in Atlas dashboard (for paid tiers)
