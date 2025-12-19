# Quick Setup Guide - WBL Integration

This guide helps you set up the LinkedIn bot to log activities to the WBL backend.

##  Setup for New Employees

### 1. Install Dependencies
Make sure you have all required libraries installed:
```bash
pip install -r requirements.txt
```

### 2. Run Automated Setup
Each employee needs their own credentials and token. Run this script once to configure your personal `.env` file:
```bash
python setup_production.py
```
This script will:
- Ask for your WBL email/password
- Ask for your **Employee ID**
- Get your personal **JWT Token**
- Update your `.env` file automatically

### 3. Check LinkedIn Credentials
Open your `.env` file and make sure your LinkedIn email and password are set:
```env
LINKEDIN_EMAIL=your_email@gmail.com
LINKEDIN_PASSWORD=your_password
```

### 4. Run the Bot
Now you are ready to start extraction:
```bash
python linkedin_bot_complete.py
```

##  How it Works
The bot extracts contact information from LinkedIn posts and automatically sends a summary of how many contacts were processed to the WBL dashboard. This helps track bot activity and performance across different employees.

##  Important Notes
- **Employee ID**: Ensure you use your correct ID from the WBL system.
- **Token Expiry**: If you get a "401 Unauthorized" error later, just re-run `python setup_production.py` to get a fresh token.
- **Database Logic**: No database passwords are stored in this script. All communication is done securely via the WBL API.
