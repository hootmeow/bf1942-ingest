# Battlefield 1942 Data Ingestion Engine

This application continuously monitors all online Battlefield 1942 game servers, collects detailed status information, and stores it in a PostgreSQL database. It serves as the data collection backend for stats websites, APIs, and Discord bots.

## Features

* **Real-time Data:** Fetches the master server list and polls individual servers frequently.
* **Adaptive Polling:** Uses different polling intervals for active, empty, and offline servers to be efficient.
* **Robust Querying:** Implements a fallback strategy to query servers on both the master list port and the standard query port (23000).
* **Data Storage:** Uses PostgreSQL with `asyncpg` for asynchronous database operations.
* **Detailed History:** Stores immutable snapshots of server state over time.
* **Player Tracking:** Records player join/leave times for session analysis.
* **Dynamic Exclusions:** Allows filtering out specific game types or player names via a database table.
* **Modular Design:** Code is organized into logical components for maintainability.
* **Service Ready:** Includes instructions for running as a `systemd` service on Linux.

## Requirements

* **Server:** Ubuntu 24.04 LTS (or a similar Debian-based Linux distribution)
* **Database:** PostgreSQL (Version 14 or higher recommended)
* **Python:** Version 3.10 or higher

## Setup Instructions

These instructions guide you through setting up the engine on a new Ubuntu server where both the application and the PostgreSQL database will run.

### 1. Initial Server & User Setup

First, secure your server by creating a dedicated non-root user.

1.  **Log in as `root`** and update the system:
    ```bash
    ssh root@your_server_ip
    apt update && apt upgrade -y
    ```
2.  **Create a new user** (e.g., `bf1942_user`):
    ```bash
    adduser bf1942_user
    # Follow prompts to set a password
    ```
3.  **Grant `sudo` privileges**:
    ```bash
    usermod -aG sudo bf1942_user
    ```
4.  **Log out and log back in as the new user**:
    ```bash
    # From your local machine
    ssh bf1942_user@your_server_ip
    ```

### 2. Install PostgreSQL & System Dependencies

Install the database and other required software.

1.  **Install packages**:
    ```bash
    sudo apt install postgresql postgresql-contrib python3-pip python3-venv git ufw -y
    ```
2.  **Configure Firewall**: Allow SSH and outgoing connections.
    ```bash
    sudo ufw allow ssh
    sudo ufw default allow outgoing
    sudo ufw enable
    # Press 'y' to confirm
    ```

### 3. Configure PostgreSQL Database

Create the database and a dedicated user for the application.

1.  **Log in to `psql`** as the `postgres` superuser:
    ```bash
    sudo -u postgres psql
    ```
2.  **Run SQL Commands**: Execute these commands inside the `psql` shell. **Replace `'your_secure_password_here'` with a strong, unique password.**
    ```sql
    -- Create the database
    CREATE DATABASE bf1942_db;

    -- Create the application user
    CREATE USER bf1942_user WITH PASSWORD 'your_secure_password_here';

    -- Grant ownership of the database to the user
    ALTER DATABASE bf1942_db OWNER TO bf1942_user;

    -- Exit psql
    \q
    ```

### 4. Deploy the Application Code

1.  **Clone the Repository**: Navigate to your home directory and clone the code.
    ```bash
    cd ~
    git clone (https://github.com/hootmeow/bf1942-ingest.git
    cd bf1942-ingest
    ```
2.  **Set Up Python Environment**: Create a virtual environment and install dependencies.
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```
3.  **Configure Environment**: Create a `.env` file to store the database connection string.
    ```bash
    nano .env
    ```
    Paste the following line into the file, replacing `'your_secure_password_here'` with the database password you created earlier.
    ```env
    POSTGRES_DSN="postgresql://bf1942_user:your_secure_password_here@localhost:5432/bf1942_db"
    ```
    Save and close the file (`Ctrl+X`, `Y`, `Enter`).

### 5. Run the Application (Manual Test)

Before setting up the service, run the application manually to ensure the database connection works and the schema is created.

1.  Make sure your virtual environment is active:
    ```bash
    source venv/bin/activate
    ```
2.  Run the main script:
    ```bash
    python main.py
    ```
    You should see output indicating it's connecting to PostgreSQL, setting up the schema, and starting the scheduler. Press `Ctrl+C` to stop it after verifying the initial connection.

### 6. Set Up the `systemd` Service

Create a service file to run the application in the background and ensure it restarts automatically.

1.  **Create the Service File**:
    ```bash
    sudo nano /etc/systemd/system/bf1942-ingest.service
    ```
2.  **Paste the Configuration**:
    ```ini
    [Unit]
    Description=Battlefield 1942 Data Ingestion Engine (PostgreSQL)
    After=network.target

    [Service]
    User=bf1942_user
    WorkingDirectory=/home/bf1942_user/bf1942-ingest
    ExecStart=/home/bf1942_user/bf1942-ingest/venv/bin/python -u main.py
    Restart=always
    RestartSec=10

    [Install]
    WantedBy=multi-user.target
    ```
    Save and close the file.

3.  **Enable and Start the Service**:
    ```bash
    sudo systemctl daemon-reload
    sudo systemctl enable bf1942-ingest.service
    sudo systemctl start bf1942-ingest.service
    ```

### 7. Verify Operation

1.  **Check Service Status**:
    ```bash
    sudo systemctl status bf1942-ingest.service
    # Look for 'Active: active (running)'
    ```
2.  **View Live Logs**:
    ```bash
    sudo journalctl -u bf1942-ingest.service -f
    # You should see connection messages, schema setup, and server discovery logs.
    ```
3.  **Check the Database**: Connect with `psql` or a GUI tool and verify that tables are being populated.

## Managing Exclusions

A command-line script is included to manage exclusion rules (e.g., ignoring specific game types or player names).

Make sure your virtual environment is active (`source venv/bin/activate`) before running these commands from the project root directory (`/home/bf1942_user/bf1942-ingest`).

* **List all exclusions:**
    ```bash
    python manage_exclusions.py list
    ```
* **Add a new exclusion:**
    ```bash
    # Exclude co-op game mode
    python manage_exclusions.py add gametype coop --notes "Exclude co-op mode"
    # Exclude a specific bot player name
    python manage_exclusions.py add player_name SomeBotName
    # Exclude a server from leaderboard calculations (but still collect data)
    python manage_exclusions.py add server_id "1.2.3.4:23000" --notes "Exclude stats only"
    ```
* **Remove an exclusion** (use the ID from the `list` command):
    ```bash
    python manage_exclusions.py remove 1
    ```


