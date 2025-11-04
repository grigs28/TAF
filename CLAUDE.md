# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

企业级磁带备份系统 (Enterprise Tape Backup System) is a Python-based modern tape backup solution supporting Windows and openEuler platforms. The system features a Web management interface, 6-month backup cycle, 7-Zip SDK compression, and DingTalk notification integration.

## Development Commands

### Environment Setup
```bash
# Create conda environment (recommended)
conda create -n taf python=3.9
conda activate taf

# Install dependencies
pip install -r requirements.txt
```

### Running the Application
```bash
# Start the main application
python main.py
```

The web interface will be available at: http://localhost:8080

### Testing
```bash
# Run all tests
pytest

# Run specific tests
pytest tests/test_backup.py

# Generate coverage report
pytest --cov=.
```

### Code Quality
```bash
# Format code
black .

# Lint code
flake8 .

# Type checking
mypy .
```

## Architecture Overview

### Core Components

1. **Main Application** (`main.py`)
   - Entry point and system orchestration
   - Async initialization and shutdown handling
   - Component lifecycle management

2. **Web Framework** (`web/`)
   - FastAPI-based REST API
   - Modern dark theme UI with responsive design
   - Authentication and logging middleware

3. **Database Layer** (`config/database.py`, `models/`)
   - Multi-database support: SQLite, PostgreSQL, openGauss, MySQL
   - SQLAlchemy ORM with async support
   - Automatic database initialization and migration

4. **Storage Engines**
   - **Backup Engine** (`backup/backup_engine.py`) - Handles backup operations
   - **Recovery Engine** (`recovery/recovery_engine.py`) - Handles recovery operations
   - **Tape Manager** (`tape/tape_manager.py`) - Manages tape devices and operations

5. **Utilities** (`utils/`)
   - **Scheduler** - Task scheduling and execution
   - **Logger** - Structured logging system
   - **DingTalk Notifier** - Notification integration

### Key Features

- **6-Month Backup Cycle**: Automatic backup lifecycle management
- **Multiple Backup Strategies**: Full, incremental, differential, mirror, archive, snapshot
- **Tape Management**: SCSI tape device interface, LTFS file system support
- **Web Configuration**: Visual database configuration and system settings
- **Cross-Platform**: Windows and openEuler support

## Configuration

### Environment Variables
Copy `.env.sample` to `.env` and configure:
- Database connection settings
- Tape device paths
- DingTalk notification settings
- Web server configuration

### Database Configuration
The system supports web-based database configuration. Access System Settings → Database tab to configure connections visually.

### Tape Device Configuration
Configure tape devices in System Settings or through the dedicated Tape Management interface.

## API Structure

- `/api/backup` - Backup management
- `/api/recovery` - Recovery management
- `/api/tape` - Tape device management
- `/api/scheduler` - Task scheduling
- `/api/system` - System configuration and monitoring
- `/api/user` - User management

## Development Guidelines

### File Organization
- Models in `models/` directory
- API endpoints in `web/api/` directory
- HTML templates in `web/templates/` directory
- Static assets in `web/static/` directory

### Error Handling
- Components initialize gracefully on startup
- Database connection failures don't prevent system startup
- Web interface provides configuration error resolution

### Frontend Development
- Uses Bootstrap 5 with custom dark theme
- JavaScript modules in `web/static/js/modules/`
- Axios for HTTP requests
- Real-time UI updates for system status

## Known Issues and Solutions

### Scheduler Task Save Error
**Issue**: `TypeError: Cannot read properties of null (reading 'value')` in scheduler.js:1230

**Cause**: DOM element not found when getting action configuration values

**Solution**: Ensure action config panels are properly rendered before reading values, check element existence before accessing `.value`

### Database Compatibility
- OpenGauss version parsing issues handled gracefully
- ENUM case sensitivity normalized to uppercase
- Automatic table creation for missing tables

### Tape Device Detection
- Windows: Uses WMI and SCSI Pass Through API
- Linux: Uses SG_IO and device detection
- LTFS file system support for tape label reading

## Platform-Specific Notes

### Windows
- Requires pywin32 and wmi packages
- Tape device paths use `\\.\TAPEn` format
- LTFS drive letter configuration supported

### Linux/openEuler
- Uses udev for device detection
- Requires proper SCSI device permissions
- Mount point-based tape drive access

## Testing Strategy

Focus testing on:
- Database connection and operations
- Tape device detection and operations (with hardware)
- API endpoint functionality
- Scheduler task creation and execution
- Web interface interactions

## Debugging

- Logs stored in `logs/` directory
- Structured logging with different levels
- Web interface shows system status and errors
- Database connection testing in System Settings