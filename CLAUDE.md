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

# Or use the console script (after pip install)
tape-backup
```

The web interface will be available at: http://localhost:8080

### Testing
```bash
# Run all tests
pytest

# Run specific tests
pytest tests/test_backup.py
pytest tests/test_tape.py
pytest tests/test_config.py

# Run tests with coverage
pytest --cov=.

# Run tests with verbose output
pytest -v

# Run specific test modules
pytest tests/test_7zip_command.py
pytest tests/test_py7zr_compression.py

# Development installation with testing dependencies
pip install -e .[dev]
```

The test suite uses `conftest.py` for async setup and automatically configures the test environment with debug logging enabled.

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
   - Special openGauss handling to avoid version parsing issues
   - Schema migration system with automatic column additions

4. **Storage Engines**
   - **Backup Engine** (`backup/backup_engine.py`) - Handles backup operations
   - **Recovery Engine** (`recovery/recovery_engine.py`) - Handles recovery operations
   - **Tape Manager** (`tape/tape_manager.py`) - Manages tape devices and operations

5. **Utilities** (`utils/`)
   - **Scheduler** - Advanced task scheduling with cron support and multiple schedule types
   - **Logger** - Structured logging system with system log handler
   - **DingTalk Notifier** - Notification integration
   - **Action Handlers** - Modular task execution framework
   - **Task Storage** - Database-persisted task management

### Key Features

- **6-Month Backup Cycle**: Automatic backup lifecycle management
- **Multiple Backup Strategies**: Full, incremental, differential, mirror, archive, snapshot
- **Advanced Task Scheduling**: Cron-based scheduling with multiple schedule types (once, interval, daily, weekly, monthly)
- **Tape Management**: SCSI tape device interface, LTFS file system support, ITDT integration
- **Multi-Engine Compression**: PGZip for speed, 7-Zip SDK for compatibility, configurable compression levels
- **Web Configuration**: Visual database configuration and system settings
- **Cross-Platform**: Windows and openEuler support
- **Database Migration**: Automatic schema migrations and column additions

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
- Vue.js 3 for reactive components and state management
- Modular JavaScript architecture:
  - `web/static/js/modules/` - Core application modules
  - `web/static/js/components/` - Reusable Vue.js components
  - `web/static/js/pages/` - Page-specific JavaScript
- Axios for HTTP requests
- Real-time UI updates for system status
- Component-based architecture with login modals, system monitoring, and configuration management

## Known Issues and Solutions

### Scheduler Task Save Error
**Issue**: `TypeError: Cannot read properties of null (reading 'value')` in scheduler.js:1230

**Cause**: DOM element not found when getting action configuration values

**Solution**: Ensure action config panels are properly rendered before reading values, check element existence before accessing `.value`

### Database Compatibility
- OpenGauss version parsing issues handled gracefully
- ENUM case sensitivity normalized to uppercase
- Automatic table creation for missing tables
- Schema migration system handles column additions automatically
- Connection pooling with configurable timeouts

### Compression Pipeline
- Multi-engine support: PGZip for speed, py7zr and 7-Zip CLI for compatibility
- Configurable compression levels (1-9) and dictionary sizes
- Direct-to-tape compression to minimize disk I/O
- Batch processing with configurable file size thresholds (3GB max)
- Solid compression with configurable block sizes (64MB default)

### Tape Device Detection
- Windows: Uses WMI and SCSI Pass Through API
- Linux: Uses SG_IO and device detection
- LTFS file system support for tape label reading
- ITDT (IBM Tape Diagnostic Tool) integration for hardware operations
- Device caching and background scanning for performance

## Platform-Specific Notes

### Windows
- Requires pywin32 and wmi packages for hardware access
- Tape device paths use `\\.\TAPEn` format
- LTFS drive letter configuration supported
- ITDT integration via `c:\itdt\itdt.exe`
- SCSI Pass Through API for low-level tape operations

### Linux/openEuler
- Uses udev for device detection and pyudev for device management
- Requires proper SCSI device permissions and group membership
- Mount point-based tape drive access
- ITDT integration via `/usr/local/itdt/itdt`
- SG_IO interface for direct SCSI communication

## Testing Strategy

Focus testing on:
- Database connection and operations (especially openGauss compatibility)
- Tape device detection and operations (with hardware)
- Compression pipeline functionality and 7-Zip integration
- API endpoint functionality and authentication
- Scheduler task creation and execution (cron expressions)
- Web interface interactions and JavaScript modules
- Task scheduling with different schedule types
- Async operations and proper cleanup

## Debugging

- Logs stored in `logs/` directory
- Structured logging with different levels
- Web interface shows system status and errors
- Database connection testing in System Settings