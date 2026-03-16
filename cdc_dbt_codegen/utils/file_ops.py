# /* Copyright (C) 2022 Cloud Data Consultants Inc. - All Rights Reserved - 
# *
# * You may not copy, reproduce, distribute, transmit, modify, create derivative works, 
# * or in any other way exploit any part of copyrighted material without permission.
# * 
# */

"""
File operation utilities for CDC DBT Codegen.

This module provides safe file operations with backup capabilities.
"""

import os
import shutil
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def backup_file(file_path: Path) -> Optional[Path]:
    """
    Create a backup of an existing file.
    
    Args:
        file_path: Path to the file to backup
        
    Returns:
        Path to the backup file, or None if no backup was needed
    """
    if not file_path.exists():
        return None
    
    # Create backup filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = file_path.with_suffix(f".backup_{timestamp}{file_path.suffix}")
    
    # Copy file to backup
    shutil.copy2(file_path, backup_path)
    logger.info(f"Created backup: {backup_path}")
    
    return backup_path


def safe_write_file(file_path: Path, content: str, 
                   backup: bool = True, dry_run: bool = False) -> Optional[Path]:
    """
    Safely write content to a file with optional backup.
    
    Args:
        file_path: Path to write to
        content: Content to write
        backup: Whether to backup existing file
        dry_run: If True, don't actually write
        
    Returns:
        Path to backup file if created, None otherwise
    """
    backup_path = None
    
    if dry_run:
        logger.info(f"[DRY RUN] Would write to: {file_path}")
        if file_path.exists():
            logger.info(f"[DRY RUN] File exists, would create backup")
        return None
    
    # Create directory if needed
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Backup existing file if requested
    if backup and file_path.exists():
        backup_path = backup_file(file_path)
    
    # Write the file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
        if not content.endswith('\n'):
            f.write('\n')
    
    logger.info(f"Wrote file: {file_path}")
    
    return backup_path


def ensure_directory(dir_path: Path, dry_run: bool = False) -> bool:
    """
    Ensure a directory exists.
    
    Args:
        dir_path: Directory path to create
        dry_run: If True, don't actually create
        
    Returns:
        True if directory was created, False if it already existed
    """
    if dir_path.exists():
        return False
    
    if dry_run:
        logger.info(f"[DRY RUN] Would create directory: {dir_path}")
        return True
    
    dir_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created directory: {dir_path}")
    return True


def clean_old_backups(directory: Path, keep_count: int = 5):
    """
    Clean old backup files in a directory.
    
    Args:
        directory: Directory to clean
        keep_count: Number of recent backups to keep per file
    """
    if not directory.exists():
        return
    
    # Group backups by original filename
    backup_groups = {}
    for file in directory.iterdir():
        if '.backup_' in file.name:
            # Extract original filename
            parts = file.name.split('.backup_')
            if parts:
                original = parts[0]
                if original not in backup_groups:
                    backup_groups[original] = []
                backup_groups[original].append(file)
    
    # Sort and clean each group
    for original, backups in backup_groups.items():
        # Sort by modification time (newest first)
        backups.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        
        # Remove old backups
        for backup in backups[keep_count:]:
            backup.unlink()
            logger.info(f"Removed old backup: {backup}")


def compare_files(file1: Path, file2: Path) -> bool:
    """
    Compare two files for equality.
    
    Args:
        file1: First file path
        file2: Second file path
        
    Returns:
        True if files are identical, False otherwise
    """
    if not file1.exists() or not file2.exists():
        return False
    
    # Compare file sizes first
    if file1.stat().st_size != file2.stat().st_size:
        return False
    
    # Compare content
    with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
        while True:
            chunk1 = f1.read(8192)
            chunk2 = f2.read(8192)
            if chunk1 != chunk2:
                return False
            if not chunk1:
                break
    
    return True