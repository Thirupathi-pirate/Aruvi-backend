"""
TV-specific API endpoints optimized for Android TV clients.
"""
from typing import List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from sqlalchemy.orm import selectinload

from ..database import get_db
from ..models import File, User, Folder, WatchProgress
from ..auth import get_current_user
from ..config import get_settings
from ..services import (
    escape_like, 
    add_urls_to_file, 
    fetch_recent_files, 
    fetch_continue_watching_files
)

router = APIRouter(prefix="/tv", tags=["TV"])
settings = get_settings()


@router.get("/browse")
async def tv_browse(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get TV home screen data in a single request.
    Returns continue watching, recent files, and folders.
    Optimized for TV client to minimize API calls.
    """
    # Get continue watching
    continue_watching = await fetch_continue_watching_files(db, current_user.id, 20)
    
    # Get recent files
    recent_files = await fetch_recent_files(db, current_user.id, 20)
    
    from sqlalchemy import select, func
    from ..models import Folder, File

    # Get top-level folders with file counts
    file_count_subq = (
        select(func.count(File.id))
        .where(File.folder_id == Folder.id)
        .correlate(Folder)
        .scalar_subquery()
    )
    folders_query = (
        select(Folder, file_count_subq.label("file_count"))
        .where(Folder.user_id == current_user.id, Folder.parent_id.is_(None))
        .order_by(Folder.name)
    )
    folders_result = await db.execute(folders_query)
    folders_rows = folders_result.all()

    return {
        "continue_watching": [add_urls_to_file(f) for f in continue_watching],
        "recent": [add_urls_to_file(f) for f in recent_files],
        "folders": [
            {
                "id": f.id,
                "name": f.name,
                "parent_id": f.parent_id,
                "file_count": fc
            }
            for f, fc in folders_rows
        ]
    }


@router.get("/continue")
async def tv_continue_watching(
    limit: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get continue watching list for TV."""
    files = await fetch_continue_watching_files(db, current_user.id, limit)
    return [add_urls_to_file(f) for f in files]


@router.get("/recent")
async def tv_recent_files(
    limit: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get recently added files for TV."""
    files = await fetch_recent_files(db, current_user.id, limit)
    return [add_urls_to_file(f) for f in files]


@router.get("/search")
async def tv_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(30, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Search files for TV client."""
    # Search files by name
    files_query = (
        select(File)
        .where(
            File.user_id == current_user.id,
            File.file_name.ilike(f"%{escape_like(q)}%", escape="\\")
        )
        .options(selectinload(File.watch_progress))
        .order_by(desc(File.created_at))
        .limit(limit)
    )
    files_result = await db.execute(files_query)
    files = files_result.scalars().all()
    
    # Search folders by name
    folders_query = (
        select(Folder)
        .where(
            Folder.user_id == current_user.id,
            Folder.name.ilike(f"%{escape_like(q)}%", escape="\\")
        )
        .order_by(Folder.name)
        .limit(20)
    )
    folders_result = await db.execute(folders_query)
    folders = folders_result.scalars().all()
    
    return {
        "files": [add_urls_to_file(f) for f in files],
        "folders": [
            {
                "id": f.id,
                "name": f.name,
                "parent_id": f.parent_id
            }
            for f in folders
        ]
    }


@router.get("/folder/{folder_id}")
async def tv_folder_detail(
    folder_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get folder details with files and subfolders for TV client.
    Returns folder info, subfolders, files, and parent path for navigation.
    """
    from fastapi import HTTPException
    
    # Get the folder
    folder_result = await db.execute(
        select(Folder).where(Folder.id == folder_id, Folder.user_id == current_user.id)
    )
    folder = folder_result.scalar_one_or_none()
    
    if not folder:
        raise HTTPException(status_code=404, detail="Folder not found")
    
    # Get subfolders
    subfolders_result = await db.execute(
        select(Folder)
        .where(Folder.user_id == current_user.id, Folder.parent_id == folder_id)
        .order_by(Folder.name)
    )
    subfolders = subfolders_result.scalars().all()
    
    # Get files in this folder
    files_result = await db.execute(
        select(File)
        .where(File.user_id == current_user.id, File.folder_id == folder_id)
        .options(selectinload(File.watch_progress))
        .order_by(File.file_name)
    )
    files = files_result.scalars().all()
    
    # Build parent path for breadcrumb navigation — single query
    parent_path = []
    if folder.parent_id:
        all_folders_result = await db.execute(
            select(Folder)
            .where(Folder.user_id == current_user.id)
        )
        folder_map: dict[int, Folder] = {f.id: f for f in all_folders_result.scalars().all()}
        current = folder
        chain: list[int] = []
        while current.parent_id and current.parent_id in folder_map:
            chain.append(current.parent_id)
            current = folder_map[current.parent_id]
        for pid in reversed(chain):
            a = folder_map[pid]
            parent_path.append({
                "id": a.id,
                "name": a.name,
                "parent_id": a.parent_id,
                "user_id": a.user_id,
                "created_at": a.created_at.isoformat() if a.created_at else None,
                "updated_at": a.updated_at.isoformat() if a.updated_at else None,
            })
    
    return {
        "folder": {
            "id": folder.id,
            "name": folder.name,
            "parent_id": folder.parent_id,
            "user_id": folder.user_id,
            "created_at": folder.created_at.isoformat() if folder.created_at else None,
            "updated_at": folder.updated_at.isoformat() if folder.updated_at else None,
        },
        "subfolders": [
            {
                "id": sf.id,
                "name": sf.name,
                "parent_id": sf.parent_id,
                "user_id": sf.user_id,
                "created_at": sf.created_at.isoformat() if sf.created_at else None,
                "updated_at": sf.updated_at.isoformat() if sf.updated_at else None,
            }
            for sf in subfolders
        ],
        "files": [add_urls_to_file(f) for f in files],
        "parent_path": parent_path
    }
