"""
Database setup with SQLAlchemy async support.
Supports both SQLite (for development) and PostgreSQL (for production).
"""
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.engine import make_url
from sqlalchemy import event, inspect as sa_inspect
from .config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Convert database URL for async drivers and handle query params
url = make_url(settings.database_url)

if url.drivername == "postgresql":
    url = url.set(drivername="postgresql+asyncpg")
    # Extract non-asyncpg params from URL query
    query = dict(url.query)
    ssl_mode = query.pop("sslmode", None)
    query.pop("schema", None)
    url = url.set(query=query)
    connect_args = {}
    if ssl_mode:
        connect_args["ssl"] = ssl_mode
    engine = create_async_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=5,
        connect_args=connect_args,
    )
elif url.drivername.startswith("sqlite"):
    url = url.set(drivername="sqlite+aiosqlite")
    engine = create_async_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        connect_args={"check_same_thread": False},
    )

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()
else:
    engine = create_async_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
    )
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    """Dependency for getting database session."""
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """Create all tables and auto-migrate missing columns."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Auto-migrate: add columns that exist in models but not in the actual DB
    async with engine.begin() as conn:
        def _migrate(sync_conn):
            inspector = sa_inspect(sync_conn)
            for table_name, table in Base.metadata.tables.items():
                existing = {c["name"] for c in inspector.get_columns(table_name)}
                for col in table.columns:
                    if col.name not in existing:
                        col_type = col.type.compile(sync_conn.dialect)
                        sql_parts = [f"ALTER TABLE {table_name} ADD COLUMN {col.name} {col_type}"]
                        if not col.nullable:
                            sql_parts.append("NOT NULL")
                            # Use server_default or Python default for existing rows
                            default = None
                            if col.server_default is not None:
                                default = col.server_default.arg
                            if default is None and col.default is not None:
                                default = col.default.arg
                            if default is None:
                                # Type-based fallback to prevent migration crash
                                type_name = col.type.__class__.__name__.upper()
                                if "INT" in type_name:
                                    default = "0"
                                elif "BOOL" in type_name:
                                    default = "FALSE"
                                elif "DATETIME" in type_name or "TIMESTAMP" in type_name:
                                    default = "CURRENT_TIMESTAMP"
                                elif "TEXT" in type_name or "VARCHAR" in type_name or "STRING" in type_name:
                                    default = "''"
                                elif "FLOAT" in type_name or "NUMERIC" in type_name or "DECIMAL" in type_name:
                                    default = "0"
                                else:
                                    default = "NULL"
                            sql_parts.append(f"DEFAULT {default}")
                        sql = " ".join(sql_parts)
                        sync_conn.exec_driver_sql(sql)
                        logger.info("Migrated: added column %s.%s (%s)", table_name, col.name, col_type)
        await conn.run_sync(_migrate)
