# bootstrap/inserter.py
import logging
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import inspect

logger = logging.getLogger("task-generator")


def sanitize_row(model, row):
    """
    Sanitize a row dictionary to match the model columns.
    Remove any keys that don't correspond to columns.
    """
    valid_columns = {c.name for c in model.__table__.columns}
    sanitized = {}
    
    for key, value in row.items():
        if key in valid_columns:
            # Convert None/null values properly
            if value is None or (isinstance(value, str) and value.lower() == 'null'):
                sanitized[key] = None
            else:
                sanitized[key] = value
        else:
            logger.debug(f"Skipping unknown column '{key}' for table {model.__tablename__}")
    
    return sanitized


def get_unique_columns(model):
    """
    Automatically detect unique columns from the model.
    Returns the first unique column found, or ['id'] as fallback.
    """
    inspector = inspect(model)
    
    # Check for unique constraints
    for constraint in model.__table__.constraints:
        if hasattr(constraint, 'columns') and len(constraint.columns) > 0:
            # Check if it's a unique constraint
            if constraint.__class__.__name__ == 'UniqueConstraint':
                col_names = [col.name for col in constraint.columns]
                logger.debug(f"Found unique constraint on {model.__tablename__}: {col_names}")
                return col_names
    
    # Check for unique columns
    for column in model.__table__.columns:
        if column.unique and not column.primary_key:
            logger.debug(f"Found unique column on {model.__tablename__}: {column.name}")
            return [column.name]
    
    # Fallback to primary key
    pk_cols = [col.name for col in model.__table__.primary_key.columns]
    logger.debug(f"Using primary key for {model.__tablename__}: {pk_cols}")
    return pk_cols


def upsert_rows(session, model, rows):
    """
    Upsert rows into a table using ON CONFLICT DO UPDATE.
    Automatically detects unique columns from the model.
    
    Args:
        session: SQLAlchemy session
        model: SQLAlchemy model class
        rows: List of row dictionaries
    """
    # Auto-detect conflict columns
    conflict_cols = get_unique_columns(model)
    
    logger.debug(f"Upserting {len(rows)} rows into {model.__tablename__} (conflict on: {conflict_cols})")
    
    # Sanitize all rows first
    sanitized_rows = [sanitize_row(model, row) for row in rows]
    
    # Process each row individually to avoid bulk insert issues
    inserted = 0
    
    for i, row in enumerate(sanitized_rows, 1):
        try:
            stmt = insert(model).values(row)
            
            # Build the update set - update all columns except conflict columns
            update_set = {
                c.name: getattr(stmt.excluded, c.name)
                for c in model.__table__.columns
                if c.name not in conflict_cols and c.name != 'id'
            }
            
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_cols,
                set_=update_set,
            )
            
            session.execute(stmt)
            inserted += 1
            
            if i % 10 == 0:
                logger.debug(f"Processed {i}/{len(sanitized_rows)} rows...")
                
        except Exception as e:
            logger.error(f"Failed to upsert row {i} into {model.__tablename__}: {e}")
            logger.debug(f"Problematic row data: {row}")
            raise
    
    logger.debug(f"Upsert complete: {inserted} rows processed successfully")
