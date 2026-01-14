from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """
    Base class for all SQLAlchemy ORM models.

    This class provides the declarative base used by SQLAlchemy to define
    database tables as Python classes.

    All ORM models in the application must inherit from this base class
    in order to:
    - Be registered in the SQLAlchemy metadata
    - Be automatically created when initializing the database
    - Participate in migrations and schema management
    """
    pass
