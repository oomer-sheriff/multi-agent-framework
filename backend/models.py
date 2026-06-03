from sqlalchemy import Column, String, Text, JSON
import uuid
from db import Base

class Profile(Base):
    __tablename__ = "profiles"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, unique=True, index=True, nullable=False)
    system_prompt = Column(Text, nullable=False)
    mcp_servers = Column(JSON, nullable=False, default=list)
    workflow_config = Column(JSON, nullable=False, default=dict)
