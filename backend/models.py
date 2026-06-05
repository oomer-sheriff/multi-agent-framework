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

class SubtaskItem(Base):
    __tablename__ = "subtasks"
    
    id = Column(String, primary_key=True, index=True)
    parent_task_id = Column(String, index=True, nullable=False)
    description = Column(Text, nullable=False)
    profile_name = Column(String, nullable=False)
    status = Column(String, nullable=False, default="waiting")
    dependencies = Column(JSON, nullable=False, default=list)
    s3_url = Column(String, nullable=True)
