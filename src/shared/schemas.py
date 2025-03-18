from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List, Tuple
import uuid


class ParticleData(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    position: List[float]
    velocity: List[float]
    acceleration: List[float]
    mass: float
    radius: float

class KernelUpdate(BaseModel):
    code: str
    version: str

class SimulationControl(BaseModel):
    command: str  # 'start' | 'stop' | 'advance_step'
    dt: Optional[float] = None
    box_size: Optional[float] = None
    worker_id: Optional[float] = None
    bounds: Optional[List[Tuple[float, float]]] = None
    step: Optional[int] = None
