import json
from src.shared.schemas import ParticleData
from typing import List
from src.utils.custom_logging import setup_logging
from src import path_to_project
from env import Env

log = setup_logging()
env = Env()


def serialize_particles(particles: List[ParticleData]) -> bytes:
    return json.dumps([p.dict() for p in particles]).encode('utf-8')


def deserialize_particles(data):
    return [ParticleData(**p) for p in json.loads(data)]


def get_env_value(env, key, default=None, required=False, type_cast=str):
    value = env.__getattr__(key)
    if not value or value == ['']:
        if required:
            raise ValueError(f"{key} environment variable is required")
        log.warning(f"{key} environment variable was set to default value -> {default}")
        return default
    return type_cast(value)