# worker/src/scripts/patricle_processor.py

import numpy as np
import pycuda.driver as cuda
from src.shared.schemas import ParticleData
from typing import Dict, Any, List, Tuple
from src.utils.custom_logging import setup_logging


log = setup_logging()



class ParticleProcessor:
    def __init__(self):
        self.all_particles = []  # Список для сохранения порядка
        self.local_indices = []  # Индексы локальных частиц
        self.local_particles = {}  # Словарь локальных частиц (particle_id -> ParticleData)
        self.current_bounds = None

    def load_particles(self, particles: List[ParticleData]):
        self.all_particles = particles  # Сохраняем как список
        self.local_particles = {p.id: p for p in particles}  # Инициализируем словарь

    def filter_by_bounds(self, bounds):
        self.current_bounds = bounds
        self.local_indices = []
        self.local_particles = {}  # Очищаем словарь перед фильтрацией

        for idx, p in enumerate(self.all_particles):
            if self._is_in_bounds(p.position, bounds):
                self.local_indices.append(idx)
                self.local_particles[p.id] = p  # Добавляем в словарь

    def _is_in_bounds(self, pos, bounds):
        """Улучшенная проверка границ"""
        try:
            x, y, z = pos
            return (bounds[0][0] <= x < bounds[0][1] and 
                    bounds[1][0] <= y < bounds[1][1] and 
                    bounds[2][0] <= z < bounds[2][1])
        except (TypeError, IndexError):
            return False

    def get_local_data(self):
        if not self.all_particles:
            return (np.empty((0, 3), np.empty((0, 3)), np.empty((0, 3)), 
                    np.empty(0), np.empty(0), np.empty(0, dtype=np.int32), 0))
        try:
            positions = np.array([p.position for p in self.all_particles], dtype=np.float32)
            velocities = np.array([p.velocity for p in self.all_particles], dtype=np.float32)
            accelerations = np.array([p.acceleration for p in self.all_particles], dtype=np.float32)
            masses = np.array([p.mass for p in self.all_particles], dtype=np.float32)
            radii = np.array([p.radius for p in self.all_particles], dtype=np.float32)
            local_flags = np.zeros(len(self.all_particles), dtype=np.int32)
            local_flags[self.local_indices] = 1
            N = len(self.all_particles)
            return (positions, velocities, accelerations, masses, radii, local_flags, N)
        except Exception as e:
            log.error(f"Data preparation failed: {e}")
            raise
