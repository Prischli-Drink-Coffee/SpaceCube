# worker/src/scripts/patricle_processor.py

import numpy as np
import pycuda.driver as cuda
from src.shared.schemas import ParticleData
from typing import Dict, Any, List, Tuple
from src.utils.custom_logging import setup_logging


log = setup_logging()



class ParticleProcessor:
    def __init__(self):
        self.all_particles = {}  # Все полученные частицы
        self.local_particles = {}  # Частицы в границах воркера
        
    def load_particles(self, particles: List[ParticleData]):
        # Сохраняем все частицы
        self.all_particles = {p.id: p for p in particles}
        log.info(f"Loaded {len(self.all_particles)} particles")

    def filter_by_bounds(self, bounds):
        prev_count = len(self.local_particles)
        self.local_particles = {pid: p for pid, p in self.all_particles.items() 
                            if self._is_in_bounds(p.position, bounds)}
        log.info(f"Particles after filtering: {len(self.local_particles)} "
                f"(Δ={len(self.local_particles) - prev_count})")
    
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
        """Подготовка данных для локальных частиц"""
        if not self.local_particles:
            # Возвращаем 6 элементов: 5 пустых массивов и N=0
            return (np.empty((0, 3), dtype=np.float32), 
                np.empty((0, 3), dtype=np.float32),
                np.empty((0, 3), dtype=np.float32),
                np.empty(0, dtype=np.float32),
                np.empty(0, dtype=np.float32),
                0)  # Добавляем N=0
            
        try:
            positions = np.array([p.position for p in self.local_particles.values()], dtype=np.float32)
            velocities = np.array([p.velocity for p in self.local_particles.values()], dtype=np.float32)
            accelerations = np.array([p.acceleration for p in self.local_particles.values()], dtype=np.float32)
            masses = np.array([p.mass for p in self.local_particles.values()], dtype=np.float32)
            radii = np.array([p.radius for p in self.local_particles.values()], dtype=np.float32)
            
            # Проверка согласованности размеров
            assert len(positions) == len(velocities) == len(accelerations) == len(masses) == len(radii)
            N = len(self.local_particles)
            return positions, velocities, accelerations, masses, radii, N
            
        except Exception as e:
            log.error(f"Data preparation failed: {e}")
            raise
