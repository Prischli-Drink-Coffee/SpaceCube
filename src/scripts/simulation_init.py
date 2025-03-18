# orchestrator/src/scripts/simulation_init.py

import numpy as np
from src.shared.schemas import ParticleData
import uuid


class ParticleInitializer:
    def __init__(self, box_size=100.0):
        self.box_size = box_size
        
    def generate(self, num_particles: int) -> list:
        """Генерация частиц с гарантированно уникальными ID"""
        particles = []
        for _ in range(num_particles):
            # Масса и радиус (аналогично C++ коду)
            mass = 0.6 + 0.2 * np.random.rand()  # Масса от 0.6 до 0.8
            radius = 1.0 * (mass ** (1.0/3.0))  # Радиус пропорционален кубическому корню массы
            
            # Позиция (аналогично C++ коду)
            position = (np.random.rand(3)) * self.box_size  # От 0 до boxSize
            
            # Скорость (аналогично C++ коду)
            velocity = (np.random.rand(3) * 2.0 - 1.0) * 10.0  # От -10 до 10
            
            # Ускорение (аналогично C++ коду)
            acceleration = (np.random.rand(3) * 2.0 - 1.0) * 0.01  # От -0.01 до 0.01
            
            particles.append({
                "id": str(uuid.uuid4()),
                "position": position.tolist(),
                "velocity": velocity.tolist(),
                "acceleration": acceleration.tolist(),
                "mass": mass,
                "radius": radius
            })
        return particles
