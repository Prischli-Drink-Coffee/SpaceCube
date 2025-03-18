from typing import Dict, List, Tuple
from collections import defaultdict
from math import isclose, ceil
from src.shared.schemas import ParticleData


class GridManager:
    def __init__(self, box_size: float, num_workers: int):
        self.box_size = box_size
        self.num_workers = num_workers
        self.dimensions = self._optimize_grid_dimensions()
        self.cell_sizes = (
            box_size / self.dimensions[0], 
            box_size / self.dimensions[1], 
            box_size / self.dimensions[2]
        )
        self.worker_bounds = self._calculate_worker_bounds()

    def _optimize_grid_dimensions(self) -> Tuple[int, int, int]:
        """
        Оптимизирует размеры сетки на основе числа воркеров.
        Возвращает кортеж (x, y, z) с количеством ячеек по каждой оси.
        """
        # Если воркер один, возвращаем (1, 1, 1)
        if self.num_workers == 1:
            return 1, 1, 1
        
        # Находим кубический корень из числа воркеров
        cube_root = self.num_workers ** (1 / 3)
        
        # Округляем до ближайшего целого
        x = ceil(cube_root)
        y = ceil(cube_root)
        z = ceil(cube_root)
        
        # Корректируем размеры, чтобы x * y * z >= num_workers
        while x * y * z < self.num_workers:
            if x <= y and x <= z:
                x += 1
            elif y <= x and y <= z:
                y += 1
            else:
                z += 1
        
        return x, y, z

    def _calculate_worker_bounds(self) -> Dict[int, List[Tuple[float, float]]]:
        bounds = {}
        
        # Если воркер один, возвращаем границы всего куба
        if self.num_workers == 1:
            bounds[0] = [
                (0.0, self.box_size),
                (0.0, self.box_size),
                (0.0, self.box_size)
            ]
            return bounds
        
        # Иначе вычисляем границы для каждого воркера
        for worker_id in range(self.num_workers):
            i = worker_id // (self.dimensions[1] * self.dimensions[2])
            remainder = worker_id % (self.dimensions[1] * self.dimensions[2])
            j = remainder // self.dimensions[2]
            k = remainder % self.dimensions[2]
            
            x_min = i * self.cell_sizes[0]
            x_max = (i + 1) * self.cell_sizes[0]
            y_min = j * self.cell_sizes[1]
            y_max = (j + 1) * self.cell_sizes[1]
            z_min = k * self.cell_sizes[2]
            z_max = (k + 1) * self.cell_sizes[2]
            
            bounds[worker_id] = [
                (x_min, x_max),
                (y_min, y_max),
                (z_min, z_max)
            ]
        return bounds

    def get_worker_bounds(self, worker_id: int) -> List[Tuple[float, float]]:
        return self.worker_bounds[worker_id]