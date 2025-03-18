from pycuda.compiler import SourceModule
import pycuda.driver as cuda
import numpy as np


class DynamicKernelLoader:
    def __init__(self):
        self.mod = None
        self.update_func = None
        self.collision_func = None
        self.predict_func = None
        
    def compile(self, kernel_code):
        try:
            self.mod = SourceModule(kernel_code)
            self.update_func = self.mod.get_function("update_particles")
            return self
        except Exception as e:
            raise RuntimeError(f"Kernel compilation error: {e}")
            
    def execute(self, positions, velocities, accelerations, masses, radii, N, dt, box_size, prediction_steps=1):
        # Конвертация в contiguous arrays
        positions = np.ascontiguousarray(positions, dtype=np.float32).reshape(-1, 3)
        velocities = np.ascontiguousarray(velocities, dtype=np.float32).reshape(-1, 3)
        accelerations = np.ascontiguousarray(accelerations, dtype=np.float32).reshape(-1, 3)
        masses = np.ascontiguousarray(masses, dtype=np.float32)
        radii = np.ascontiguousarray(radii, dtype=np.float32)

        # Выделение памяти на GPU
        d_pos = cuda.mem_alloc(positions.nbytes)
        d_vel = cuda.mem_alloc(velocities.nbytes)
        d_acc = cuda.mem_alloc(accelerations.nbytes)
        d_mass = cuda.mem_alloc(masses.nbytes)
        d_rad = cuda.mem_alloc(radii.nbytes)

        # Копирование данных
        cuda.memcpy_htod(d_pos, positions)
        cuda.memcpy_htod(d_vel, velocities)
        cuda.memcpy_htod(d_acc, accelerations)
        cuda.memcpy_htod(d_mass, masses)
        cuda.memcpy_htod(d_rad, radii)

        # Вызов последовательности ядер
        block = (256, 1, 1)
        grid = ((N + block[0] - 1) // block[0], 1)
        shared_mem_size = block[0] * 4 * 4  # float4 (16 bytes) * threads per block

        # 1. Обновление частиц
        self.update_func(
            d_pos, d_vel, d_acc, d_mass, d_rad,
            np.int32(N), np.float32(dt), np.float32(box_size),
            block=block, grid=grid, shared=shared_mem_size
        )
        cuda.Context.synchronize()

        # Получение текущих данных
        new_pos = np.empty_like(positions)
        new_vel = np.empty_like(velocities)
        new_acc = np.empty_like(accelerations)
        
        cuda.memcpy_dtoh(new_pos, d_pos)
        cuda.memcpy_dtoh(new_vel, d_vel)
        cuda.memcpy_dtoh(new_acc, d_acc)

        # Освобождение памяти
        d_pos.free()
        d_vel.free()
        d_acc.free()
        d_mass.free()
        d_rad.free()
        
        return new_pos, new_vel, new_acc
