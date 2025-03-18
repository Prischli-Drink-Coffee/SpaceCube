# scr/scripts/particle_sim.py

import numpy as np
import pycuda.autoinit
from pycuda import gpuarray, compiler
from pycuda.compiler import SourceModule
import json
import signal
import sys

class SimParams:
    def __init__(self, N=1000, dt=0.5, prediction_steps=1, box_size=100.0):
        self.N = N
        self.dt = np.float32(dt)
        self.prediction_steps = prediction_steps
        self.box_size = np.float32(box_size)
        
class ParticleSimulator:
    def __init__(self, params):
        self.params = params
        self._compile_kernels()
        self._init_particles()
        
    def _compile_kernels(self):
        kernel_code = """
        #include <math.h>
        #define SOFTENING 1.3f
        #define MIN_DISTANCE 1e-4f
        #define DAMPING 0.999f
        #define MAX_SPEED 100.0f
        #define WALL_STIFFNESS 0.01f

        __device__ float3 calculate_force(float3 p1_pos, float3 p1_vel, float3 p1_acc, float mass1,
                                          float3 p2_pos, float3 p2_vel, float3 p2_acc, float mass2) {
            float3 r;
            r.x = p2_pos.x - p1_pos.x;
            r.y = p2_pos.y - p1_pos.y;
            r.z = p2_pos.z - p1_pos.z;
            
            float distanceSq = r.x*r.x + r.y*r.y + r.z*r.z + SOFTENING*SOFTENING;
            float distance = sqrtf(distanceSq);
            float forceMagnitude = (mass1 * mass2) / (distanceSq * distance);
            return (float3){r.x * forceMagnitude, r.y * forceMagnitude, r.z * forceMagnitude};
        }

        __device__ float3 calculate_wall_force(float3 pos, float box_size) {
            float3 force = {0.0f, 0.0f, 0.0f};
            float overlap;
            
            // X walls
            overlap = -box_size - pos.x;
            if (overlap > 0) force.x += WALL_STIFFNESS * overlap;
            overlap = pos.x - box_size;
            if (overlap > 0) force.x -= WALL_STIFFNESS * overlap;
            
            // Y walls
            overlap = -box_size - pos.y;
            if (overlap > 0) force.y += WALL_STIFFNESS * overlap;
            overlap = pos.y - box_size;
            if (overlap > 0) force.y -= WALL_STIFFNESS * overlap;
            
            // Z walls
            overlap = -box_size - pos.z;
            if (overlap > 0) force.z += WALL_STIFFNESS * overlap;
            overlap = pos.z - box_size;
            if (overlap > 0) force.z -= WALL_STIFFNESS * overlap;
            
            return force;
        }

        __device__ void resolve_collision(float3* pos1, float3* vel1, float radius1, float mass1,
                                          float3* pos2, float3* vel2, float radius2, float mass2) {
            float3 delta = {pos1->x - pos2->x, pos1->y - pos2->y, pos1->z - pos2->z};
            float distanceSq = delta.x*delta.x + delta.y*delta.y + delta.z*delta.z;
            float minDistance = radius1 + radius2 + MIN_DISTANCE;
            
            if (distanceSq < minDistance * minDistance && distanceSq > 1e-6f) {
                float distance = sqrtf(distanceSq);
                float3 normal = {delta.x/distance, delta.y/distance, delta.z/distance};
                
                float3 relVelocity = {vel1->x - vel2->x, vel1->y - vel2->y, vel1->z - vel2->z};
                float impulse = 2.0f * (relVelocity.x*normal.x + relVelocity.y*normal.y + relVelocity.z*normal.z) /
                               (1.0f/mass1 + 1.0f/mass2);
                
                vel1->x -= impulse * normal.x / mass1;
                vel1->y -= impulse * normal.y / mass1;
                vel1->z -= impulse * normal.z / mass1;
                
                vel2->x += impulse * normal.x / mass2;
                vel2->y += impulse * normal.y / mass2;
                vel2->z += impulse * normal.z / mass2;
                
                float overlap = 0.5f * (minDistance - distance);
                pos1->x += overlap * normal.x;
                pos1->y += overlap * normal.y;
                pos1->z += overlap * normal.z;
                
                pos2->x -= overlap * normal.x;
                pos2->y -= overlap * normal.y;
                pos2->z -= overlap * normal.z;
            }
        }

        __global__ void update_particles(
            float3* positions, float3* velocities, float3* accelerations,
            float* masses, float* radii,
            int N, float dt, float box_size) {
            
            int idx = blockIdx.x * blockDim.x + threadIdx.x;
            if (idx >= N) return;
            
            float3 pos = positions[idx];
            float3 vel = velocities[idx];
            float3 acc = accelerations[idx];
            float mass = masses[idx];
            float radius = radii[idx];
            
            float3 total_force = {0.0f, 0.0f, 0.0f};
            
            // Calculate inter-particle forces
            for (int i = 0; i < N; i++) {
                if (i != idx) {
                    float3 f = calculate_force(
                        pos, vel, acc, mass,
                        positions[i], velocities[i], accelerations[i], masses[i]
                    );
                    total_force.x += f.x;
                    total_force.y += f.y;
                    total_force.z += f.z;
                }
            }
            
            // Add wall forces
            float3 wall_force = calculate_wall_force(pos, box_size);
            total_force.x += wall_force.x;
            total_force.y += wall_force.y;
            total_force.z += wall_force.z;
            
            // Update acceleration
            acc.x += total_force.x / mass;
            acc.y += total_force.y / mass;
            acc.z += total_force.z / mass;
            
            // Update velocity
            vel.x += acc.x * dt;
            vel.y += acc.y * dt;
            vel.z += acc.z * dt;
            
            // Apply damping
            vel.x *= DAMPING;
            vel.y *= DAMPING;
            vel.z *= DAMPING;
            
            // Speed limit
            float speed = sqrtf(vel.x*vel.x + vel.y*vel.y + vel.z*vel.z);
            if (speed > MAX_SPEED) {
                vel.x *= MAX_SPEED / speed;
                vel.y *= MAX_SPEED / speed;
                vel.z *= MAX_SPEED / speed;
            }
            
            // Update position
            pos.x += vel.x * dt;
            pos.y += vel.y * dt;
            pos.z += vel.z * dt;
            
            // Handle collisions
            for (int i = 0; i < N; i++) {
                if (i != idx) {
                    resolve_collision(
                        &pos, &vel, radius, mass,
                        &positions[i], &velocities[i], radii[i], masses[i]
                    );
                }
            }
            
            // Save changes
            positions[idx] = pos;
            velocities[idx] = vel;
            accelerations[idx] = acc;
        }
        """
        self.mod = SourceModule(kernel_code)
        self.update_particles = self.mod.get_function("update_particles")
        
    def _init_particles(self):
        N = self.params.N
        # Initialize arrays on host
        positions = np.zeros((N, 3), dtype=np.float32)
        velocities = np.zeros((N, 3), dtype=np.float32)
        accelerations = np.zeros((N, 3), dtype=np.float32)
        masses = np.zeros(N, dtype=np.float32)
        radii = np.zeros(N, dtype=np.float32)
        
        for i in range(N):
            masses[i] = 0.6 + 0.2 * np.random.rand()
            radii[i] = 1.0 * (masses[i] ** (1/3))
            
            positions[i] = (
                (np.random.rand(3) - 0.5) * self.params.box_size
            )
            velocities[i] = (np.random.rand(3) * 2 - 1) * 10.0
            accelerations[i] = (np.random.rand(3) * 2 - 1) * 0.01
            
        # Transfer to GPU
        self.d_positions = gpuarray.to_gpu(positions)
        self.d_velocities = gpuarray.to_gpu(velocities)
        self.d_accelerations = gpuarray.to_gpu(accelerations)
        self.d_masses = gpuarray.to_gpu(masses)
        self.d_radii = gpuarray.to_gpu(radii)
        
    def step(self):
        block = (256, 1, 1)
        grid = ((self.params.N + block[0] - 1) // block[0], 1)
        
        self.update_particles(
            self.d_positions, self.d_velocities, self.d_accelerations,
            self.d_masses, self.d_radii,
            np.int32(self.params.N), self.params.dt, self.params.box_size,
            block=block, grid=grid)
        
    def get_frame(self):
        return {
            'positions': self.d_positions.get().tolist(),
            'velocities': self.d_velocities.get().tolist(),
            'masses': self.d_masses.get().tolist(),
            'radii': self.d_radii.get().tolist()
        }