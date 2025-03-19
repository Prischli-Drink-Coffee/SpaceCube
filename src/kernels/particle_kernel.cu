#include <math.h>
#define SOFTENING 1.3f
#define MIN_DISTANCE 1e-4f
#define DAMPING 0.7f
#define MAX_SPEED 100.0f
#define WALL_STIFFNESS 1.5f

__device__ float3 calculate_force(float3 p1_pos, float mass1, float3 p2_pos, float mass2) {
    float3 r = {
        p2_pos.x - p1_pos.x,
        p2_pos.y - p1_pos.y,
        p2_pos.z - p1_pos.z
    };
    float distanceSq = r.x*r.x + r.y*r.y + r.z*r.z + SOFTENING*SOFTENING;
    float distance = sqrtf(distanceSq);
    float forceMagnitude = (mass1 * mass2) / (distanceSq * distance);
    return {r.x * forceMagnitude, r.y * forceMagnitude, r.z * forceMagnitude};
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
    float* masses, float* radii, int* local_flags,
    int N, float dt, float box_size) {
    
    extern __shared__ float4 shared_data[];
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= N || local_flags[idx] == 0) return; // Пропуск нелокальных частиц
    
    // Загрузка данных текущей частицы
    float3 pos = positions[idx];
    float3 vel = velocities[idx];
    float3 acc = accelerations[idx];
    float mass = masses[idx];
    float radius = radii[idx];
    
    float3 total_force = {0.0f, 0.0f, 0.0f};
    const int tiles = (N + blockDim.x - 1) / blockDim.x;
    
    for (int tile = 0; tile < tiles; ++tile) {
        // Загрузка данных в shared memory
        int load_idx = tile * blockDim.x + threadIdx.x;
        float4 p_data;
        if (load_idx < N) {
            p_data.x = positions[load_idx].x;
            p_data.y = positions[load_idx].y;
            p_data.z = positions[load_idx].z;
            p_data.w = masses[load_idx];
        } else {
            p_data = make_float4(0.0f, 0.0f, 0.0f, 0.0f);
        }
        shared_data[threadIdx.x] = p_data;
        __syncthreads();
        
        // Расчет сил для текущего тайла
        for (int i = 0; i < blockDim.x; ++i) {
            int particle_idx = tile * blockDim.x + i;
            if (particle_idx >= N || particle_idx == idx) continue;
            
            float4 p2 = shared_data[i];
            float3 p2_pos = {p2.x, p2.y, p2.z};
            float3 force = calculate_force(pos, mass, p2_pos, p2.w);
            
            total_force.x += force.x;
            total_force.y += force.y;
            total_force.z += force.z;
        }
        __syncthreads();
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
