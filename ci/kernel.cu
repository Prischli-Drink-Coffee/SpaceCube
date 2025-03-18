// kernel.cu
#include "struct.h" // Add this line

// Define global variables
Params simParams;
Particle* particles;
Particle* futureParticles;

__device__ float3 calculateForce(const Particle& p1, const Particle& p2) {
    const float SOFTENING = 1.3f; // Новый параметр для смягчения
    float3 r = {p2.position.x - p1.position.x, 
                p2.position.y - p1.position.y, 
                p2.position.z - p1.position.z};
    
    float distanceSq = r.x*r.x + r.y*r.y + r.z*r.z + SOFTENING*SOFTENING;
    float distance = sqrtf(distanceSq);
    
    float forceMagnitude = (p1.mass * p2.mass) / (distanceSq * distance);
    return {r.x * forceMagnitude, 
            r.y * forceMagnitude, 
            r.z * forceMagnitude};
}

__device__ float3 calculateWallForce(const Particle& p, float boxSize) {
    float3 force = {0.0f, 0.0f, 0.0f};
    const float wallStiffness = 0.01f; // Сила отталкивания от стенки

    // Левая стенка (x = -boxSize)
    float overlap = -boxSize - p.position.x;
    if (overlap > 0) {
        force.x += wallStiffness * overlap;
    }

    // Правая стенка (x = boxSize)
    overlap = p.position.x - boxSize;
    if (overlap > 0) {
        force.x -= wallStiffness * overlap;
    }

    // Нижняя стенка (y = -boxSize)
    overlap = -boxSize - p.position.y;
    if (overlap > 0) {
        force.y += wallStiffness * overlap;
    }

    // Верхняя стенка (y = boxSize)
    overlap = p.position.y - boxSize;
    if (overlap > 0) {
        force.y -= wallStiffness * overlap;
    }

    // Задняя стенка (z = -boxSize)
    overlap = -boxSize - p.position.z;
    if (overlap > 0) {
        force.z += wallStiffness * overlap;
    }

    // Передняя стенка (z = boxSize)
    overlap = p.position.z - boxSize;
    if (overlap > 0) {
        force.z -= wallStiffness * overlap;
    }

    return force;
}


__device__ void resolveCollision(Particle& p1, Particle& p2) {
    const float MIN_DISTANCE = 1e-4f;
    float3 delta = {
        p1.position.x - p2.position.x,
        p1.position.y - p2.position.y,
        p1.position.z - p2.position.z
    };
    
    float distanceSq = delta.x*delta.x + delta.y*delta.y + delta.z*delta.z;
    if(distanceSq < 1e-12f) return; // Защита от нулевого расстояния
    
    float distance = sqrtf(distanceSq);
    float minDistance = p1.radius + p2.radius + MIN_DISTANCE;
    
    if (distance < minDistance && distance > 1e-6f) {
        // Нормализованный вектор столкновения
        float3 normal = {delta.x/distance, delta.y/distance, delta.z/distance};
        
        // Относительная скорость
        float3 relVelocity = {
            p1.velocity.x - p2.velocity.x,
            p1.velocity.y - p2.velocity.y,
            p1.velocity.z - p2.velocity.z
        };
        
        // Импульс столкновения
        float impulse = 2.0f * (relVelocity.x*normal.x + relVelocity.y*normal.y + relVelocity.z*normal.z) /
                       (1.0f/p1.mass + 1.0f/p2.mass);
        
        // Обновляем скорости
        p1.velocity.x -= impulse * normal.x / p1.mass;
        p1.velocity.y -= impulse * normal.y / p1.mass;
        p1.velocity.z -= impulse * normal.z / p1.mass;
        
        p2.velocity.x += impulse * normal.x / p2.mass;
        p2.velocity.y += impulse * normal.y / p2.mass;
        p2.velocity.z += impulse * normal.z / p2.mass;
        
        // Корректировка позиций для устранения пересечения
        float overlap = 0.5f * (minDistance - distance);
        p1.position.x += overlap * normal.x;
        p1.position.y += overlap * normal.y;
        p1.position.z += overlap * normal.z;
        
        p2.position.x -= overlap * normal.x;
        p2.position.y -= overlap * normal.y;
        p2.position.z -= overlap * normal.z;
    }
}


__global__ void updateParticles(Particle* particles, int N, float dt, float boxSize) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= N) return;

    Particle& p = particles[idx];
    float3 force = {0.0f, 0.0f, 0.0f};

    // Сохраняем начальное ускорение
    float3 initial_accel = p.acceleration;

    // Расчет сил взаимодействия
    for (int i = 0; i < N; ++i) {
        if (i != idx) {
            float3 f = calculateForce(p, particles[i]);
            force.x += f.x;
            force.y += f.y;
            force.z += f.z;
        }
    }

    // Добавляем силу от стенок
    float3 wallForce = calculateWallForce(p, boxSize);
    force.x += wallForce.x;
    force.y += wallForce.y;
    force.z += wallForce.z;

    // Общее ускорение = начальное + от сил
    p.acceleration.x = initial_accel.x + (force.x / p.mass);
    p.acceleration.y = initial_accel.y + (force.y / p.mass);
    p.acceleration.z = initial_accel.z + (force.z / p.mass);

    // Интегрируем скорость с учетом ускорения
    p.velocity.x += p.acceleration.x * dt;
    p.velocity.y += p.acceleration.y * dt;
    p.velocity.z += p.acceleration.z * dt;

    // Применяем демпфирование ПОСЛЕ интегрирования
    const float DAMPING = 0.999f;
    p.velocity.x *= DAMPING;
    p.velocity.y *= DAMPING;
    p.velocity.z *= DAMPING;

    // Ограничение скорости
    const float MAX_SPEED = 100.0f;
    float speed = sqrtf(p.velocity.x*p.velocity.x + 
                       p.velocity.y*p.velocity.y + 
                       p.velocity.z*p.velocity.z);
    if(speed > MAX_SPEED) {
        p.velocity.x *= MAX_SPEED/speed;
        p.velocity.y *= MAX_SPEED/speed;
        p.velocity.z *= MAX_SPEED/speed;
    }

    // Обновление позиции
    p.position.x += p.velocity.x * dt;
    p.position.y += p.velocity.y * dt;
    p.position.z += p.velocity.z * dt;

    // Обработка столкновений
    for (int i = 0; i < N; ++i) {
        if (i != idx) {
            resolveCollision(p, particles[i]);
        }
    }
}


__global__ void handleCollisions(Particle* particles, int N) {
    extern __shared__ Particle sharedParticles[];
    
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= N) return;
    
    // Загрузка частиц в shared memory
    sharedParticles[threadIdx.x] = particles[idx];
    __syncthreads();
    
    // Проверка столкновений внутри блока
    for (int i = 0; i < blockDim.x; ++i) {
        if (i != threadIdx.x) {
            resolveCollision(sharedParticles[threadIdx.x], sharedParticles[i]);
        }
    }
    
    // Сохранение обратно в глобальную память
    particles[idx] = sharedParticles[threadIdx.x];
}

__global__ void predictFuture(Particle* current, Particle* future, int N, float dt, int steps) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= N) return;

    Particle p = current[idx];
    for (int i = 0; i < steps; ++i) {
        float3 total_accel = p.acceleration; // Сохраняем начальное ускорение
        
        // Расчет сил взаимодействия
        float3 force = {0.0f, 0.0f, 0.0f};
        for (int j = 0; j < N; ++j) {
            if (j != idx) {
                float3 f = calculateForce(p, current[j]);
                force.x += f.x;
                force.y += f.y;
                force.z += f.z;
            }
        }
        
        // Добавляем ускорение от сил
        total_accel.x += force.x / p.mass;
        total_accel.y += force.y / p.mass;
        total_accel.z += force.z / p.mass;

        // Интегрируем скорость
        p.velocity.x += total_accel.x * dt;
        p.velocity.y += total_accel.y * dt;
        p.velocity.z += total_accel.z * dt;

        // Обновляем позицию
        p.position.x += p.velocity.x * dt;
        p.position.y += p.velocity.y * dt;
        p.position.z += p.velocity.z * dt;
    }
    future[idx] = p;
}

void stepSimulation(int t) {
    dim3 block(256);
    dim3 grid((simParams.N + block.x - 1) / block.x);

    updateParticles<<<grid, block>>>(particles, simParams.N, simParams.dt, simParams.boxSize);
    cudaError_t err = cudaGetLastError();
    if (err != cudaSuccess) printf("Error in updateParticles: %s\n", cudaGetErrorString(err));

    handleCollisions<<<grid, block, block.x*sizeof(Particle)>>>(particles, simParams.N);
    err = cudaGetLastError();
    if (err != cudaSuccess) printf("Error in handleCollisions: %s\n", cudaGetErrorString(err));

    predictFuture<<<grid, block>>>(particles, futureParticles, simParams.N, simParams.dt, simParams.predictionSteps);
    err = cudaGetLastError();
    if (err != cudaSuccess) printf("Error in predictFuture: %s\n", cudaGetErrorString(err));

    cudaDeviceSynchronize();
}

void initialize(const Params& params) {
    simParams = params;

    cudaMalloc(&particles, params.N * sizeof(Particle));
    cudaMalloc(&futureParticles, params.N * sizeof(Particle));

    Particle* hostParticles = new Particle[params.N];
    for (int i = 0; i < params.N; ++i) {
        // Более безопасное распределение масс и радиусов
        hostParticles[i].mass = 0.6f + 0.2f * static_cast<float>(rand())/RAND_MAX;
        hostParticles[i].radius = 1.0f * cbrtf(hostParticles[i].mass);
        
        hostParticles[i].position = {
            (static_cast<float>(rand()) / RAND_MAX - 0.5f) * simParams.boxSize,
            (static_cast<float>(rand()) / RAND_MAX - 0.5f) * simParams.boxSize,
            (static_cast<float>(rand()) / RAND_MAX - 0.5f) * simParams.boxSize
        };
        // Инициализация скорости (случайные значения от -1 до 1)
        hostParticles[i].velocity = {
            (static_cast<float>(rand()) / RAND_MAX * 2.0f - 1.0f) * 10.0f, // Увеличено в 100 раз
            (static_cast<float>(rand()) / RAND_MAX * 2.0f - 1.0f) * 10.0f,
            (static_cast<float>(rand()) / RAND_MAX * 2.0f - 1.0f) * 10.0f
        };

        // Инициализация ускорения (случайные значения от -0.01 до 0.01)
        hostParticles[i].acceleration = {
            (static_cast<float>(rand()) / RAND_MAX * 2.0f - 1.0f) * 0.01f,
            (static_cast<float>(rand()) / RAND_MAX * 2.0f - 1.0f) * 0.01f,
            (static_cast<float>(rand()) / RAND_MAX * 2.0f - 1.0f) * 0.01f
        };
    }
    cudaMemcpy(particles, hostParticles, params.N * sizeof(Particle), cudaMemcpyHostToDevice);
    delete[] hostParticles;
}

void cleanup() {
    cudaFree(particles);
    cudaFree(futureParticles);
}