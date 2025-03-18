// struct.h
#pragma once
#include <cuda.h>
#include <cuda_runtime.h>
#include <cuComplex.h>
#include <vector>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

struct Particle {
    float3 position;
    float3 velocity;
    float3 acceleration;
    float mass;
    float radius;
};


struct Params {
    int N = 1000; // Количество частиц
    float dt = 0.5f; // Шаг по времени
    int predictionSteps = 1; // Количество шагов предсказания
    float boxSize = 100.0f; // Размер куба

    void print() const {
        printf("Simulation params:\n");
        printf("N: %d, dt: %.3f, predictionSteps: %d, boxSize: %.3f\n", N, dt, predictionSteps, boxSize);
    }
};

// Declare global variables as extern
extern Params simParams;
extern Particle* particles;
extern Particle* futureParticles;

extern "C" {
    void initialize(const Params& params);
    void stepSimulation(int t);
    void cleanup(); // Add this line
}