// main.cpp
#include "struct.h"
#include <iostream>
#include <unistd.h> // Для sleep()
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>
#include <set>
#include <csignal> // Для signal()
#include <atomic>  // Для управления состоянием программы
#include <boost/asio.hpp>
#include <mutex>
#include <thread>
#include <chrono>
#include <nlohmann/json.hpp> // Для работы с JSON

using json = nlohmann::json;

std::mutex clients_mutex;
std::atomic<bool> running(true); // Флаг для управления циклом
using namespace websocketpp;
typedef server<config::asio> server_t;
server_t echo_server; // Делаем сервер глобальным
std::set<connection_hdl, std::owner_less<connection_hdl>> connected_clients;

// Обработчик сигналов
void signal_handler(int signal) {
    std::cout << "\nReceived signal " << signal << ", shutting down..." << std::endl;
    running = false;  // Останавливаем основной цикл
    echo_server.stop();  // Останавливаем WebSocket-сервер
}

void sendFrame(int t) {
    Particle* hostParticles = new Particle[simParams.N];
    cudaError_t err = cudaMemcpy(hostParticles, futureParticles, simParams.N * sizeof(Particle), cudaMemcpyDeviceToHost);
    if (err != cudaSuccess) {
        printf("CUDA error: %s\n", cudaGetErrorString(err));
        delete[] hostParticles;
        return;
    }

    json msg;
    msg["type"] = "frame";
    msg["t"] = t;
    msg["particles"] = json::array();
    for (int i = 0; i < simParams.N; ++i) {
        msg["particles"].push_back({
            {"x", hostParticles[i].position.x},
            {"y", hostParticles[i].position.y},
            {"z", hostParticles[i].position.z},
            {"mass", hostParticles[i].mass},
            {"velocity", {
                {"x", hostParticles[i].velocity.x},
                {"y", hostParticles[i].velocity.y},
                {"z", hostParticles[i].velocity.z}
            }},
            {"acceleration", {
                {"x", hostParticles[i].acceleration.x},
                {"y", hostParticles[i].acceleration.y},
                {"z", hostParticles[i].acceleration.z}
            }},
            {"radius", hostParticles[i].radius}
        });
    }

    std::lock_guard<std::mutex> lock(clients_mutex);
    for (auto& hdl : connected_clients) {
        echo_server.send(hdl, msg.dump(), websocketpp::frame::opcode::text);
    }

    delete[] hostParticles;
}

void on_open(connection_hdl hdl) {
    std::lock_guard<std::mutex> lock(clients_mutex);
    connected_clients.insert(hdl);
}

void on_close(connection_hdl hdl) {
    std::lock_guard<std::mutex> lock(clients_mutex);
    connected_clients.erase(hdl);
}

void on_message(connection_hdl hdl, server_t::message_ptr msg) {
    // std::cout << "Received message" << std::endl;
}

int main() {
    // Подключаем обработчики сигналов
    std::signal(SIGINT, signal_handler);  // Ctrl+C
    std::signal(SIGTERM, signal_handler); // pm2 stop

    echo_server.init_asio();
    echo_server.set_reuse_addr(true); // Позволяет повторное использование порта
    echo_server.set_open_handler(&on_open);
    echo_server.set_close_handler(&on_close);
    echo_server.set_message_handler(&on_message);

    std::string ip_address = "127.0.0.1"; // Используем localhost
    short port = 9003;

    // Запускаем сервер прослушивания в основном потоке
    echo_server.listen(boost::asio::ip::tcp::endpoint(boost::asio::ip::address::from_string(ip_address), port));
    echo_server.start_accept(); // Убедимся, что сервер начал принимать соединения

    // Запускаем сервер в отдельном потоке
    std::thread server_thread([&]() {
        echo_server.run();
    });

    // Даем серверу время запуститься (можно заменить на более изящный вариант)
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Инициализация WebSocket аналогично вашему коду
    Params params;
    params.N = 100;
    params.dt = 0.1f;
    params.predictionSteps = 1;
    params.boxSize = 100.0f;

    initialize(params);

    for(int t = 0; running; ++t) {
        stepSimulation(t);
        sendFrame(t);
    }

    cleanup();
    return 0;
}