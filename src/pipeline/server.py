# orchestrator/src/pipeline/server.py
import asyncio
import hashlib
import json
import os
from typing import List, Dict
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from kafka.errors import KafkaError
import base64
from src.scripts.simulation_init import ParticleInitializer
from src.shared.schemas import ParticleData, KernelUpdate, SimulationControl
from src.shared.utils import serialize_particles, get_env_value, deserialize_particles
from src.scripts.websocket import WebSocketServer
from src.scripts.grid_manager import GridManager
from src.utils.custom_logging import setup_logging
from src import path_to_project
from env import Env
import time


log = setup_logging()
env = Env()


def _create_admin_client(kafka_brokers):
    """Создание Admin Client"""
    try:
        return KafkaAdminClient(
            bootstrap_servers=kafka_brokers,
            client_id='orchestrator_admin'
        )
    except Exception as e:
        log.error(f"Failed to create Admin Client: {e}")
        raise


def _create_kafka_consumer(kafka_brokers):
    """Создание Kafka Consumer"""
    try:
        return KafkaConsumer(
            'particle_updates',
            bootstrap_servers=kafka_brokers,
            value_deserializer=lambda v: json.loads(v.decode()),
            group_id='orchestrator-updates',
            auto_offset_reset='earliest'
        )
    except Exception as e:
        log.error(f"Failed to create Kafka Consumer: {e}")
        raise

def _create_kafka_producer(kafka_brokers):
    """Создание Kafka Producer"""
    try:
        return KafkaProducer(
            bootstrap_servers=kafka_brokers,
            value_serializer=lambda v: v,  # Оставляем как есть
            key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode('utf-8'),
            max_request_size=52428800,
            request_timeout_ms=30000,
            retries=5,
            retry_backoff_ms=1000
        )
    except Exception as e:
        log.error(f"Failed to create Kafka Producer: {e}")
        raise



class Orchestrator:
    def __init__(self):

        # Инициализируем переменные среды
        self.running = False
        self.ws_host = get_env_value(env, 'WS_HOST', required=True)
        self.ws_port = get_env_value(env, 'WS_PORT', required=True, type_cast=int)
        self.kafka_brokers = get_env_value(env, 'KAFKA_BROKERS', required=True, type_cast=lambda x: x.split(','))
        self.num_workers = int(get_env_value(env, 'NUM_WORKERS', required=True))
        self.box_size = get_env_value(env, 'BOX_SIZE', default=100.0, type_cast=float)
        self.num_particles = get_env_value(env, 'NUM_PARTICLES', default=10000, type_cast=int)
        self.dt = get_env_value(env, 'DT', default=0.01, type_cast=float)
        self.path_kernel = os.path.join(path_to_project(), 'src/kernels/particle_kernel.cu')
        # Инициализация нулевого набора частиц
        self.particle_init = ParticleInitializer(self.box_size)
        self.particles = self.particle_init.generate(self.num_particles)
        self.particles = [ParticleData(**p) for p in self.particles]
        # Инициализация сокета
        self.ws_server = WebSocketServer(host=self.ws_host, port=self.ws_port)
        # Инициализация грид менеджера
        self.grid = GridManager(box_size=self.box_size, num_workers=self.num_workers)
        # Админ клиент для взаимодействия с кафкой
        self.admin_client = _create_admin_client(self.kafka_brokers)
        # Обработка обновлений
        self.kafka_consumer = _create_kafka_consumer(self.kafka_brokers)
        # Инициализация продюссера
        self.kafka_producer = _create_kafka_producer(self.kafka_brokers)
        # Пересоздаем топики
        self.topics_config = [
            {'name': 'kernel_updates', 'partitions': self.num_workers, 'replication_factor': 1},
            {'name': 'simulation_control', 'partitions': self.num_workers, 'replication_factor': 1},
            {'name': 'particle_updates', 'partitions': self.num_workers, 'replication_factor': 1},
            {'name': 'particle_chunks', 'partitions': self.num_workers, 'replication_factor': 1},
        ]
        self.recreate_kafka_topics(self.kafka_brokers, self.topics_config)
        # Логируем старт оркестратора
        log.info("Orchestrator started successfully")

    def recreate_kafka_topics(self, kafka_brokers, topics_config):

        try:
            # Удаляем существующие топики, даже если они есть
            topics_to_delete = [topic['name'] for topic in topics_config]
            
            if topics_to_delete:
                try:
                    self.admin_client.delete_topics(topics_to_delete)
                    log.info(f"Deleted topics: {topics_to_delete}")
                    # Ждем, пока топики будут удалены
                    time.sleep(20)
                except UnknownTopicOrPartitionError:
                    log.warning("Some topics do not exist, skipping deletion.")
                except Exception as e:
                    log.error(f"Failed to delete topics: {e}")
                    raise

            # Создаем топики заново
            new_topics = [
                NewTopic(
                    name=topic['name'],
                    num_partitions=topic['partitions'],
                    replication_factor=topic['replication_factor']
                )
                for topic in topics_config
            ]
            
            self.admin_client.create_topics(new_topics=new_topics, validate_only=False)
            log.info(f"Created topics: {[t['name'] for t in topics_config]}")

        except TopicAlreadyExistsError:
            log.warning("Some topics already exist, skipping creation.")
        except Exception as e:
            log.error(f"Failed to recreate Kafka topics: {e}")


    async def purge_topics(self):
        """Очистка топиков путем изменения retention.ms"""
        topics_to_purge = [
            'kernel_updates',
            'simulation_control',
            'particle_updates',
            'particle_chunks'
        ]
        try:
            for topic in topics_to_purge:
                # Устанавливаем retention.ms на 1 секунду
                config_resource = ConfigResource(
                    resource_type=ConfigResourceType.TOPIC,
                    name=topic,
                    configs={'retention.ms': '1000'}
                )
                self.admin_client.alter_configs([config_resource])
                log.info(f"Set retention.ms=1000 for topic: {topic}")

            # Ждем, пока Kafka удалит сообщения
            await asyncio.sleep(20)

            # Восстанавливаем стандартное значение retention.ms
            for topic in topics_to_purge:
                config_resource = ConfigResource(
                    resource_type=ConfigResourceType.TOPIC,
                    name=topic,
                    configs={'retention.ms': '604800000'}  # 7 дней (стандартное значение)
                )
                self.admin_client.alter_configs([config_resource])
                log.info(f"Restored retention.ms for topic: {topic}")

        except Exception as e:
            log.error(f"Failed to purge topics: {e}")
        finally:
            self.admin_client.close()

    async def run(self):
        """Основной цикл выполнения"""
        try:
            # # очистка топиков перед запуском
            await self.purge_topics()
            # Инициализация WebSocket сервера
            self.ws_server = WebSocketServer(host=self.ws_host, port=self.ws_port)
            server_task = asyncio.create_task(self.ws_server.run_server())
            log.info(f"WebSocket server state: {not server_task.done()}")
            # Даем серверу время на запуск
            await asyncio.sleep(2)
            # Инициализация симуляции
            await self.initialize_simulation()
            # Цикл оркестрации
            await self.run_simulation()
        except Exception as e:
            log.error(f"Critical error", exc_info=e)
        finally:
            await self.ws_server.shutdown()
            if 'server_task' in locals():
                server_task.cancel()
                await server_task

    async def initialize_simulation(self):
        """Полный цикл инициализации симуляции"""
        try:
            log.info("Initialize")
            # 1. Отправка ядра
            await self.send_kernel_update()
            # 2. Явная задержка для гарантии доставки
            await asyncio.sleep(10)
            # 3. Запуск симуляции
            await self.start_simulation()
        except Exception as e:
            log.error(f"Simulation initialization failed", exc_info=e)
            raise

    async def send_kernel_update(self):
        """Отправка CUDA ядра всем воркерам"""
        try:
            log.info(f"Trying to read kernel from: {self.path_kernel}")
            if not os.path.exists(self.path_kernel):
                raise FileNotFoundError(f"Kernel file not found at {self.path_kernel}")
                
            with open(self.path_kernel, 'r') as f:
                kernel_code = f.read()
            
            # Создаем сообщение с правильной сериализацией
            message = KernelUpdate(
                code=kernel_code,
                version=hashlib.sha256(kernel_code.encode()).hexdigest()[:8]
            ).model_dump()  # Используем model_dump() вместо dict()
            message["type"] = "kernel_update"
            
            # Сериализуем в JSON и кодируем в bytes
            serialized_message = json.dumps(message).encode('utf-8')

            log.info(f"Sending kernel to all workers")
            future = self.kafka_producer.send(
                'kernel_updates',
                value=serialized_message  # Отправляем одно сообщение всем воркерам
            )
            future.get(timeout=20)  # Ждем подтверждения доставки
            
            log.info(f"Kernel update sent to all workers")
            
        except Exception as e:
            log.error("Failed to send kernel update", exc_info=True)
            raise

    async def start_simulation(self):
        """Запуск симуляции с отправкой индивидуальных сообщений каждому воркеру"""
        try:
            # Создаем список для хранения futures (для отслеживания отправки сообщений)
            futures = []

            # Проходим по каждому воркеру
            for worker_id in range(self.num_workers):
                # Получаем границы ответственности для текущего воркера
                worker_bounds = self.grid.get_worker_bounds(worker_id)
                
                # Создаем сообщение с командой старта и границами
                control_data = SimulationControl(
                    command='start',
                    dt=self.dt,
                    box_size=self.box_size,
                    bounds=worker_bounds,  # Передаем границы ответственности
                    worker_id=worker_id,  # Указываем ID воркера
                ).model_dump()
                
                # Добавляем тип сообщения
                control_data["type"] = "control"
                
                # Сериализуем в JSON и кодируем в bytes
                control_msg = json.dumps(control_data).encode('utf-8')
                
                log.info(f"Sending control message to worker {worker_id}: {control_msg[:200]}...")
                
                # Отправляем сообщение в соответствующий partition (воркеру)
                future = self.kafka_producer.send(
                    'simulation_control',
                    value=control_msg,
                    partition=worker_id  # Отправляем в partition, соответствующий worker_id
                )
                futures.append(future)

            # Ожидаем подтверждения доставки всех сообщений
            for future in futures:
                future.get(timeout=20)  # Таймаут 10 секунд на доставку
            
            # Финализируем отправку
            self.kafka_producer.flush()
            log.info("Successfully sent start commands to all workers")
            
        except Exception as e:
            log.error("Failed to start simulation", exc_info=e)
            raise

    def _chunk_particles(self, particles: List[ParticleData], chunk_size: int) -> List[List[ParticleData]]:
        """Разбивает список частиц на чанки фиксированного размера."""
        return [particles[i:i + chunk_size] for i in range(0, len(particles), chunk_size)]

    async def distribute_particles(self, particles: List[ParticleData], step: int):
        """Рассылка частиц через отдельный топик particle_chunks"""
        futures = []
        chunk_size = 10000
        chunks = self._chunk_particles(particles, chunk_size)

        for worker_id in range(self.num_workers):
            for chunk_index, chunk in enumerate(chunks):
                message = {
                    "type": "particle_chunk",
                    "worker_id": worker_id,
                    "step": step,
                    "chunk_index": chunk_index,
                    "total_chunks": len(chunks),
                    "particles": base64.b64encode(serialize_particles(chunk)).decode()
                }
                
                future = self.kafka_producer.send(
                    'particle_chunks',  # Новый топик для чанков
                    key=f"{step}-{worker_id}".encode(),
                    value=json.dumps(message).encode('utf-8'),
                    partition=worker_id  # Используем worker_id как партицию
                )
                futures.append(future)

        # Ожидание подтверждения
        for future in futures:
            future.get(timeout=20)
        
        log.info(f"Sent {len(chunks)} chunks for step {step}")

    def _validate_update(self, data: dict, target_step: int) -> bool:
        """Проверка валидности сообщения с обновлением"""
        try:
            if data.get('type') != 'particle_update':
                return False
                
            if int(data['step']) != target_step:
                return False
                
            required_fields = ['worker_id', 'chunk_index', 'total_chunks', 'updates']
            return all(field in data for field in required_fields)
            
        except Exception as e:
            log.error(f"Invalid update message: {str(e)}")
            return False
    
    async def _collect_updates(self, target_step: int, timeout: float = 30.0):
        """Сбор и объединение чанкированных обновлений от воркеров"""
        updates = {}
        chunks_buffer = {}  # {worker_id: {step: {total_chunks: int, chunks: dict}}}
        start_time = time.time()
    
        while (time.time() - start_time) < timeout:
            batch = self.kafka_consumer.poll(30000)
            
            for _, messages in batch.items():
                for msg in messages:
                    data = msg.value
                    
                    # Валидация сообщения
                    if not self._validate_update(data, target_step):
                        continue
                        
                    worker_id = data['worker_id']
                    chunk_idx = data['chunk_index']
                    total_chunks = data['total_chunks']
                    updates_data = base64.b64decode(data['updates'])
                    
                    # Инициализация буфера для воркера
                    if worker_id not in chunks_buffer:
                        chunks_buffer[worker_id] = {
                            'total': total_chunks,
                            'chunks': {},
                            'received': 0
                        }
                    
                    # Сохраняем чанк если он новый
                    if chunk_idx not in chunks_buffer[worker_id]['chunks']:
                        chunks_buffer[worker_id]['chunks'][chunk_idx] = deserialize_particles(updates_data)
                        chunks_buffer[worker_id]['received'] += 1
                        self.kafka_consumer.commit()
                    
                    # Проверка полноты данных
                    if chunks_buffer[worker_id]['received'] == total_chunks:
                        # Объединение чанков
                        all_updates = []
                        for idx in range(total_chunks):
                            all_updates.extend(chunks_buffer[worker_id]['chunks'][idx])
                        
                        updates[worker_id] = all_updates
                        del chunks_buffer[worker_id]
            
            # Проверяем собранны ли все обновления
            if len(updates) >= self.num_workers:
                break
                
            await asyncio.sleep(0.1)
            
        return updates

    def _merge_with_bounds_check(self, updates: Dict[int, List[ParticleData]]):
        merged = {}
        for worker_id, worker_updates in updates.items():
            bounds = self.grid.get_worker_bounds(worker_id)
            for p in worker_updates:
                merged[p.id] = p  # Сохраняем объект ParticleData
        return list(merged.values())

    def _build_frame_data(self, particles: List[ParticleData], step: int) -> Dict:
        """Формирование данных фрейма"""
        frame = {
            "type": "frame",
            "t": round(step * self.dt, 4),
            "particles": []
        }
        
        for p in particles:
            frame["particles"].append({
                "x": p.position[0],
                "y": p.position[1],
                "z": p.position[2],
                "mass": p.mass,
                "velocity": {
                    "x": p.velocity[0],
                    "y": p.velocity[1],
                    "z": p.velocity[2]
                },
                "acceleration": {
                    "x": p.acceleration[0],
                    "y": p.acceleration[1],
                    "z": p.acceleration[2]
                },
                "radius": p.radius
            })
        
        return frame

    async def run_simulation(self):
        """Основной цикл обработки сообщений с синхронизацией шагов"""
        self.running = True
        current_step = 0
        retry_count = 0
        MAX_RETRIES = 10

        # Создаем очередь для отправки данных через WebSocket
        self.ws_queue = asyncio.Queue()

        # Запускаем задачу-отправитель
        sender_task = asyncio.create_task(self.ws_data_sender())
        
        try:
            while self.running:
                # 1. Отправка частиц только если есть данные
                if self.particles:
                    await self.distribute_particles(self.particles, current_step)
                
                # 2. Ожидаем обновления с увеличенным таймаутом
                updates = await self._collect_updates(current_step, timeout=60.0)
                
                # 3. Проверяем полноту данных
                if len(updates) < self.num_workers:
                    if retry_count < MAX_RETRIES:
                        log.warning(f"Retrying step {current_step} (attempt {retry_count+1})")
                        retry_count += 1
                        continue
                    else:
                        log.error(f"Aborting step {current_step} after {MAX_RETRIES} retries")
                        break
                
                # 4. Обработка успешного шага
                retry_count = 0
                merged = self._merge_with_bounds_check(updates)
                frame_data = self._build_frame_data(merged, current_step)
                
                # 5. Отправка данных и переход к следующему шагу
                await self.ws_queue.put(frame_data)  # Добавляем в очередь вместо прямой отправки

                self.particles = merged
                current_step += 1
                
        except Exception as e:
            log.error("Simulation failed", exc_info=e)

    async def ws_data_sender(self):
        """Отдельная задача для асинхронной отправки данных через WebSocket"""
        while self.running:
            try:
                frame_data = await self.ws_queue.get()
                
                # Добавляем проверку подключенных клиентов
                await self.ws_server.broadcast(frame_data)
                log.info(f"Sent frame t={frame_data['t']} to {len(self.ws_server.connected_clients)} clients")
                    
                # Искусственная задержка для контроля нагрузки
                await asyncio.sleep(0.01)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"WebSocket sender error: {str(e)}", exc_info=True)

    async def shutdown(self):
        """Корректное завершение работы"""
        try:
            self.running = False
            self.kafka_consumer.close()
            self.kafka_producer.flush()
            self.kafka_producer.close()
            log.info(f"Orchestrator shutdown complete")
        except Exception as e:
            log.error(f"Error during shutdown", exc_info=e)


if __name__ == "__main__":
    try:
        orchestrator = Orchestrator()
        asyncio.run(orchestrator.run())
    except Exception as e:
        log.error(f"Failed to start orchestrator", exc_info=e)
