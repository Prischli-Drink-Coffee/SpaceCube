# worker/src/pipeline/worker.py
import asyncio
import json
import logging
import os
import time
from typing import Dict, Any, List, Tuple
from kafka import KafkaConsumer, KafkaProducer
from pycuda import autoinit  # Важно для инициализации CUDA контекста
import base64
from kafka import TopicPartition
from src.scripts.cuda_kernel import DynamicKernelLoader
from src.scripts.particle_processor import ParticleProcessor
from src.shared.schemas import KernelUpdate, SimulationControl, ParticleData
from src.shared.utils import serialize_particles, get_env_value, deserialize_particles
from src.utils.custom_logging import setup_logging
from env import Env

log = setup_logging()
env = Env()


def _create_kafka_producer(kafka_brokers):
    """Создание Kafka Producer с обработкой ошибок"""
    try:
        return KafkaProducer(
            bootstrap_servers=kafka_brokers,
            value_serializer=lambda v: v,  # Оставляем как есть
            key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode('utf-8'),
            api_version=(2, 8, 0),  # Указываем версию API Kafka
            retries=3,  # Повторные попытки при ошибках
            request_timeout_ms=10000  # Таймаут запросов
        )
    except Exception as e:
        log.error(f"Failed to create Kafka producer: {e}")
        raise


def _create_kafka_consumer(kafka_brokers, worker_id):
    """Создание Kafka Consumer с обработкой ошибок"""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_brokers,
            group_id=f'worker-group-{worker_id}',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            enable_auto_commit=False,
            auto_offset_reset='earliest',  # Чтение с начала, если смещение не задано
            api_version=(2, 8, 0),  # Указываем версию API Kafka
            session_timeout_ms=30000,  # Таймаут сессии
            request_timeout_ms=35000,  # Таймаут запросов (должен быть больше session_timeout_ms)
            heartbeat_interval_ms=10000,  # Интервал heartbeat
            fetch_max_wait_ms=500, # Уменьшаем время ожидания
            max_poll_records=1, # Обрабатываем по одному сообщению
            max_partition_fetch_bytes=52428800,  # 50 MB
            fetch_max_bytes=52428800  # 50 MB
        )

        # Явное указание приоритета топиков
        consumer.subscribe(
            ['kernel_updates', 'simulation_control', 'particle_chunks']
        )
        return consumer
    except Exception as e:
        log.error(f"Failed to create Kafka consumer: {e}")
        raise


class ParticleWorker:
    def __init__(self, worker_id: int):
        # Инициализация параметров
        self.worker_id = worker_id
        self.running = False
        self.simulation_params = {'dt': None, 'box_size': None, 'bounds': None}
        self.kafka_brokers = get_env_value(env, 'KAFKA_BROKERS', required=True, type_cast=lambda x: x.split(','))
        # Инициализация процессора         
        self.processor = ParticleProcessor()
        # Инициализация загрузчика ядра
        self.kernel_loader = DynamicKernelLoader()
        # Инициализация Kafka клиента
        self.kafka_producer = _create_kafka_producer(self.kafka_brokers)
        self.kafka_consumer = _create_kafka_consumer(self.kafka_brokers, self.worker_id)
        log.info(f"Worker {self.worker_id} initialized with brokers: {self.kafka_brokers}")

    async def start(self):
        """Основной цикл обработки сообщений"""
        self.running = True
        log.info(f"Worker {self.worker_id} started")
        
        try:
            while self.running:
                await self._process_messages()
                await asyncio.sleep(0.1)
                
        except Exception as e:
            log.error(f"Worker {self.worker_id} failed", exc_info=e)
        finally:
            await self.shutdown()

    async def _process_messages(self):
        """Обработка входящих сообщений с учетом смещений"""
        try:
            batch = self.kafka_consumer.poll(timeout_ms=20000)
            
            if not batch:
                return

            for tp, messages in batch.items():
                for msg in messages:
                    await self.process_message(
                        message=msg.value,
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset
                    )
                    # Подтверждаем каждое сообщение сразу после обработки
                    self._commit_message(tp.topic, tp.partition, msg.offset)
                    
        except Exception as e:
            log.error("Error processing messages", exc_info=e)

    def _commit_message(self, topic: str, partition: int, offset: int):
        try:
            self.kafka_consumer.commit()
        except Exception as e:
            log.error(f"Commit failed: {str(e)}", exc_info=e)

    async def process_message(self, message: Dict[str, Any], topic: str, partition: int, offset: int):
        try:
            if not message:
                log.warning("Received empty message")
                return

            msg_type = message.get('type')

            # Фильтрация по топику и партиции
            if topic == 'simulation_control':
                # Сообщения из simulation_control должны быть обработаны только тем воркером, 
                # для которого они предназначены (partition == worker_id)
                if partition != self.worker_id:
                    log.debug(f"Ignoring message from topic '{topic}' (partition {partition} != worker_id {self.worker_id})")
                    return

            elif topic == 'particle_chunks':
                # Сообщения из particle_chunks должны быть обработаны только тем воркером,
                # для которого они предназначены (worker_id в сообщении == self.worker_id)
                if 'worker_id' in message and message['worker_id'] != self.worker_id:
                    log.debug(f"Ignoring message from topic '{topic}' (worker_id {message['worker_id']} != {self.worker_id})")
                    return

            elif topic == 'kernel_updates':
                # Сообщения из kernel_updates обрабатываются всеми воркерами
                pass

            else:
                log.warning(f"Unknown topic: {topic}")
                return

            # Обработка сообщения в зависимости от типа
            if msg_type == 'kernel_update':
                await self.handle_kernel_update(message)
            elif msg_type == 'control':
                await self.handle_control(message)
            elif msg_type == 'particle_chunk':
                await self.handle_particle_data(message)
            else:
                log.warning(f"Unknown message type: {msg_type}")

        except Exception as e:
            log.error(f"Message processing failed", exc_info=e)
            # При ошибке не подтверждаем offset для повторной обработки

    async def handle_kernel_update(self, message: Dict[str, Any]):
        """Обновление CUDA ядра"""
        try:
            validated = KernelUpdate(**message)
            self.kernel_loader.compile(validated.code)
            log.info(f"Worker {self.worker_id} updated kernel to version {validated.version}")
            
        except Exception as e:
            log.error(f"Kernel update failed", exc_info=e)
            raise  # Повторно вызываем исключение для отмены подтверждения

    async def handle_control(self, message: Dict[str, Any]):
        """Обработка управляющих команд"""
        try:
            # Валидация и десериализация сообщения
            cmd = SimulationControl(**message)
            
            # Проверяем, предназначено ли сообщение этому воркеру
            if cmd.worker_id != self.worker_id:
                return

            if cmd.command == 'start':
                # Ждем завершения обработки предыдущих сообщений
                await asyncio.sleep(2)

                # Обновляем параметры симуляции
                self.simulation_params.update({
                    'dt': cmd.dt,
                    'box_size': cmd.box_size,
                    'bounds': cmd.bounds
                })
                
                # Логируем начало симуляции
                log.info(f"Starting simulation with params: {cmd}")
            elif cmd.command == 'stop':
                # Останавливаем симуляцию
                self.running = False
                log.info(f"Worker {self.worker_id} received stop command")

        except Exception as e:
            log.error(f"Control command error", exc_info=e)

    async def handle_particle_data(self, message: Dict[str, Any]):
        """Обработка сообщения с частицами"""
        try:
            # Проверяем принадлежность сообщения текущему воркеру
            if message.get('worker_id') != self.worker_id:
                return

            step = message['step']
            chunk_index = message['chunk_index']
            total_chunks = message['total_chunks']
            serialized_particles = message['particles']

            # Десериализуем частицы
            chunk = deserialize_particles(base64.b64decode(serialized_particles))

            # Сохраняем чанк
            if not hasattr(self, 'particle_chunks'):
                self.particle_chunks = {}
            self.particle_chunks[chunk_index] = chunk

            # Если все чанки получены, объединяем их
            if len(self.particle_chunks) == total_chunks:
                all_particles = []
                for i in range(total_chunks):
                    all_particles.extend(self.particle_chunks[i])
                del self.particle_chunks  # Очищаем чанки после объединения

                # Загружаем частицы в процессор
                self.processor.load_particles(all_particles)
                self.processor.filter_by_bounds(self.simulation_params['bounds'])
                log.info(f"Worker {self.worker_id} loaded {len(all_particles)} particles for step {step}")

                # Запускаем выполнение шага симуляции
                await self.run_simulation(step=step)

        except KeyError as e:
            log.error(f"Missing required field in message: {str(e)}")
        except Exception as e:
            log.error(f"Particle processing error", exc_info=e)

    async def run_simulation(self, step: int):
        """Выполнение шага симуляции"""
        try:

            data = self.processor.get_local_data()
            positions, velocities, accelerations, masses, radii, local_flags, N = data
            if N == 0:
                return
                
            # Проверка наличия данных
            if len(positions) == 0:
                return
                
            new_pos, new_vel, new_acc = self.kernel_loader.execute(
                positions=positions,
                velocities=velocities,
                accelerations=accelerations,
                masses=masses,
                radii=radii,
                local_flags=local_flags,
                N=N,
                dt=self.simulation_params['dt'],
                box_size=self.simulation_params['box_size']
            )
            
            updates = []
            for particle_id, particle in self.processor.local_particles.items():
                idx = self.processor.all_particles.index(particle)
                particle_data = {
                    'id': particle_id,
                    'position': new_pos[idx].tolist(),
                    'velocity': new_vel[idx].tolist(),
                    'acceleration': new_acc[idx].tolist(),
                    'mass': particle.mass,
                    'radius': particle.radius
                }
                # Преобразуем словарь в объект ParticleData
                updates.append(ParticleData(**particle_data))
            
            await self.send_updates(updates, step)
            
        except Exception as e:
            log.error(f"Simulation step failed", exc_info=e)

    def _chunk_particles(self, particles: List[ParticleData], chunk_size: int) -> List[List[ParticleData]]:
        """Разбивает список частиц на чанки фиксированного размера."""
        return [particles[i:i + chunk_size] for i in range(0, len(particles), chunk_size)]

    async def send_updates(self, updates: List[ParticleData], step: int):
        """Надежная отправка чанков с подтверждением"""
        chunk_size = 1000
        chunks = self._chunk_particles(updates, chunk_size)
        
        for chunk_index, chunk in enumerate(chunks):
            message = {
                'type': 'particle_update',
                'worker_id': self.worker_id,
                'step': step,
                'chunk_index': chunk_index,
                'total_chunks': len(chunks),
                'updates': base64.b64encode(serialize_particles(chunk)).decode()
            }
            
            future = self.kafka_producer.send(
                'particle_updates',
                key=f"{step}-{self.worker_id}".encode(),
                value=json.dumps(message).encode('utf-8'),
                partition=self.worker_id
            )
            future.get(timeout=20)

    async def shutdown(self):
        """Корректное завершение работы"""
        try:
            self.kafka_consumer.close()
            self.kafka_producer.flush()
            self.kafka_producer.close()
            log.info(f"Worker {self.worker_id} shutdown complete")
        except Exception as e:
            log.error(f"Error during shutdown", exc_info=e)


if __name__ == "__main__":
    try:
        worker_id = int(env.__getattr__('WORKER_ID'))
        worker = ParticleWorker(worker_id)
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        log.info("Worker stopped by user")
    except Exception as e:
        log.error(f"Failed to start worker", exc_info=e)
