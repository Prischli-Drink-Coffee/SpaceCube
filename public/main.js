import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';
import Stats from 'three/examples/jsm/libs/stats.module.js';
import { OutlineEffect } from 'three/examples/jsm/effects/OutlineEffect.js';
import { TextGeometry } from 'three/examples/jsm/geometries/TextGeometry.js';
import { FontLoader } from 'three/examples/jsm/loaders/FontLoader.js';

let container, stats;
let socket = null;
let currentData = null;
let camera, scene, renderer, effect;
let controls;
let sceneCube;
let particleMesh = null;

// Добавим константы для настройки
const SCALE_FACTOR = 0.5; // Коэффициент масштабирования данных
const COUNT_PARTICLE = 10000;
const BOX_SIZE = 100; // Размер куба сцены
const BASE_SIZE = BOX_SIZE / COUNT_PARTICLE * 10; // Базовый размер для массы 1
const SPEED_COLOR_INTENSITY = 0.3; // Усиление цвета скорости

async function setupWebSocket() {
    socket = new WebSocket('ws://127.0.0.1:9003');

    socket.onopen = () => {
        console.log('WebSocket connected');
    };

    socket.onmessage = (event) => {
        try {
            // console.log('Raw data:', event.data);
            const newData = JSON.parse(event.data);
            // console.log('Parsed data:', newData);

            if (!currentData || newData.particles.length !== currentData.particles.length) {
                currentData = newData;
                createParticles();
            } else {
                currentData = newData;
                updateParticles();
            }
        } catch (e) {
            console.error('Ошибка обработки данных:', e);
        }
    };

    socket.onerror = (error) => {
        console.error('Ошибка WebSocket:', error);
    };

    socket.onclose = (event) => {
        console.log('WebSocket closed', event);
        setTimeout(setupWebSocket, 1000);
    };
}

function createParticles() {
    if (particleMesh) {
        scene.remove(particleMesh);
        particleMesh.geometry.dispose();
        particleMesh.material.dispose();
    }

    const instanceCount = currentData.particles.length;
    const geometry = new THREE.InstancedBufferGeometry();
    const baseGeometry = new THREE.SphereGeometry(1, 16, 16); // Единичная сфера
    geometry.copy(baseGeometry);
    baseGeometry.dispose();

    // Добавляем атрибуты
    const speeds = new Float32Array(instanceCount);
    const masses = new Float32Array(instanceCount);
    const radii = new Float32Array(instanceCount); // Добавляем радиусы
    let maxSpeed = 0.001;

    currentData.particles.forEach((p, i) => {
        const speed = Math.sqrt(p.velocity.x**2 + p.velocity.y**2 + p.velocity.z**2);
        speeds[i] = speed;
        masses[i] = p.mass;
        radii[i] = p.radius; // Сохраняем радиус
        if (speed > maxSpeed) maxSpeed = speed;
    });

    geometry.setAttribute('speed', new THREE.InstancedBufferAttribute(speeds, 1));
    geometry.setAttribute('mass', new THREE.InstancedBufferAttribute(masses, 1));
    geometry.setAttribute('radius', new THREE.InstancedBufferAttribute(radii, 1)); // Добавляем радиус в атрибуты

    const material = new THREE.ShaderMaterial({
        vertexShader: document.getElementById('vertexShader').textContent,
        fragmentShader: document.getElementById('fragmentShader').textContent,
        uniforms: {
            maxSpeed: { value: maxSpeed },
            colorIntensity: { value: SPEED_COLOR_INTENSITY }
        },
        transparent: true,
        depthTest: true
    });

    // Добавьте обновление colorIntensity
    particleMesh = new THREE.InstancedMesh(geometry, material, instanceCount);
    particleMesh.instanceMatrix.setUsage(THREE.DynamicDrawUsage);

    // Инициализация матриц с учетом радиуса
    const matrix = new THREE.Matrix4();
    const position = new THREE.Vector3();
    const scale = new THREE.Vector3();
    
    currentData.particles.forEach((p, i) => {
        position.set(
            p.x * SCALE_FACTOR,
            p.y * SCALE_FACTOR,
            p.z * SCALE_FACTOR
        );
        const size = BASE_SIZE * p.radius * 2;
        scale.set(size, size, size);
        matrix.compose(position, new THREE.Quaternion(), scale);
        particleMesh.setMatrixAt(i, matrix);
    });

    particleMesh.instanceMatrix.needsUpdate = true;
    scene.add(particleMesh);
}

function updateParticles() {
    if (!particleMesh || !currentData) return;

    // Обновление скоростей
    const speeds = particleMesh.geometry.attributes.speed.array;
    let maxSpeed = 0.001;
    
    currentData.particles.forEach((p, i) => {
        const speed = Math.sqrt(p.velocity.x**2 + p.velocity.y**2 + p.velocity.z**2);
        speeds[i] = speed;
        if (speed > maxSpeed) maxSpeed = speed;
    });
    
    particleMesh.material.uniforms.maxSpeed.value = maxSpeed;
    particleMesh.geometry.attributes.speed.needsUpdate = true;

    // Обновление позиций и размеров с учетом радиуса
    const matrix = new THREE.Matrix4();
    const position = new THREE.Vector3();
    const scale = new THREE.Vector3();
    
    currentData.particles.forEach((p, i) => {
        position.set(
            p.x * SCALE_FACTOR,
            p.y * SCALE_FACTOR,
            p.z * SCALE_FACTOR
        );
        const size = BASE_SIZE * p.radius * 2;
        scale.set(size, size, size);
        matrix.compose(position, new THREE.Quaternion(), scale);
        particleMesh.setMatrixAt(i, matrix);
    });

    particleMesh.instanceMatrix.needsUpdate = true;
}

// Создаем куб границ
function createSceneCube() {
    const cubeGeometry = new THREE.BoxGeometry(BOX_SIZE * 0.5, BOX_SIZE * 0.5, BOX_SIZE * 0.5);
    const edges = new THREE.EdgesGeometry(cubeGeometry);
    const cubeMaterial = new THREE.LineBasicMaterial({ color: 0xfffff });
    sceneCube = new THREE.LineSegments(edges, cubeMaterial);

    sceneCube.position.x = BOX_SIZE * 0.25;
    sceneCube.position.y = BOX_SIZE * 0.25;
    sceneCube.position.z = BOX_SIZE * 0.25;

    scene.add(sceneCube);
}

init();

function init() {
    container = document.createElement('div');
    document.body.appendChild(container);

    camera = new THREE.PerspectiveCamera(75, window.innerWidth / window.innerHeight, 0.1, BOX_SIZE * 10);
    camera.position.x = BOX_SIZE;
    camera.position.y = BOX_SIZE;
    camera.position.z = BOX_SIZE;

    scene = new THREE.Scene();
    scene.background = new THREE.Color( 0xffffff );

    // Добавляем освещение
    const light = new THREE.DirectionalLight(0xffffff, 1);
    light.position.set(1, 1, 1).normalize();
    scene.add(light);

    createSceneCube(); // Создаем границы сцены

    setupWebSocket();

    renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setPixelRatio( window.devicePixelRatio );
    renderer.setSize(window.innerWidth, window.innerHeight);
    container.appendChild(renderer.domElement);

    controls = new OrbitControls(camera, renderer.domElement);
    controls.minDistance = BOX_SIZE * 0.1;
    controls.maxDistance = BOX_SIZE * 3;
    controls.autoRotate = true; // Включаем автоматическое вращение
    controls.autoRotateSpeed = 0.1; // Скорость автоматического вращения

    // Смещаем центр вращения на BOX_SIZE по осям
    controls.target.set(BOX_SIZE * 0.25, BOX_SIZE * 0.25, BOX_SIZE * 0.25);

    // Устанавливаем стартовое расстояние камеры до целевой точки
    const startDistance = BOX_SIZE * 0.75; // Нужное расстояние
    const direction = new THREE.Vector3()
        .subVectors(camera.position, controls.target) // Вектор от камеры к цели
        .normalize(); // Нормализуем, чтобы получить направление
    camera.position.copy(controls.target).add(direction.multiplyScalar(startDistance));

    // Обновляем контролы
    controls.update();

    stats = new Stats();
    container.appendChild(stats.dom);

    window.addEventListener('resize', onWindowResize);

    // Запускаем анимацию
    animate();
}

function onWindowResize() {
    camera.aspect = window.innerWidth / window.innerHeight;
    camera.updateProjectionMatrix();
    renderer.setSize(window.innerWidth, window.innerHeight);
}

function animate() {
    requestAnimationFrame(animate);
    controls.update();
    renderer.render(scene, camera);
    stats.update();
}
