const express = require('express');
const path = require('path');
const app = express();

// Путь к директории, которая на один уровень выше текущей
const parentDirPath = path.join(__dirname, '..');

// Путь к папке, где находится current_state.json
const serverDataPath = path.join(parentDirPath);

// Пути к папкам
const publicPath = path.join(parentDirPath, 'client/public');

// Настройка статических файлов
app.use(express.static(publicPath));
app.use(express.static(serverDataPath));

// Маршрут для главной страницы
app.get('/', (req, res) => {
    res.sendFile(path.join(publicPath, 'index.html'));
});

const PORT = 3000;
const HOST = '127.0.0.1';
    
app.listen(PORT, HOST, () => {
    console.log(`Server running on http://${HOST}:${PORT}`);
});