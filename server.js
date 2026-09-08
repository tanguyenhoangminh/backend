const express = require('express');
const http = require('http'); // [SOCKET.IO]
const { Server } = require('socket.io'); // [SOCKET.IO]
const cors = require('cors');
const mysql = require('mysql2/promise');
const mqtt = require('mqtt'); // Thư viện MQTT
require('dotenv').config(); 

// Voice NLU 
const GROQ_API_KEY = process.env.GROQ_API_KEY;
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;

const app = express();
app.use(cors());
app.use(express.json());

// [SOCKET.IO] Khởi tạo HTTP Server & gắn WebSocket
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST", "PUT"]
    }
});

io.on('connection', (socket) => {
    console.log(`🔌 Client kết nối Socket.io: ${socket.id}`);
    socket.on('join_room', (roomNumber) => {
        socket.join(`room_${roomNumber}`);
    });
    socket.on('disconnect', () => {
        console.log(`❌ Client ngắt kết nối: ${socket.id}`);
    });
});

// Kết nối MySQL 8.0
const pool = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || 'minhmongmo1',
    database: process.env.DB_NAME || 'iot_hotel',
    timezone: '+00:00',          // ← THÊM
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Thêm cột nếu chưa có (safe migration)
pool.query(`ALTER TABLE room_iot_state ADD COLUMN IF NOT EXISTS main_brightness INT DEFAULT 100`).catch(() => {});
pool.query(`ALTER TABLE room_iot_state ADD COLUMN IF NOT EXISTS desk_brightness INT DEFAULT 100`).catch(() => {});
pool.query(`ALTER TABLE room_iot_state ADD COLUMN IF NOT EXISTS light_brightness TINYINT UNSIGNED DEFAULT 100`).catch(() => {});
pool.query(`ALTER TABLE room_iot_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`).catch(() => {});

pool.query(`
    CREATE TABLE IF NOT EXISTS alert_acks (
        room_number VARCHAR(10) NOT NULL,
        alert_type VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (room_number, alert_type)
    )
`).then(() => console.log("✅ Alert table ready!"))
  .catch(err => console.error("Alert_acks_error:", err));

pool.query(`
    CREATE TABLE IF NOT EXISTS edge_gateway (
        gateway_id     INT UNSIGNED PRIMARY KEY,
        node_name      VARCHAR(100),
        status         ENUM('online','offline') DEFAULT 'offline',
        last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
`).catch(err => console.error("edge_gateway_error:", err));

pool.query(`
    CREATE TABLE IF NOT EXISTS ai_prediction (
        prediction_id       BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        room_number         VARCHAR(10) NOT NULL,
        model_name          VARCHAR(50),
        model_version       VARCHAR(20),
        features_used       JSON,
        predicted_humidity  DECIMAL(6,2),
        predicted_co2       DECIMAL(8,2),
        predicted_energy_kwh DECIMAL(10,4),
        predicted_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
`).catch(err => console.error("ai_prediction_error:", err));

pool.query(`
    CREATE TABLE IF NOT EXISTS perf_metric (
        metric_id     BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        component     VARCHAR(50),
        gateway_id    INT UNSIGNED,
        metric_name   VARCHAR(50),
        metric_value  DECIMAL(12,4),
        unit          VARCHAR(20),
        recorded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
`).catch(err => console.error("perf_metric_error:", err));

// ==========================================
// MQTT CLOUD (HIVEMQ TLS)
// ==========================================
const REAL_ROOMS = ['0101', '0102', '101', '102']; 

const MQTT_BROKER = `mqtts://${process.env.MQTT_HOST || '9285fd3c13654137ab1f1c4d1fbf39ae.s1.eu.hivemq.cloud'}:${process.env.MQTT_PORT || 8883}`;

const MQTT_OPTIONS = {
    clientId: 'hotel_backend_render_' + Math.random().toString(16).substring(2, 8),
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    protocol: 'mqtts',
    rejectUnauthorized: true
};

const mqttClient = mqtt.connect(MQTT_BROKER, MQTT_OPTIONS);

mqttClient.on('connect', () => {
    console.log("☁️ Đã kết nối MQTT với HiveMQ Cloud!");
    mqttClient.subscribe('hotel/room/+/sensors', (err) => {
        if (!err) console.log("📡 Đang lắng nghe dữ liệu cảm biến từ HiveMQ...");
    });
});

mqttClient.on('message', async (topic, message) => {
    try {
        const topicParts = topic.split('/');
        const roomNumber = topicParts[2]; 
        
        if (!REAL_ROOMS.includes(roomNumber)) return;

        const sensorData = JSON.parse(message.toString());
        const [rooms] = await pool.query("SELECT room_id FROM room WHERE room_number = ?", [roomNumber]);
        if (rooms.length === 0) return;
        const roomId = rooms[0].room_id;

        const fields = [];
        const values = [];

        if (sensorData.temp !== undefined) { fields.push('temp=?'); values.push(sensorData.temp); }
        if (sensorData.humidity !== undefined) { fields.push('humidity=?'); values.push(sensorData.humidity); }
        if (sensorData.co2 !== undefined) { fields.push('co2=?'); values.push(sensorData.co2); }
        if (sensorData.noise !== undefined) { fields.push('noise=?'); values.push(sensorData.noise); }
        if (sensorData.light !== undefined) { fields.push('light=?'); values.push(sensorData.light); }
        if (sensorData.motion !== undefined) { fields.push('motion=?'); values.push(sensorData.motion); }
        if (sensorData.smoke !== undefined) { fields.push('smoke=?'); values.push(sensorData.smoke); }
        if (sensorData.smoke_alert !== undefined) { fields.push('siren=?'); values.push(sensorData.smoke_alert); }
        if (sensorData.main_light !== undefined) { fields.push('main_light=?'); values.push(sensorData.main_light); }
        if (sensorData.desk_lamp !== undefined) { fields.push('desk_lamp=?'); values.push(sensorData.desk_lamp); }
        if (sensorData.main_brightness !== undefined) { fields.push('main_brightness=?'); values.push(sensorData.main_brightness); }
        if (sensorData.desk_brightness !== undefined) { fields.push('desk_brightness=?'); values.push(sensorData.desk_brightness); }

        if (fields.length > 0) {
            values.push(roomId);
            await pool.query(
                `UPDATE room_iot_state SET ${fields.join(', ')} WHERE room_id=?`,
                values
            );

            // Bắn realtime cho Frontend
            const payload = { room_number: roomNumber, ...sensorData };
            io.to(`room_${roomNumber}`).emit('room_state_changed', payload);
            io.emit('room_update_global', payload);
        }
    } catch (error) {
        console.error("Lỗi xử lý tin nhắn MQTT:", error);
    }
});

// ==========================================
// --- API QUẢN LÝ PHÒNG & GUEST & BOOKINGS ---
// ==========================================
app.route('/api/rooms')
    .get(async (req, res) => {
        try {
            const sql = `
                SELECT 
                    r.room_id as id, r.room_number, r.status, f.floor_number as floor, 
                    rt.type_name as type, rt.base_price as price, rt.max_occupancy as occupancy, rt.description as \`desc\`
                FROM room r
                JOIN floor f ON r.floor_id = f.floor_id
                JOIN room_type rt ON r.type_id = rt.type_id
                ORDER BY r.room_number ASC
            `;
            const [rooms] = await pool.query(sql);
            const formattedRooms = rooms.map(room => ({
                ...room,
                price: Number(room.price).toLocaleString('vi-VN')
            }));
            res.json(formattedRooms);
        } catch (error) {
            res.status(500).json({ error: error.message });
        }
    });

app.route('/api/rooms/:room_number/status')
    .put(async (req, res) => {
        try {
            const sql = `UPDATE room SET status = ? WHERE room_number = ?`;
            await pool.query(sql, [req.body.status.toLowerCase(), req.params.room_number]);
            res.json({ message: `Cập nhật trạng thái phòng ${req.params.room_number} thành công!` });
        } catch (error) {
            res.status(400).json({ error: error.message });
        }
    });

app.route('/api/rooms/:room_number/type')
    .put(async (req, res) => {
        const { type_name } = req.body; 
        const { room_number } = req.params;
        try {
            const [types] = await pool.query('SELECT type_id FROM room_type WHERE type_name = ?', [type_name]);
            if (types.length === 0) return res.status(404).json({ error: "Loại phòng không tồn tại trong database!" });
            await pool.query(`UPDATE room SET type_id = ? WHERE room_number = ?`, [types[0].type_id, room_number]);
            res.json({ message: `Cập nhật loại phòng thành công!` });
        } catch (error) {
            res.status(400).json({ error: error.message });
        }
    });

app.route('/api/guests')
    .get(async (req, res) => {
        try {
            const [guests] = await pool.query(`SELECT * FROM guest ORDER BY guest_id DESC`);
            res.json(guests);
        } catch (error) {
            res.status(500).json({ error: error.message });
        }
    })
    .post(async (req, res) => {
        const data = req.body;
        const nameParts = (data.full_name || '').trim().split(' ');
        const first_name = nameParts[0] || 'Unknown';
        const last_name = nameParts.slice(1).join(' ') || ' ';
        const email = data.email || `guest_${Date.now()}@hotel.com`;
        const dob = data.date_of_birth || '1990-01-01';

        try {
            const sql = `INSERT INTO guest (first_name, last_name, email, phone, nationality, passport_no, gender, date_of_birth, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW())`;
            await pool.query(sql, [first_name, last_name, email, data.phone, data.nationality, data.passport_no, data.gender, dob]);
            res.status(201).json({ message: "Thêm thành công!" });
        } catch (error) {
            res.status(400).json({ error: error.message });
        }
    });

app.route('/api/guests/:id')
    .put(async (req, res) => {
        const data = req.body;
        const nameParts = (data.full_name || '').trim().split(' ');
        const first_name = nameParts[0];
        const last_name = nameParts.slice(1).join(' ') || ' ';
        const dob = data.date_of_birth || '1990-01-01';

        try {
            const sql = `UPDATE guest SET first_name=?, last_name=?, phone=?, nationality=?, passport_no=?, gender=?, date_of_birth=? WHERE guest_id=?`;
            await pool.query(sql, [first_name, last_name, data.phone, data.nationality, data.passport_no, data.gender, dob, req.params.id]);
            res.json({ message: "Cập nhật thành công!" });
        } catch (error) {
            res.status(400).json({ error: error.message });
        }
    })
    .delete(async (req, res) => {
        try {
            await pool.query("DELETE FROM guest WHERE guest_id = ?", [req.params.id]);
            res.json({ message: "Xóa thành công!" });
        } catch (error) {
            res.status(500).json({ error: error.message });
        }
    });

app.route('/api/bookings')
    .get(async (req, res) => {
        try {
            const sql = `
                SELECT b.*, 
                       g.first_name, g.last_name, g.passport_no, g.phone, g.email, g.nationality, g.gender, g.date_of_birth, 
                       r.room_number, rt.type_name as room_type, rt.base_price
                FROM booking b
                JOIN guest g ON b.guest_id = g.guest_id
                JOIN room r ON b.room_id = r.room_id
                JOIN room_type rt ON r.type_id = rt.type_id
                ORDER BY b.booking_id DESC
            `;
            const [bookings] = await pool.query(sql);
            res.json(bookings);
        } catch (error) {
            res.status(500).json({ error: error.message });
        }
    })
    .post(async (req, res) => {
        const { guest_id, room_id, payment_status, total_price } = req.body;
        const connection = await pool.getConnection(); 
        try {
            await connection.beginTransaction(); 
            const sqlInsert = `
                INSERT INTO booking (guest_id, room_id, check_in_date, check_out_date, status, payment_status, total_price) 
                VALUES (?, ?, CURDATE(), DATE_ADD(CURDATE(), INTERVAL 1 DAY), 'checked_in', ?, ?)
            `;
            await connection.query(sqlInsert, [guest_id, room_id, payment_status, total_price || 0]);
            await connection.query(`UPDATE room SET status = 'occupied' WHERE room_id = ?`, [room_id]);
            await connection.commit(); 
            res.status(201).json({ message: "Đặt phòng thành công!" });
        } catch (error) {
            await connection.rollback(); 
            res.status(400).json({ error: error.message });
        } finally {
            connection.release(); 
        }
    });

app.route('/api/bookings/:id')
    .put(async (req, res) => {
        const { payment_status, status, check_in_date, check_out_date } = req.body;
        try {
            const sql = `UPDATE booking SET payment_status = ?, status = ?, check_in_date = ?, check_out_date = ? WHERE booking_id = ?`;
            await pool.query(sql, [payment_status, status, check_in_date, check_out_date, req.params.id]);
            res.json({ message: "Cập nhật thành công!" });
        } catch (error) {
            res.status(400).json({ error: error.message });
        }
    })
    .delete(async (req, res) => {
        const connection = await pool.getConnection();
        try {
            await connection.beginTransaction(); 
            const [booking] = await connection.query("SELECT room_id FROM booking WHERE booking_id = ?", [req.params.id]);
            if (booking.length > 0) {
                await connection.query("UPDATE room SET status = 'available' WHERE room_id = ?", [booking[0].room_id]);
            }
            await connection.query("DELETE FROM booking WHERE booking_id = ?", [req.params.id]);
            await connection.commit();
            res.json({ message: "Đã hủy booking và giải phóng phòng!" });
        } catch (error) {
            await connection.rollback(); 
            res.status(500).json({ error: error.message }); 
        } finally {
            connection.release(); 
        }
    });

app.get('/api/staff', async (req, res) => {
    try {
        const sql = `
            SELECT s.staff_id, s.first_name, s.last_name, s.phone, s.email, r.role_name,
                   t.task_id, t.task_type, rm.room_number,
                   IF(t.task_id IS NOT NULL, 'Busy', 'Available') as status
            FROM staff s
            JOIN role r ON s.role_id = r.role_id
            LEFT JOIN staff_task t ON s.staff_id = t.staff_id AND t.status = 'In Progress'
            LEFT JOIN room rm ON t.room_id = rm.room_id
            WHERE s.is_active = 1
            ORDER BY s.staff_id ASC
        `;
        const [staffList] = await pool.query(sql);
        const formattedStaff = staffList.map(st => ({
            ...st,
            full_name: `${st.first_name} ${st.last_name}`,
            role: st.role_name
        }));
        res.json(formattedStaff);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/tasks', async (req, res) => {
    const { staff_id, room_id, task_type } = req.body;
    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();
        const [checkStaff] = await connection.query("SELECT * FROM staff_task WHERE staff_id = ? AND status = 'In Progress'", [staff_id]);
        if (checkStaff.length > 0) throw new Error("Nhân viên này đang thực hiện một công việc khác!");

        const [checkRoom] = await connection.query("SELECT * FROM staff_task WHERE room_id = ? AND status = 'In Progress'", [room_id]);
        if (checkRoom.length > 0) throw new Error("Phòng này đang được nhân viên khác xử lý!");
        
        await connection.query("INSERT INTO staff_task (staff_id, room_id, task_type) VALUES (?, ?, ?)", [staff_id, room_id, task_type]);
        await connection.query("UPDATE room SET status = ? WHERE room_id = ?", [task_type.toLowerCase(), room_id]);
        
        await connection.commit();
        res.status(201).json({ message: "Giao việc thành công!" });
    } catch (error) {
        await connection.rollback();
        res.status(400).json({ error: error.message });
    } finally {
        connection.release(); 
    }
});

app.put('/api/tasks/:task_id/complete', async (req, res) => {
    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();
        const [tasks] = await connection.query("SELECT room_id FROM staff_task WHERE task_id = ?", [req.params.task_id]);
        if (tasks.length > 0) {
            await connection.query("UPDATE room SET status = 'available' WHERE room_id = ?", [tasks[0].room_id]);
        }
        await connection.query("UPDATE staff_task SET status = 'Completed' WHERE task_id = ?", [req.params.task_id]);
        await connection.commit();
        res.json({ message: "Công việc đã hoàn thành, phòng đã sẵn sàng!" });
    } catch (error) {
        await connection.rollback();
        res.status(400).json({ error: error.message });
    } finally {
        connection.release(); 
    }
});

app.post('/api/staff', async (req, res) => {
    const { first_name, last_name, email, phone, role } = req.body;
    try {
        const role_id = role === 'Housekeeping' ? 1 : 2; 
        await pool.query("INSERT INTO staff (first_name, last_name, email, phone, role_id, password_hash, hire_date) VALUES (?, ?, ?, ?, ?, 'dummy_hash', CURDATE())", 
        [first_name, last_name, email, phone, role_id]);
        res.status(201).json({ message: "Thêm thành công!" });
    } catch (error) { 
        res.status(400).json({ error: error.message }); 
    }
});

app.put('/api/staff/:id', async (req, res) => {
    const { first_name, last_name, phone, role } = req.body;
    try {
        const role_id = role === 'Housekeeping' ? 1 : 2;
        await pool.query("UPDATE staff SET first_name=?, last_name=?, phone=?, role_id=? WHERE staff_id=?", 
        [first_name, last_name, phone, role_id, req.params.id]);
        res.json({ message: "Sửa thành công!" });
    } catch (error) { 
        res.status(400).json({ error: error.message }); 
    }
});

app.delete('/api/staff/:id', async (req, res) => {
    try {
        await pool.query("UPDATE staff SET is_active = 0 WHERE staff_id = ?", [req.params.id]);
        res.json({ message: "Xóa thành công!" });
    } catch (error) { 
        res.status(400).json({ error: error.message }); 
    }
});

// ==========================================
// --- API HỆ THỐNG CẢNH BÁO (ALERTS SYSTEM) ---
// ==========================================
app.post('/api/alerts/acknowledge', async (req, res) => {
    const { alertsToAck } = req.body; 
    if (!alertsToAck || alertsToAck.length === 0) return res.json({ message: "Không có alert nào" });

    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();
        for (let alert of alertsToAck) {
            const sql = `INSERT IGNORE INTO alert_acks (room_number, alert_type) VALUES (?, ?)`;
            await connection.query(sql, [alert.room_number, alert.type]);
        }
        await connection.commit();
        res.json({ message: "Đã lưu trạng thái Acknowledge!" });
    } catch (error) {
        await connection.rollback();
        res.status(500).json({ error: error.message });
    } finally {
        connection.release(); 
    }
});

app.get('/api/alerts', async (req, res) => {
    try {
        const sql = `
            SELECT r.room_number, f.floor_number, i.* FROM room_iot_state i 
            JOIN room r ON i.room_id = r.room_id 
            JOIN floor f ON r.floor_id = f.floor_id
        `;
        const [rooms] = await pool.query(sql);
        const [acks] = await pool.query("SELECT * FROM alert_acks");
        const ackSet = new Set(acks.map(a => `${a.room_number}-${a.alert_type}`));

        let alerts = [];
        let idCounter = 1;

        const addAlert = (room, type, message, severity, value, sensor) => {
            alerts.push({ 
                id: idCounter++, room_id: room.room_id, room_number: room.room_number, 
                floor: room.floor_number, type, message, severity, status: 'Active', 
                value, sensor, time: 'Just now', 
                is_acknowledged: ackSet.has(`${room.room_number}-${type}`)
            });
        };

        rooms.forEach(room => {
            if (room.humidity > 96 || room.leak_detected) addAlert(room, 'Water Leak', `High humidity (${room.humidity}%) or leak detected. System at risk.`, 'critical', room.humidity, 'Humidity Sensor');
            if (room.noise > 120 || room.siren) addAlert(room, 'Alarm Active', `Siren is active or noise level is critical (${room.noise}dB).`, 'critical', room.noise, 'Sound Sensor');
            if (room.smoke > 50) addAlert(room, 'Smoke Detected', `Smoke level ${room.smoke} ppm detected in room.`, 'critical', room.smoke, 'Smoke Sensor');
            if (room.temp > 34) addAlert(room, 'High Temperature', `Temperature ${room.temp}°C above safe threshold.`, 'warning', room.temp, 'Temperature Sensor');
        });
        res.json(alerts);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.put('/api/alerts/resolve/:room_number/:alert_type', async (req, res) => {
    const { room_number, alert_type } = req.params;
    try {
        const [rooms] = await pool.query("SELECT room_id FROM room WHERE room_number = ?", [room_number]);
        if (rooms.length === 0) return res.status(404).json({ error: "Room not found" });
        const roomId = rooms[0].room_id;

        let sql = "";
        if (alert_type === 'Water Leak') sql = `UPDATE room_iot_state SET sprinkler = 0, updated_at = NOW() WHERE room_id = ?`; 
        else if (alert_type === 'Alarm Active') sql = `UPDATE room_iot_state SET siren = 0, tv = 0, updated_at = NOW() WHERE room_id = ?`; 
        else if (alert_type === 'Smoke Detected' || alert_type === 'High Temperature') sql = `UPDATE room_iot_state SET siren = 0, fan = 1, curtain = 1, door_lock = 0, door_open = 1, updated_at = NOW() WHERE room_id = ?`;

        if (sql) await pool.query(sql, [roomId]);
        await pool.query("DELETE FROM alert_acks WHERE room_number = ? AND alert_type = ?", [room_number, alert_type]);

        res.json({ message: "Action taken! Actuators resetting." });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==========================================
// --- API THIẾT BỊ VÀ ĐIỀU KHIỂN IOT ---
// ==========================================
app.get('/api/iot/all', async (req, res) => {
    try {
        const [rows] = await pool.query(`
            SELECT i.*, r.room_number 
            FROM room_iot_state i
            JOIN room r ON i.room_id = r.room_id
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/iot/:room_number', async (req, res) => {
    try {
        const sql = `
            SELECT i.* FROM room_iot_state i
            JOIN room r ON i.room_id = r.room_id
            WHERE r.room_number = ?
        `;
        const [data] = await pool.query(sql, [req.params.room_number]);
        if (data.length > 0) res.json(data[0]);
        else res.status(404).json({ error: "Không tìm thấy dữ liệu IoT cho phòng này" });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.put('/api/iot/:room_number/control', async (req, res) => {
    const { deviceKey, value, brightness } = req.body; 
    try {
        const [rooms] = await pool.query("SELECT room_id FROM room WHERE room_number = ?", [req.params.room_number]);
        if (rooms.length === 0) return res.status(404).json({ error: "Không tìm thấy phòng" });
        
        const roomId = rooms[0].room_id;
        const valNum = (value === true || value === 1 || value === '1') ? 1 : 0;
        
        if (deviceKey === 'door_lock') {
            const doorOpenValue = valNum ? 0 : 1;
            const sql = `UPDATE room_iot_state SET door_lock = ?, door_open = ?, updated_at = NOW() WHERE room_id = ?`;
            await pool.query(sql, [valNum, doorOpenValue, roomId]);
        } else {
            const sql = `UPDATE room_iot_state SET ${deviceKey} = ?, updated_at = NOW() WHERE room_id = ?`;
            await pool.query(sql, [valNum, roomId]);
        }

        if (brightness !== undefined && (deviceKey === 'main_light' || deviceKey === 'desk_lamp')) {
            const brightnessCol = deviceKey === 'main_light' ? 'light_brightness' : 'desk_brightness';
            await pool.query(`UPDATE room_iot_state SET ${brightnessCol} = ?, updated_at = NOW() WHERE room_id = ?`, [brightness, roomId]);
        }

        // Phát MQTT cho Mosquitto Bridge dội xuống Pi
        const controlTopic = `hotel/room/${req.params.room_number}/control`;
        const payload = JSON.stringify({ 
            sender: 'cloud_render',
            device: deviceKey, 
            state: valNum, 
            ...(brightness !== undefined && { brightness })
        });
        mqttClient.publish(controlTopic, payload, { qos: 1 });

        // Bắn Socket.io tức thì cho tất cả Client đang mở Web
        const updatedData = {
            room_number: req.params.room_number,
            [deviceKey]: valNum,
            ...(deviceKey === 'door_lock' && { door_open: valNum ? 0 : 1 }),
            ...(brightness !== undefined && { 
                [deviceKey === 'main_light' ? 'light_brightness' : 'desk_brightness']: brightness 
            })
        };
        io.to(`room_${req.params.room_number}`).emit('room_state_changed', updatedData);
        io.emit('room_update_global', updatedData);

        res.json({ message: "Đã cập nhật thiết bị" });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==========================================
// --- API CHO EDGE AI AGENT ---
// ==========================================
app.get('/api/sensors/snapshot', async (req, res) => {
    try {
        const sql = `
            SELECT r.room_number, i.temp, i.humidity, i.co2, i.motion,
                   i.light, i.noise, i.smoke, i.energy
            FROM room_iot_state i
            JOIN room r ON i.room_id = r.room_id
        `;
        const [rows] = await pool.query(sql);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/prediction', async (req, res) => {
    const {
        room_number, model_name, model_version,
        predicted_humidity, predicted_co2, predicted_energy_kwh
    } = req.body;
    try {
        await pool.query(
            `INSERT INTO ai_prediction
                (room_number, model_name, model_version, predicted_humidity, predicted_co2, predicted_energy_kwh)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [room_number, model_name || 'RandomForest', model_version || null, predicted_humidity, predicted_co2, predicted_energy_kwh]
        );
        res.status(201).json({ message: "Prediction saved" });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/prediction/latest', async (req, res) => {
    try {
        const sql = `
            SELECT p1.room_number, p1.model_name, p1.model_version,
                   p1.predicted_humidity, p1.predicted_co2, p1.predicted_energy_kwh, p1.predicted_at
            FROM ai_prediction p1
            INNER JOIN (
                SELECT room_number, MAX(predicted_at) AS max_time
                FROM ai_prediction
                GROUP BY room_number
            ) p2 ON p1.room_number = p2.room_number AND p1.predicted_at = p2.max_time
        `;
        const [rows] = await pool.query(sql);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/prediction/:room_number', async (req, res) => {
    try {
        const sql = `
            SELECT room_number, model_name, model_version,
                   predicted_humidity, predicted_co2, predicted_energy_kwh, predicted_at
            FROM ai_prediction
            WHERE room_number = ?
            ORDER BY predicted_at DESC
            LIMIT 1
        `;
        const [rows] = await pool.query(sql, [req.params.room_number]);
        if (rows.length === 0) return res.status(404).json({ error: "Chưa có prediction cho phòng này" });
        res.json(rows[0]);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/perf', async (req, res) => {
    const { component, gateway_id, metric_name, metric_value, unit } = req.body;
    try {
        await pool.query(
            `INSERT INTO perf_metric (component, gateway_id, metric_name, metric_value, unit)
             VALUES (?, ?, ?, ?, ?)`,
            [component, gateway_id, metric_name, metric_value, unit]
        );
        res.status(201).json({ message: "Perf metric logged" });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.put('/api/gateways/:id/heartbeat', async (req, res) => {
    const t0 = Date.now();
    try {
        await pool.query(
            `INSERT INTO edge_gateway (gateway_id, node_name, status, last_heartbeat)
             VALUES (?, ?, 'online', NOW())
             ON DUPLICATE KEY UPDATE status = 'online', last_heartbeat = NOW()`,
            [req.params.id, req.body.node_name || `Gateway-${req.params.id}`]
        );
        res.json({ message: "Heartbeat OK", latency_ms: Date.now() - t0 });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// STATE RECONCILIATION ĐỒNG BỘ 2 CHIỀU (PI <-> RENDER)
// ============================================================
const SYNCABLE_COLUMNS = [
    'temp', 'humidity', 'co2', 'noise', 'light', 'motion', 'smoke', 'siren',
    'main_light', 'desk_lamp', 'bedside_lamp', 'tv', 'ac_power', 'ac_temp',
    'fan', 'sprinkler', 'curtain', 'door_open', 'door_lock', 'main_power',
    'energy', 'leak_detected', 'light_brightness', 'main_brightness', 'desk_brightness'
];

app.post('/api/sync/rooms', async (req, res) => {
    const { rows } = req.body;
    if (!Array.isArray(rows) || rows.length === 0) {
        return res.json({ message: "Không có dữ liệu để sync", updated: 0, skipped: 0, cloudRows: [] });
    }

    let updated = 0, skipped = 0;
    try {
        for (const row of rows) {
            const { room_number, updated_at_ms, updated_at } = row;
            if (!room_number) { skipped++; continue; }

            const [rooms] = await pool.query("SELECT room_id FROM room WHERE room_number = ?", [room_number]);
            if (rooms.length === 0) { skipped++; continue; }
            const roomId = rooms[0].room_id;

            const [current] = await pool.query(
                "SELECT UNIX_TIMESTAMP(updated_at) * 1000 AS current_time_ms FROM room_iot_state WHERE room_id = ?", 
                [roomId]
            );
            
            const currentTime = current[0]?.current_time_ms ? Number(current[0].current_time_ms) : 0;
            const incomingTime = updated_at_ms ? Number(updated_at_ms) : (updated_at ? new Date(updated_at).getTime() : 0);

            if (currentTime - incomingTime > 1000) {
                skipped++;
                continue;
            }

            const cols = Object.keys(row).filter(c => 
                SYNCABLE_COLUMNS.includes(c) && 
                row[c] !== undefined && 
                row[c] !== null
            );
            if (cols.length === 0) { skipped++; continue; }

            const setClause = cols.map(c => `${c} = ?`).join(', ');
            const values = cols.map(c => row[c]);
            
            values.push(new Date(incomingTime), roomId);

            await pool.query(
                `UPDATE room_iot_state SET ${setClause}, updated_at = ? WHERE room_id = ?`,
                values
            );
            updated++;

            // Bắn WebSocket cập nhật cho Frontend
            io.to(`room_${room_number}`).emit('room_state_changed', row);
        }

        // Lấy lại danh sách để trả về cho Pi (Loại trừ phòng thật để không bao giờ ghi đè Pi)
        const [cloudRows] = await pool.query(`
            SELECT r.room_number, i.* 
            FROM room_iot_state i 
            JOIN room r ON i.room_id = r.room_id
            WHERE r.room_number NOT IN ('0101', '0102', '101', '102')
        `);

        res.json({ message: "Sync xong", updated, skipped, cloudRows });
    } catch (error) {
        console.error("Lỗi sync tại Render:", error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================================
// --- HỆ THỐNG MÔ PHỎNG VẬT LÝ IOT (CHỈ ÁP DỤNG CHO PHÒNG ẢO) ---
// ============================================================
const randomNoise = (min, max) => Math.random() * (max - min) + min;
const clamp = (val, min, max) => Math.min(Math.max(val, min), max);

const runIoTSimulation = async () => {
    try {
        const sql = `
            SELECT i.*, r.room_number 
            FROM room_iot_state i
            JOIN room r ON i.room_id = r.room_id
        `;
        const [rooms] = await pool.query(sql);
        
        for (let room of rooms) {
            const rNum = String(room.room_number).trim();
            // Tuyệt đối không chạm vào phòng thật
            if (REAL_ROOMS.includes(rNum) || rNum === '0101' || rNum === '101') continue;

            let currentEnergy = Number(room.energy) || 0;
            let energyCost = 0; 
            let currentSiren = room.siren;

            let targetTemp = 32.0;       
            let targetHumidity = 65.0;   
            let targetCo2 = 400.0;       
            let targetLight = 5.0;      
            let targetNoise = 30.0;      

            let currentSmoke = Number(room.smoke) || 0;
            if (Math.random() < 0.03) {
                currentSmoke += randomNoise(15, 30); 
            }
            let smokeClearRate = 0.5; 

            if (room.main_power) {
                energyCost += 0.001; 
                if (room.main_light) { 
                    const mainBri = (Number(room.light_brightness) || 100) / 100;
                    targetLight += 300 * mainBri; 
                    energyCost += 0.01 * mainBri;
                }
                if (room.desk_lamp) { 
                    const deskBri = (Number(room.desk_brightness) || 100) / 100;
                    targetLight += 100 * deskBri; 
                    energyCost += 0.005 * deskBri; 
                }
                if (room.bedside_lamp) { targetLight += 50; energyCost += 0.002; }
                if (room.tv) { targetLight += 30; targetNoise += 35; energyCost += 0.02; }
                
                if (room.ac_power) { 
                    targetTemp = room.ac_temp; 
                    targetHumidity = 45.0; 
                    energyCost += 0.05; 
                }
                if (room.fan) { 
                    targetHumidity -= 10; 
                    targetCo2 = Math.max(400, targetCo2 - 50); 
                    targetNoise += 15; 
                    energyCost += 0.01; 
                    smokeClearRate += 20; 
                }
                if (room.sprinkler) { 
                    targetHumidity = 100; 
                    targetTemp = 25.0; 
                    energyCost += 0.03; 
                    smokeClearRate += 100; 
                }
            }

            if (room.curtain) { targetLight += 400; targetCo2 = 400; smokeClearRate += 15; }
            if (room.door_open) { targetCo2 = 400; smokeClearRate += 15; }
            if (room.motion) { targetCo2 += 150; targetTemp += 0.5; }

            let newSmoke = Math.max(0, currentSmoke - smokeClearRate); 
            if (newSmoke > 0) newSmoke += randomNoise(-0.2, 0.2); 

            if (newSmoke > 50 && !currentSiren && room.main_power) {
                currentSiren = true;
            }
            if (currentSiren) { targetNoise = 100; energyCost += 0.01; }

            let newTemp = clamp((Number(room.temp) || 25) + (targetTemp - (Number(room.temp) || 25)) * 0.5 + randomNoise(-0.1, 0.1), 16, 45);
            let newHumidity = clamp((Number(room.humidity) || 60) + (targetHumidity - (Number(room.humidity) || 60)) * 0.6 + randomNoise(-0.5, 0.5), 20, 100);
            let newLight = clamp(targetLight + randomNoise(-2, 2), 0, 1500);
            let newMotion = Math.random() < 0.05 ? !room.motion : room.motion;
            let newCo2 = clamp((Number(room.co2) || 450) + (targetCo2 - (Number(room.co2) || 450)) * 0.7 + randomNoise(-2, 2), 300, 2000);
            let newNoise = clamp(targetNoise + randomNoise(-1, 1), 20, 130);
            
            let newEnergy = currentEnergy + energyCost;
            let leakDetected = newHumidity > 98;

            const sqlUpdate = `
                UPDATE room_iot_state 
                SET temp=?, humidity=?, smoke=?, co2=?, light=?, noise=?, motion=?, energy=?, leak_detected=?, siren=?
                WHERE room_id=?
            `;
            await pool.query(sqlUpdate, [
                newTemp.toFixed(2), newHumidity.toFixed(2), newSmoke.toFixed(2), 
                newCo2.toFixed(2), newLight.toFixed(2), newNoise.toFixed(2), 
                newMotion, newEnergy.toFixed(3), leakDetected, currentSiren, room.room_id
            ]);
        }
    } catch (error) {
        console.error("Lỗi mô phỏng IoT:", error);
    }
};

// ============================================================
// 🎙️ VOICE COMMAND — LLM-BACKED INTENT PARSING
// ============================================================
const VOICE_SYSTEM_PROMPT = `
    You are a smart hotel room assistant. Extract intent from the user's text and return a strict JSON object.
    Valid Devices: main_light, bedside_lamp, desk_lamp, curtain, fan, ac_power, door_lock, tv, siren, sprinkler, main_power, temp, humidity, light, motion, smoke, energy, co2, noise, leak.
    Return ONLY a valid JSON object: { "device": "device_name", "action": "ON/OFF/QUERY", "type": "CONTROL/QUERY" }
`;

const voiceCache = new Map();
const VOICE_CACHE_TTL_MS = 10_000;

function getCachedIntent(text) {
    const key = text.trim().toLowerCase();
    const hit = voiceCache.get(key);
    if (hit && hit.expiresAt > Date.now()) return hit.intent;
    if (hit) voiceCache.delete(key);
    return null;
}
function setCachedIntent(text, intent) {
    const key = text.trim().toLowerCase();
    voiceCache.set(key, { intent, expiresAt: Date.now() + VOICE_CACHE_TTL_MS });
}

async function callGroq(text) {
    const resp = await fetch('https://api.groq.com/openai/v1/chat/completions', {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${GROQ_API_KEY}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            model: 'openai/gpt-oss-20b',
            messages: [
                { role: 'system', content: VOICE_SYSTEM_PROMPT },
                { role: 'user', content: text }
            ],
            temperature: 0,
            response_format: { type: 'json_object' }
        })
    });

    if (!resp.ok) {
        const body = await resp.text().catch(() => '');
        const reason = resp.status === 429 ? 'RATE_LIMITED' : `HTTP_${resp.status}`;
        throw new Error(`Groq ${reason}: ${body.slice(0, 200)}`);
    }
    const data = await resp.json();
    return JSON.parse(data.choices[0].message.content);
}

async function callOpenRouter(text) {
    if (!OPENROUTER_API_KEY) throw new Error('OpenRouter skipped: no API key configured');

    const resp = await fetch('https://openrouter.ai/api/v1/chat/completions', {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            model: 'openrouter/free',
            messages: [
                { role: 'system', content: VOICE_SYSTEM_PROMPT },
                { role: 'user', content: text }
            ],
            temperature: 0,
            response_format: { type: 'json_object' }
        })
    });

    if (!resp.ok) {
        const body = await resp.text().catch(() => '');
        const reason = resp.status === 429 ? 'RATE_LIMITED' : `HTTP_${resp.status}`;
        throw new Error(`OpenRouter ${reason}: ${body.slice(0, 200)}`);
    }
    const data = await resp.json();
    return JSON.parse(data.choices[0].message.content);
}

async function classifyIntent(text) {
    const cached = getCachedIntent(text);
    if (cached) return cached;

    try {
        const intent = await callGroq(text);
        setCachedIntent(text, intent);
        return intent;
    } catch (groqErr) {
        try {
            const intent = await callOpenRouter(text);
            setCachedIntent(text, intent);
            return intent;
        } catch (orErr) {
            throw new Error('All LLM providers failed');
        }
    }
}

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

app.post('/api/voice/llm-command', async (req, res) => {
    try {
        const { text, room_number } = req.body;
        if (!text || !room_number) return res.status(400).json({ success: false, message: "Thiếu dữ liệu đầu vào." });

        let aiIntent;
        try {
            aiIntent = await classifyIntent(text);
        } catch (llmErr) {
            return res.json({ success: false, message: "Voice service temporarily unavailable." });
        }

        const { device, action, type } = aiIntent;
        if (!device || device === "none" || device === "unknown") {
            return res.json({ success: false, message: "Sorry, I didn't catch that." });
        }

        if (type === "QUERY") {
            const [rooms] = await pool.query(
                "SELECT i.* FROM room_iot_state i JOIN room r ON i.room_id = r.room_id WHERE r.room_number = ?",
                [room_number]
            );
            if (rooms.length === 0) return res.json({ success: false, message: "Room not found." });

            const value = rooms[0][device];
            let unit = "";
            if (device === "temp") unit = "degrees Celsius";
            if (device === "humidity") unit = "percent";
            if (device === "light") unit = "lux";
            if (device === "co2") unit = "ppm";
            if (device === "noise") unit = "decibels";
            if (device === "energy") unit = "kWh";

            let msg;
            if (device === "motion") {
                msg = value ? pick(["Motion detected in the room.", "Yes, someone appears to be in the room."])
                            : pick(["No motion detected.", "No, the room looks empty right now."]);
            } else if (device === "leak") {
                msg = value ? "Warning! Water leak detected!" : pick(["No water leak detected.", "All clear — no leaks right now."]);
            } else if (device === "smoke") {
                msg = value > 50 ? pick([`Warning! Smoke level is high at ${value} ppm.`, `Careful — smoke reads ${value} ppm.`])
                                 : pick([`Smoke level is normal at ${value} ppm.`, `No concern — smoke reading is ${value} ppm.`]);
            } else {
                msg = pick([
                    `The current ${device.replace('_', ' ')} is ${value} ${unit}.`,
                    `Right now it's ${value} ${unit}.`,
                    `${device.replace('_', ' ')} reads ${value} ${unit}.`
                ]);
            }

            return res.json({ success: true, type: "QUERY", message: msg });
        }

        if (type === "CONTROL") {
            const [rooms] = await pool.query("SELECT room_id FROM room WHERE room_number = ?", [room_number]);
            if (rooms.length === 0) return res.json({ success: false, message: "Room not found." });
            const roomId = rooms[0].room_id;
            const boolState = action === "ON";
            const actionWord = boolState ? "turned on" : "turned off";

            const readOnlyDevices = ['door_open', 'motion', 'temp', 'humidity', 'leak', 'smoke', 'co2', 'light', 'noise', 'energy'];
            if (readOnlyDevices.includes(device)) {
                return res.json({ success: false, message: "I cannot control that sensor." });
            }

            if (device === 'door_lock') {
                await pool.query(
                    `UPDATE room_iot_state SET door_lock = ?, door_open = ?, updated_at = NOW() WHERE room_id = ?`,
                    [boolState, !boolState, roomId]
                );
            } else {
                await pool.query(`UPDATE room_iot_state SET ${device} = ?, updated_at = NOW() WHERE room_id = ?`, [boolState, roomId]);
            }

            const controlTopic = `hotel/room/${room_number}/control`;
            mqttClient.publish(controlTopic, JSON.stringify({ sender: 'cloud_render', device, state: boolState }), { qos: 1 });

            // Bắn Socket.io cập nhật
            const voiceUpdate = {
                room_number,
                [device]: boolState ? 1 : 0,
                ...(device === 'door_lock' && { door_open: boolState ? 0 : 1 })
            };
            io.to(`room_${room_number}`).emit('room_state_changed', voiceUpdate);
            io.emit('room_update_global', voiceUpdate);

            return res.json({ success: true, type: "CONTROL", message: `Successfully ${actionWord} the ${device.replace('_', ' ')}.` });
        }

        return res.json({ success: false, message: "Action not supported." });

    } catch (error) {
        console.error("Voice command error:", error.message);
        res.status(500).json({ success: false, message: "Server error." });
    }
});

// Endpoint giữ sống dịch vụ cho UptimeRobot / Cron monitor
app.get('/health', (req, res) => res.json({ status: 'ok' }));

// ============================================================
// ============================================================
// setInterval(runIoTSimulation, 5000);

module.exports = app;

if (require.main === module) {
    const PORT = process.env.PORT || 5000;
    server.listen(PORT, () => {
        console.log(`🚀 API Hotel Server chạy tại port ${PORT}`);
    });
}
