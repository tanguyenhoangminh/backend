const express = require('express');
const cors = require('cors');
const mysql = require('mysql2/promise');
const mqtt = require('mqtt');
require('dotenv').config(); 

const GROQ_API_KEY = process.env.GROQ_API_KEY;
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;

const app = express();
app.use(cors());
app.use(express.json());

// Kết nối MariaDB Local trên Raspberry Pi
const pool = mysql.createPool({
    host: '127.0.0.1',
    port: 3306,
    user: 'root',
    password: 'minhmongmo1',
    database: 'iot_hotel',
    timezone: '+00:00',          // FIX: force UTC tránh lệch múi giờ
    waitForConnections: true,
    connectionLimit: 50,
    queueLimit: 0
});

pool.query(`ALTER TABLE room_iot_state ADD COLUMN IF NOT EXISTS light_brightness TINYINT UNSIGNED DEFAULT 100`).catch(() => {});
pool.query(`ALTER TABLE room_iot_state ADD COLUMN IF NOT EXISTS desk_brightness INT DEFAULT 100`).catch(() => {});
pool.query(`ALTER TABLE room_iot_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`).catch(() => {});

pool.query(`
    CREATE TABLE IF NOT EXISTS alert_acks (
        room_number VARCHAR(10) NOT NULL,
        alert_type VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (room_number, alert_type)
    )
`).then(() => console.log("✅ Alert_acks ready"))
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
        predicted_humidity  DECIMAL(6,3),
        predicted_co2       DECIMAL(8,2),
        predicted_energy_kwh DECIMAL(8,4),
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
// MQTT LOCAL (Mosquitto trên Pi)
// ==========================================
const REAL_ROOMS = ['0101', '0102'];
const MQTT_BROKER = 'mqtt://127.0.0.1:1883'; 
const MQTT_OPTIONS = {
    clientId: 'hotel_edge_gateway_' + Math.random().toString(16).substring(2, 8)
};

const mqttClient = mqtt.connect(MQTT_BROKER, MQTT_OPTIONS);

mqttClient.on('connect', () => {
    console.log("☁️ Đã kết nối MQTT Local với Mosquitto qua cổng 1883!");
    mqttClient.subscribe('hotel/room/+/sensors', (err) => {
        if (!err) console.log("📡 Đang lắng nghe dữ liệu cảm biến từ mạch thật...");
    });
    mqttClient.subscribe('hotel/room/+/control');
});

mqttClient.on('message', async (topic, message) => {
    try {
        const topicParts = topic.split('/');
        const roomNumber = topicParts[2]; 
        const actionType = topicParts[3];
        
        const [rooms] = await pool.query("SELECT room_id FROM room WHERE room_number = ?", [roomNumber]);
        if (rooms.length === 0) return;
        const roomId = rooms[0].room_id;
        const payload = JSON.parse(message.toString());

        // 1. Nhận cảm biến từ mạch thật
        if (actionType === 'sensors') {
            if (!REAL_ROOMS.includes(roomNumber)) return;

            const fields = [];
            const values = [];

            if (payload.temp            !== undefined) { fields.push('temp=?');             values.push(payload.temp); }
            if (payload.humidity        !== undefined) { fields.push('humidity=?');         values.push(payload.humidity); }
            if (payload.co2             !== undefined) { fields.push('co2=?');              values.push(payload.co2); }
            if (payload.noise           !== undefined) { fields.push('noise=?');            values.push(payload.noise); }
            if (payload.light           !== undefined) { fields.push('light=?');            values.push(payload.light); }
            if (payload.motion          !== undefined) { fields.push('motion=?');           values.push(payload.motion); }
            if (payload.smoke           !== undefined) { fields.push('smoke=?');            values.push(payload.smoke); }
            if (payload.smoke_alert     !== undefined) { fields.push('siren=?');            values.push(payload.smoke_alert); }
            if (payload.main_light      !== undefined) { fields.push('main_light=?');       values.push(payload.main_light); }
            if (payload.desk_lamp       !== undefined) { fields.push('desk_lamp=?');        values.push(payload.desk_lamp); }
            if (payload.main_brightness !== undefined) { fields.push('main_brightness=?');  values.push(payload.main_brightness); }
            if (payload.desk_brightness !== undefined) { fields.push('desk_brightness=?');  values.push(payload.desk_brightness); }

            if (fields.length > 0) {
                fields.push('updated_at = NOW()');
                values.push(roomId);
                await pool.query(`UPDATE room_iot_state SET ${fields.join(', ')} WHERE room_id=?`, values);
            }
        }

        // 2. Nhận lệnh điều khiển dội từ Cloud Render xuống qua Mosquitto Bridge
        if (actionType === 'control') {
            // Bỏ qua nếu chính Pi phát hoặc Pi sync phát (tránh vòng lặp)
            if (payload.sender === 'pi_local_rest' || payload.sender === 'pi_sync') return;

            const dev = payload.device || payload.deviceKey;
            const val = payload.state !== undefined ? payload.state : payload.value;
            const valNum = (val === true || val === 1 || val === '1') ? 1 : 0;

            if (dev) {
                if (dev === 'door_lock') {
                    await pool.query(`UPDATE room_iot_state SET door_lock = ?, door_open = ?, updated_at = NOW() WHERE room_id = ?`, [valNum, valNum ? 0 : 1, roomId]);
                } else {
                    await pool.query(`UPDATE room_iot_state SET ${dev} = ?, updated_at = NOW() WHERE room_id = ?`, [valNum, roomId]);
                }
                if (payload.brightness !== undefined && dev === 'main_light') {
                    await pool.query(`UPDATE room_iot_state SET light_brightness = ?, updated_at = NOW() WHERE room_id = ?`, [payload.brightness, roomId]);
                }
            }
        }
    } catch (error) {
        console.error("Lỗi xử lý tin nhắn MQTT:", error);
    }
});

// ==========================================
// --- API QUẢN LÝ PHÒNG (ROOMS) ---
// ==========================================
app.route('/api/rooms')
    .get(async (req, res) => {
        try {
            const sql = `
                SELECT 
                    r.room_id as id, 
                    r.room_number, 
                    r.status, 
                    f.floor_number as floor, 
                    rt.type_name as type, 
                    rt.base_price as price, 
                    rt.max_occupancy as occupancy, 
                    rt.description as \`desc\`
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
            if (types.length === 0) {
                return res.status(404).json({ error: "Loại phòng không tồn tại trong database!" });
            }
            const sql = `UPDATE room SET type_id = ? WHERE room_number = ?`;
            await pool.query(sql, [types[0].type_id, room_number]);
            res.json({ message: `Cập nhật loại phòng thành công!` });
        } catch (error) {
            res.status(400).json({ error: error.message });
        }
    });

// ==========================================
// --- API QUẢN LÝ KHÁCH HÀNG (GUESTS) ---
// ==========================================
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
        try {
            const sql = `INSERT INTO guest (first_name, last_name, email, phone, nationality, passport_no, gender, date_of_birth, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW())`;
            await pool.query(sql, [first_name, last_name, data.email || `guest_${Date.now()}@hotel.com`, data.phone, data.nationality, data.passport_no, data.gender, data.date_of_birth || '1990-01-01']);
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
        try {
            const sql = `UPDATE guest SET first_name=?, last_name=?, phone=?, nationality=?, passport_no=?, gender=?, date_of_birth=? WHERE guest_id=?`;
            await pool.query(sql, [first_name, last_name, data.phone, data.nationality, data.passport_no, data.gender, data.date_of_birth || '1990-01-01', req.params.id]);
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

// ==========================================
// --- API QUẢN LÝ ĐẶT PHÒNG (BOOKINGS) ---
// ==========================================
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

// ==========================================
// --- API NHÂN VIÊN (STAFF) ---
// ==========================================
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
        res.json(staffList.map(st => ({
            ...st,
            full_name: `${st.first_name} ${st.last_name}`,
            role: st.role_name
        })));
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==========================================
// --- API HỆ THỐNG CẢNH BÁO (ALERTS) ---
// ==========================================
app.post('/api/alerts/acknowledge', async (req, res) => {
    const { alertsToAck } = req.body; 
    if (!alertsToAck || alertsToAck.length === 0) return res.json({ message: "Không có alert nào" });

    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();
        for (let alert of alertsToAck) {
            await connection.query(`INSERT IGNORE INTO alert_acks (room_number, alert_type) VALUES (?, ?)`, [alert.room_number, alert.type]);
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
            if (Number(room.humidity) > 96 || room.leak_detected) addAlert(room, 'Water Leak', `High humidity (${room.humidity}%) or leak detected. System at risk.`, 'critical', room.humidity, 'Humidity Sensor');
            if (Number(room.noise) > 120 || room.siren) addAlert(room, 'Alarm Active', `Siren is active or noise level is critical (${room.noise}dB).`, 'critical', room.noise, 'Sound Sensor');
            if (Number(room.smoke) > 50) addAlert(room, 'Smoke Detected', `Smoke level ${room.smoke} ppm detected in room.`, 'critical', room.smoke, 'Smoke Sensor');
            if (Number(room.temp) > 34) addAlert(room, 'High Temperature', `Temperature ${room.temp}°C above safe threshold.`, 'warning', room.temp, 'Temperature Sensor');
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
            SELECT i.*, r.room_number FROM room_iot_state i
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
            await pool.query(`UPDATE room_iot_state SET door_lock = ?, door_open = ?, updated_at = NOW() WHERE room_id = ?`, [valNum, valNum ? 0 : 1, roomId]);
        } else {
            await pool.query(`UPDATE room_iot_state SET ${deviceKey} = ?, updated_at = NOW() WHERE room_id = ?`, [valNum, roomId]);
        }

        if (brightness !== undefined && deviceKey === 'main_light') {
            await pool.query(`UPDATE room_iot_state SET light_brightness = ?, updated_at = NOW() WHERE room_id = ?`, [brightness, roomId]);
        }

        // Gắn sender: 'pi_local_rest' để tránh vòng lặp tự nhận lại lệnh của mình
        const controlTopic = `hotel/room/${req.params.room_number}/control`;
        const payload = JSON.stringify({ 
            sender: 'pi_local_rest',
            device: deviceKey, 
            state: valNum,
            ...(brightness !== undefined && { brightness })
        });
        mqttClient.publish(controlTopic, payload, { qos: 1 });

        res.json({ message: "Đã cập nhật thiết bị" });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==========================================
// --- API EDGE AI & METRICS ---
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
// --- HỆ THỐNG MÔ PHỎNG VẬT LÝ IOT (DÀNH CHO EDGE GATEWAY) ---
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
            const isRealRoom = REAL_ROOMS.includes(room.room_number);

            let currentEnergy = parseFloat(room.energy) || 0;
            let energyCost = 0; 
            let currentSiren = room.siren ? 1 : 0;

            let targetTemp = 32.0;       
            let targetHumidity = 65.0;   
            let targetCo2 = 400.0;       
            let targetLight = 5.0;      
            let targetNoise = 30.0;      

            let currentSmoke = parseFloat(room.smoke) || 0;
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
                    targetTemp = parseFloat(room.ac_temp) || 25.0; 
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
                currentSiren = 1;
            }
            if (currentSiren) { targetNoise = 100; energyCost += 0.01; }

            let tempVal  = parseFloat(room.temp) || 25.0;
            let humVal   = parseFloat(room.humidity) || 60.0;
            let lightVal = parseFloat(room.light) || 300.0;
            let motionVal = room.motion ? 1 : 0;

            let newTemp     = tempVal;
            let newHumidity = humVal;
            let newLight    = lightVal;
            let newMotion   = motionVal;

            if (!isRealRoom) {
                newTemp     = clamp(tempVal + (targetTemp - tempVal) * 0.5 + randomNoise(-0.1, 0.1), 16, 45);
                newHumidity = clamp(humVal + (targetHumidity - humVal) * 0.6 + randomNoise(-0.5, 0.5), 20, 100);
                newLight    = clamp(targetLight + randomNoise(-2, 2), 0, 1500);
                newMotion   = Math.random() < 0.05 ? (motionVal ? 0 : 1) : motionVal;
            }

            let newCo2   = clamp((parseFloat(room.co2) || 450) + (targetCo2 - (parseFloat(room.co2) || 450)) * 0.7 + randomNoise(-2, 2), 300, 2000);
            let newNoise = clamp(targetNoise + randomNoise(-1, 1), 20, 130);
            
            let newEnergy    = currentEnergy + energyCost;
            let leakDetected = newHumidity > 98 ? 1 : 0;

            const safeTemp   = Number(newTemp)     || 25.0;
            const safeHum    = Number(newHumidity) || 60.0;
            const safeSmoke  = Number(newSmoke)    || 0.0;
            const safeCo2    = Number(newCo2)      || 400.0;
            const safeLight  = Number(newLight)    || 300.0;
            const safeNoise  = Number(newNoise)    || 30.0;
            const safeEnergy = Number(newEnergy)   || 0.0;

            const sqlUpdate = `
                UPDATE room_iot_state 
                SET temp=?, humidity=?, smoke=?, co2=?, light=?, noise=?, motion=?, energy=?, leak_detected=?, siren=?
                WHERE room_id=?
            `;
            await pool.query(sqlUpdate, [
                safeTemp.toFixed(2), 
                safeHum.toFixed(2), 
                safeSmoke.toFixed(2), 
                safeCo2.toFixed(2), 
                safeLight.toFixed(2), 
                safeNoise.toFixed(2), 
                newMotion, 
                safeEnergy.toFixed(4), 
                leakDetected, 
                currentSiren, 
                room.room_id
            ]);

            if (!isRealRoom) {
                const topic = `hotel/room/${room.room_number}/sensors`;
                const payload = JSON.stringify({
                    temp:     Number(safeTemp.toFixed(2)),
                    humidity: Number(safeHum.toFixed(2)),
                    light:    Number(safeLight.toFixed(2)),
                    motion:   newMotion,
                    smoke:    Number(safeSmoke.toFixed(2)),
                    co2:      Number(safeCo2.toFixed(2)),
                    noise:    Number(safeNoise.toFixed(2)),
                    energy:   Number(safeEnergy.toFixed(4))
                });
                mqttClient.publish(topic, payload, { qos: 0 });
            }
        }
    } catch (error) {
        console.error("Lỗi mô phỏng IoT:", error);
    }
};

// BẬT MÔ PHỎNG TRÊN PI ĐỊNH KỲ MỖI 5S
// setInterval(runIoTSimulation, 5000);

// ============================================================
// STATE RECONCILIATION: ĐỒNG BỘ ĐỊNH KỲ 2 CHIỀU PI <-> RENDER
// FIX: tăng interval 3s→5s, fix timestamp parse, thêm MQTT publish sau sync
// ============================================================
const CLOUD_SYNC_URL = process.env.CLOUD_SYNC_URL || 'https://backend-cz3y.onrender.com/api/sync/rooms';

setInterval(async () => {
    try {
        // Kéo toàn bộ dữ liệu hiện tại trên Pi, gửi kèm mili-giây UNIX epoch
        const [rows] = await pool.query(`
            SELECT r.room_number, i.*, UNIX_TIMESTAMP(i.updated_at) * 1000 AS updated_at_ms
            FROM room_iot_state i 
            JOIN room r ON i.room_id = r.room_id
        `);
        if (!rows || rows.length === 0) return;

        const res = await fetch(CLOUD_SYNC_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ rows }),
            signal: AbortSignal.timeout(4000)
        });

        if (!res.ok) return;
        const data = await res.json();
        
        // Nếu Cloud Render có dữ liệu mới hơn (ai đó vừa bấm trên Vercel/Render) -> Pi cập nhật lại MariaDB
        if (Array.isArray(data.cloudRows)) {
            for (const cr of data.cloudRows) {
                // FIX: ưu tiên dùng updated_at_ms từ cloud, fallback parse string
                const cloudTime = cr.updated_at_ms
                    ? Number(cr.updated_at_ms)
                    : (cr.updated_at ? new Date(cr.updated_at).getTime() : 0);

                // FIX: query bằng room_number thay vì room_id để tránh ID mismatch giữa 2 DB
                const [local] = await pool.query(
                    `SELECT UNIX_TIMESTAMP(i.updated_at) * 1000 AS local_time
                     FROM room_iot_state i
                     JOIN room r ON i.room_id = r.room_id
                     WHERE r.room_number = ?`,
                    [cr.room_number]
                );
                const localTime = Number(local[0]?.local_time) || 0;

                // FIX: tăng tolerance lên 5s để tránh false-override khi latency cao
                if (cloudTime - localTime > 5000) {
                    const [piRoom] = await pool.query(
                        "SELECT room_id FROM room WHERE room_number = ?",
                        [cr.room_number]
                    );
                    if (piRoom.length === 0) continue;
                    const piRoomId = piRoom[0].room_id;

                    await pool.query(
                        `UPDATE room_iot_state SET 
                            main_light = ?, desk_lamp = ?, bedside_lamp = ?, fan = ?, ac_power = ?, 
                            door_lock = ?, door_open = ?, siren = ?, sprinkler = ?, tv = ?, 
                            light_brightness = ?, desk_brightness = ?, updated_at = NOW()
                         WHERE room_id = ?`,
                        [
                            cr.main_light, cr.desk_lamp, cr.bedside_lamp, cr.fan, cr.ac_power,
                            cr.door_lock, cr.door_open, cr.siren, cr.sprinkler, cr.tv,
                            cr.light_brightness, cr.desk_brightness, piRoomId
                        ]
                    );

                    // FIX: Publish MQTT để cập nhật phần cứng thật sau khi sync từ cloud
                    const controlTopic = `hotel/room/${cr.room_number}/control`;
                    const devices = ['main_light', 'desk_lamp', 'bedside_lamp', 'fan', 'ac_power', 'door_lock', 'tv', 'siren', 'sprinkler'];
                    for (const dev of devices) {
                        if (cr[dev] !== undefined) {
                            mqttClient.publish(controlTopic, JSON.stringify({
                                sender: 'pi_sync',   // filter này sẽ bị bỏ qua bởi chính Pi
                                device: dev,
                                state: cr[dev]
                            }), { qos: 1 });
                        }
                    }

                    console.log(`🔄 Pi sync từ cloud: phòng ${cr.room_number} (cloud ${cloudTime} > local ${localTime})`);
                }
            }
        }
    } catch (err) {
        // Bỏ qua lỗi kết nối mạng tạm thời để không gây gián đoạn
    }
}, 5000); // FIX: tăng từ 3000 lên 5000ms

app.get('/health', (req, res) => res.json({ status: 'ok' }));

module.exports = app;

if (require.main === module) {
    const PORT = 5000;
    app.listen(PORT, () => {
        console.log(`🚀 API Hotel Server chạy tại http://localhost:${PORT}`);
    });
}
