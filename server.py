# -*- coding: utf-8 -*-
"""
Бэкенд для маркетплейса «Сеть обмена 101».
Стек: Python 3.11 + FastAPI + SQLite + WebSocket
"""

import json
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ==================== ПУТИ И БАЗА ====================

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"
DB_PATH = DATA_DIR / "marketplace.db"

DATA_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)


def init_db():
    """Создаёт таблицы, если их нет."""
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS products (
            id           INTEGER PRIMARY KEY,
            title        TEXT NOT NULL,
            description  TEXT NOT NULL,
            category     TEXT NOT NULL,
            categoryName TEXT,
            price        REAL NOT NULL,
            oldPrice     REAL,
            rating       REAL DEFAULT 5,
            badge        TEXT,
            custom       INTEGER DEFAULT 1,
            image        TEXT,
            createdAt    INTEGER
        );

        CREATE TABLE IF NOT EXISTS messages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            productId  INTEGER NOT NULL,
            fromUser   TEXT NOT NULL,
            text       TEXT NOT NULL,
            time       INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_messages_product
            ON messages(productId, time);
    """)
    conn.commit()
    conn.close()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ==================== WEB-SOCKET МЕНЕДЖЕР ====================

class ChatManager:
    """Управляет активными WebSocket-подключениями и комнатами."""

    def __init__(self):
        # Комнаты: {productId: set(WebSocket)}
        self.rooms: dict[int, set[WebSocket]] = {}
        # Все клиенты
        self.clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket):
        self.clients.discard(ws)
        for room in self.rooms.values():
            room.discard(ws)

    def join(self, product_id: int, ws: WebSocket):
        self.rooms.setdefault(product_id, set()).add(ws)

    def leave(self, product_id: int, ws: WebSocket):
        if product_id in self.rooms:
            self.rooms[product_id].discard(ws)

    async def send_to_room(self, product_id: int, data: dict):
        """Отправить сообщение всем в комнате товара."""
        room = self.rooms.get(product_id)
        if not room:
            return
        msg = json.dumps(data, ensure_ascii=False)
        dead = []
        for ws in list(room):
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            room.discard(ws)

    async def broadcast(self, data: dict):
        """Отправить всем клиентам."""
        msg = json.dumps(data, ensure_ascii=False)
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


manager = ChatManager()


# ==================== LIFESPAN ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    print()
    print("=" * 50)
    print("✅ Сервер запущен")
    print("🌐 Открой: http://localhost:3000")
    print("=" * 50)
    print()
    yield


app = FastAPI(lifespan=lifespan)


# ==================== API: ТОВАРЫ ====================

@app.get("/api/products")
async def get_products():
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM products ORDER BY createdAt DESC"
        ).fetchall()
        result = []
        for r in rows:
            result.append({
                "id": r["id"],
                "title": r["title"],
                "desc": r["description"],
                "category": r["category"],
                "categoryName": r["categoryName"],
                "price": r["price"],
                "oldPrice": r["oldPrice"],
                "rating": r["rating"],
                "badge": r["badge"],
                "custom": bool(r["custom"]),
                "image": r["image"],
                "icon": None,
            })
        return result
    finally:
        conn.close()


@app.post("/api/products")
async def create_product(request: Request):
    data = await request.json()

    if not data.get("title") or not data.get("price"):
        return JSONResponse(
            status_code=400,
            content={"error": "Не хватает полей: title, price"}
        )

    new_id = int(time.time() * 1000)
    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO products
                (id, title, description, category, categoryName, price,
                 oldPrice, rating, badge, custom, image, createdAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            new_id,
            data.get("title", ""),
            data.get("desc", ""),
            data.get("category", "set"),
            data.get("categoryName"),
            float(data.get("price", 0)),
            data.get("oldPrice"),
            float(data.get("rating", 5)),
            data.get("badge"),
            1 if data.get("custom", True) else 0,
            data.get("image"),
            int(time.time() * 1000),
        ))
        conn.commit()
    finally:
        conn.close()

    # Оповещаем всех
    await manager.broadcast({"type": "products_updated"})

    return {**data, "id": new_id}


@app.delete("/api/products/{product_id}")
async def delete_product(product_id: int):
    conn = get_db()
    try:
        conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
        conn.execute("DELETE FROM messages WHERE productId = ?", (product_id,))
        conn.commit()
    finally:
        conn.close()

    await manager.broadcast({"type": "products_updated"})
    return {"ok": True}


# ==================== API: ЧАТЫ ====================

@app.get("/api/chats")
async def get_chats():
    """Список всех чатов — по одному на товар, с последним сообщением."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT
                m.productId AS productId,
                p.title     AS title,
                p.image     AS image,
                p.price     AS price,
                m.text      AS lastText,
                m.fromUser  AS lastFrom,
                m.time      AS lastTime
            FROM messages m
            JOIN products p ON p.id = m.productId
            WHERE m.id = (
                SELECT id FROM messages
                WHERE productId = m.productId
                ORDER BY time DESC LIMIT 1
            )
            ORDER BY m.time DESC
        """).fetchall()

        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/chats/{product_id}")
async def get_chat_history(product_id: int):
    """История чата по конкретному товару."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT fromUser AS "from", text, time
            FROM messages
            WHERE productId = ?
            ORDER BY time ASC
        """, (product_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.delete("/api/chats/{product_id}")
async def clear_chat(product_id: int):
    """Очистить чат по товару."""
    conn = get_db()
    try:
        conn.execute("DELETE FROM messages WHERE productId = ?", (product_id,))
        conn.commit()
    finally:
        conn.close()

    await manager.send_to_room(product_id, {
        "type": "chat_cleared",
        "productId": product_id,
    })
    await manager.broadcast({"type": "chats_updated"})
    return {"ok": True}


# ==================== WEB-SOCKET ====================

@app.websocket("/ws/chat")
async def websocket_chat(ws: WebSocket):
    await manager.connect(ws)
    print(f"🔌 Подключён клиент")

    try:
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")

            if msg_type == "join":
                pid = data.get("productId")
                if pid is not None:
                    manager.join(int(pid), ws)
                    print(f"→ Клиент зашёл в комнату {pid}")

            elif msg_type == "leave":
                pid = data.get("productId")
                if pid is not None:
                    manager.leave(int(pid), ws)

            elif msg_type == "message":
                pid = data.get("productId")
                sender = data.get("from")
                text = data.get("text")

                if pid is None or sender is None or text is None:
                    continue
                if sender not in ("me", "unit"):
                    continue

                text = str(text)[:1000]
                ts = int(time.time() * 1000)

                # Сохраняем в БД
                conn = get_db()
                try:
                    conn.execute("""
                        INSERT INTO messages (productId, fromUser, text, time)
                        VALUES (?, ?, ?, ?)
                    """, (int(pid), sender, text, ts))
                    conn.commit()
                finally:
                    conn.close()

                # Рассылаем всем в комнате
                await manager.send_to_room(int(pid), {
                    "type": "message",
                    "productId": int(pid),
                    "from": sender,
                    "text": text,
                    "time": ts,
                })

                # Оповещаем всех об обновлении списка чатов
                await manager.broadcast({"type": "chats_updated"})

    except WebSocketDisconnect:
        manager.disconnect(ws)
        print("❌ Клиент отключён")
    except Exception as e:
        manager.disconnect(ws)
        print(f"⚠️ Ошибка WS: {e}")


# ==================== СТАТИКА ====================

@app.get("/")
async def root():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return JSONResponse({"error": "index.html не найден в папке static/"}, status_code=404)


# Отдаём статику (только после всех API-роутов)
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


# ==================== ЗАПУСК ====================
if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", 3000))
    uvicorn.run(app, host="0.0.0.0", port=port)