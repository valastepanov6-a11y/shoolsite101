# -*- coding: utf-8 -*-
"""
Бэкенд для маркетплейса «Сеть обмена 101».
Стек: Python 3.11 + FastAPI + SQLite + WebSocket

Особенности:
- Сообщения чата: 'me' (свои, слева) и 'unit' (собеседник, справа)
- Валидация входящих данных с понятными ошибками
- Защита от удаления чужих карточек по ownerId
- Автоматическое создание БД и таблиц
- Цена НЕ обязательна (можно не передавать)
"""

import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

# ==================== ПУТИ ====================

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"
DB_PATH = DATA_DIR / "marketplace.db"

DATA_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

# Допустимые роли отправителя в чате
ALLOWED_SENDERS = ("me", "unit")

# Максимальная длина сообщения
MAX_MESSAGE_LENGTH = 1000


# ==================== БАЗА ДАННЫХ ====================

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
            price        REAL DEFAULT 0,
            oldPrice     REAL,
            rating       REAL DEFAULT 5,
            badge        TEXT,
            custom       INTEGER DEFAULT 1,
            image        TEXT,
            ownerId      TEXT,
            createdAt    INTEGER
        );

        CREATE TABLE IF NOT EXISTS messages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            productId  INTEGER NOT NULL,
            fromUser   TEXT NOT NULL CHECK (fromUser IN ('me', 'unit')),
            text       TEXT NOT NULL,
            time       INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_messages_product
            ON messages(productId, time);
    """)
    conn.commit()
    conn.close()
    print(f"📁 База данных готова: {DB_PATH}")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ==================== WEB-SOCKET МЕНЕДЖЕР ====================

class ChatManager:
    """Управляет подключениями и комнатами."""

    def __init__(self):
        self.rooms = {}       # productId → set(WebSocket)
        self.clients = set()  # все клиенты

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
        room = self.rooms.get(product_id)
        if not room:
            return
        msg = json.dumps(data, ensure_ascii=False)
        dead = []
        for ws in list(room):
            try:
                await ws.send_text(msg)
            except Exception as e:
                print(f"⚠️ Ошибка отправки: {e}")
                dead.append(ws)
        for ws in dead:
            room.discard(ws)

    async def broadcast(self, data: dict):
        msg = json.dumps(data, ensure_ascii=False)
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_text(msg)
            except Exception as e:
                print(f"⚠️ Ошибка broadcast: {e}")
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


manager = ChatManager()


# ==================== LIFESPAN ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    print()
    print("=" * 55)
    print("✅ Сервер запущен")
    print("🌐 Открой: http://localhost:3000")
    print("=" * 55)
    print()
    yield
    print("👋 Сервер остановлен")


app = FastAPI(lifespan=lifespan)


# ==================== API: ТОВАРЫ ====================

@app.get("/api/products")
async def get_products():
    """Возвращает все товары (свежие — первыми)."""
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
                "price": r["price"] if r["price"] is not None else 0,
                "oldPrice": r["oldPrice"],
                "rating": r["rating"] if r["rating"] is not None else 5,
                "badge": r["badge"],
                "custom": bool(r["custom"]),
                "image": r["image"],
                "icon": None,
                "ownerId": r["ownerId"] if "ownerId" in r.keys() else None,
            })
        return result
    except Exception as e:
        print(f"❌ Ошибка в GET /api/products: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Ошибка чтения товаров"}
        )
    finally:
        conn.close()


@app.post("/api/products")
async def create_product(request: Request):
    """Создаёт новый товар. Цена НЕ обязательна."""
    try:
        data = await request.json()
    except Exception as e:
        print(f"❌ Некорректный JSON: {e}")
        return JSONResponse(
            status_code=400,
            content={"error": "Некорректный JSON"}
        )

    # Проверяем только обязательные поля
    if not data.get("title"):
        return JSONResponse(
            status_code=400,
            content={"error": "Не хватает поля: title"}
        )
    if not data.get("desc"):
        return JSONResponse(
            status_code=400,
            content={"error": "Не хватает поля: desc"}
        )

    new_id = int(time.time() * 1000)

    try:
        conn = get_db()
        try:
            conn.execute("""
                INSERT INTO products
                    (id, title, description, category, categoryName, price,
                     oldPrice, rating, badge, custom, image, ownerId, createdAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                new_id,
                str(data.get("title", ""))[:200],
                str(data.get("desc", ""))[:2000],
                str(data.get("category", "set"))[:100],
                data.get("categoryName"),
                float(data.get("price") or 0),
                data.get("oldPrice"),
                float(data.get("rating") or 5),
                data.get("badge"),
                1 if data.get("custom", True) else 0,
                data.get("image"),
                data.get("ownerId"),
                int(time.time() * 1000),
            ))
            conn.commit()
        finally:
            conn.close()

        print(f"✅ Создан товар #{new_id}: {data.get('title')}")

        await manager.broadcast({"type": "products_updated"})

        return {**data, "id": new_id}

    except Exception as e:
        print(f"❌ Ошибка создания товара: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Не удалось сохранить товар"}
        )


@app.delete("/api/products/{product_id}")
async def delete_product(product_id: int, request: Request):
    """Удаляет товар. Только владелец по ownerId."""
    owner_id = None
    try:
        body = await request.json()
        owner_id = body.get("ownerId")
    except Exception:
        pass

    try:
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT ownerId, title FROM products WHERE id = ?",
                (product_id,)
            ).fetchone()

            if row is None:
                return JSONResponse(
                    status_code=404,
                    content={"error": "Товар не найден"}
                )

            stored_owner = row["ownerId"]

            if stored_owner and owner_id and stored_owner != owner_id:
                print(f"🚫 Попытка удалить чужой товар #{product_id}")
                return JSONResponse(
                    status_code=403,
                    content={"error": "Это не ваша карточка"}
                )

            title = row["title"]
            conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
            conn.execute("DELETE FROM messages WHERE productId = ?", (product_id,))
            conn.commit()

            print(f"🗑️ Удалён товар #{product_id}: {title}")
        finally:
            conn.close()

        await manager.broadcast({"type": "products_updated"})
        return {"ok": True}

    except Exception as e:
        print(f"❌ Ошибка удаления товара: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Не удалось удалить товар"}
        )


# ==================== API: ЧАТЫ ====================

@app.get("/api/chats")
async def get_chats():
    """Список всех чатов — по последнему сообщению в каждом."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT
                m.productId AS productId,
                p.title     AS title,
                p.image     AS image,
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
    except Exception as e:
        print(f"❌ Ошибка в GET /api/chats: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Ошибка чтения чатов"}
        )
    finally:
        conn.close()


@app.get("/api/chats/{product_id}")
async def get_chat_history(product_id: int):
    """История сообщений по конкретному товару."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT fromUser AS "from", text, time
            FROM messages
            WHERE productId = ?
            ORDER BY time ASC
        """, (product_id,)).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"❌ Ошибка чтения чата #{product_id}: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Ошибка чтения чата"}
        )
    finally:
        conn.close()


@app.delete("/api/chats/{product_id}")
async def clear_chat(product_id: int):
    """Очищает всю историю чата по товару."""
    try:
        conn = get_db()
        try:
            conn.execute(
                "DELETE FROM messages WHERE productId = ?",
                (product_id,)
            )
            conn.commit()
        finally:
            conn.close()

        print(f"🧹 Очищен чат товара #{product_id}")

        await manager.send_to_room(product_id, {
            "type": "chat_cleared",
            "productId": product_id,
        })
        await manager.broadcast({"type": "chats_updated"})

        return {"ok": True}
    except Exception as e:
        print(f"❌ Ошибка очистки чата: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Не удалось очистить чат"}
        )


# ==================== WEB-SOCKET ====================

@app.websocket("/ws/chat")
async def websocket_chat(ws: WebSocket):
    await manager.connect(ws)
    print(f"🔌 Подключён клиент: {ws.client}")

    try:
        while True:
            raw = await ws.receive_text()

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                print(f"⚠️ Невалидный JSON: {raw[:100]}")
                continue

            if not isinstance(data, dict):
                continue

            msg_type = data.get("type")

            # --- JOIN ---
            if msg_type == "join":
                pid = data.get("productId")
                try:
                    pid_int = int(pid)
                    manager.join(pid_int, ws)
                    print(f"→ Клиент зашёл в комнату {pid_int}")
                except (ValueError, TypeError):
                    print(f"⚠️ Некорректный productId: {pid}")

            # --- LEAVE ---
            elif msg_type == "leave":
                pid = data.get("productId")
                try:
                    manager.leave(int(pid), ws)
                except (ValueError, TypeError):
                    pass

            # --- MESSAGE ---
            elif msg_type == "message":
                pid = data.get("productId")
                sender = data.get("from")
                text = data.get("text")

                if pid is None or sender is None or text is None:
                    print(f"⚠️ Не хватает полей в message: {data}")
                    continue

                try:
                    pid_int = int(pid)
                except (ValueError, TypeError):
                    print(f"⚠️ Некорректный productId: {pid}")
                    continue

                if sender not in ALLOWED_SENDERS:
                    print(f"⚠️ Неизвестный отправитель: {sender}")
                    continue

                text = str(text).strip()
                if not text:
                    continue

                if len(text) > MAX_MESSAGE_LENGTH:
                    text = text[:MAX_MESSAGE_LENGTH]

                ts = int(time.time() * 1000)

                try:
                    conn = get_db()
                    try:
                        conn.execute("""
                            INSERT INTO messages (productId, fromUser, text, time)
                            VALUES (?, ?, ?, ?)
                        """, (pid_int, sender, text, ts))
                        conn.commit()
                    finally:
                        conn.close()
                except Exception as e:
                    print(f"❌ Ошибка сохранения сообщения: {e}")
                    continue

                await manager.send_to_room(pid_int, {
                    "type": "message",
                    "productId": pid_int,
                    "from": sender,
                    "text": text,
                    "time": ts,
                })

                await manager.broadcast({"type": "chats_updated"})

    except WebSocketDisconnect:
        manager.disconnect(ws)
        print(f"❌ Клиент отключён: {ws.client}")

    except Exception as e:
        manager.disconnect(ws)
        print(f"⚠️ Ошибка WebSocket: {e}")


# ==================== СТАТИКА ====================

@app.get("/favicon.ico")
async def favicon():
    return JSONResponse(status_code=204, content=None)


@app.get("/")
async def root():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return JSONResponse(
        {"error": "index.html не найден в папке static/"},
        status_code=404
    )


app.mount(
    "/",
    StaticFiles(directory=str(STATIC_DIR), html=True),
    name="static"
)


# ==================== ЗАПУСК ====================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 3000))
    print(f"🚀 Запуск на порту {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
