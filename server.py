# -*- coding: utf-8 -*-
"""
Бэкенд для маркетплейса «Сеть обмена 101».
Стек: Python 3.11 + FastAPI + SQLite + WebSocket

Ключевые моменты:
- Каждый чат = (productId, buyerId). У продавца по одному товару
  может быть МНОГО отдельных чатов — по одному на покупателя.
- Роль: 'seller' (владелец товара) | 'buyer' (все остальные).
- Сообщения хранятся с ролью отправителя.
- POST /api/chats/{id}/join — вычисляет роль и (для продавца) buyerId.
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

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"
DB_PATH = DATA_DIR / "marketplace.db"

DATA_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

ALLOWED_ROLES = ("seller", "buyer")
MAX_MESSAGE_LENGTH = 1000


# ==================== БАЗА ====================

def init_db():
    """Создаёт таблицы. Старую схему messages пересоздаёт автоматически."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(messages)").fetchall()]
        # Старая схема (fromUser / нет buyerId) → пересоздаём
        if cols and ("buyerId" not in cols or "role" not in cols):
            print("🔄 Обнаружена старая схема messages — пересоздаём таблицу")
            conn.execute("DROP TABLE IF EXISTS messages")
    except Exception as e:
        print(f"⚠️ Не удалось проверить схему messages: {e}")

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
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            productId     INTEGER NOT NULL,
            buyerId       TEXT NOT NULL,
            role          TEXT NOT NULL,
            senderOwnerId TEXT,
            text          TEXT NOT NULL,
            time          INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_messages_chat
            ON messages(productId, buyerId, time);
    """)
    conn.commit()
    conn.close()
    print(f"📁 База данных готова: {DB_PATH}")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ==================== WEBSOCKET МЕНЕДЖЕР ====================

def room_key(product_id, buyer_id: str) -> str:
    return f"{product_id}:{buyer_id}"


class ChatManager:
    def __init__(self):
        self.rooms = {}       # room_key -> set[WebSocket]
        self.clients = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket):
        self.clients.discard(ws)
        for room in self.rooms.values():
            room.discard(ws)

    def join(self, key: str, ws: WebSocket):
        self.rooms.setdefault(key, set()).add(ws)

    def leave(self, key: str, ws: WebSocket):
        if key in self.rooms:
            self.rooms[key].discard(ws)

    async def send_to_room(self, key: str, data: dict):
        room = self.rooms.get(key)
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


# ==================== ТОВАРЫ ====================

@app.get("/api/products")
async def get_products():
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM products ORDER BY createdAt DESC").fetchall()
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
                "ownerId": r["ownerId"],
            })
        return result
    except Exception as e:
        print(f"❌ GET /api/products: {e}")
        return JSONResponse(status_code=500, content={"error": "Ошибка чтения товаров"})
    finally:
        conn.close()


@app.post("/api/products")
async def create_product(request: Request):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Некорректный JSON"})

    if not data.get("title"):
        return JSONResponse(status_code=400, content={"error": "Не хватает поля: title"})
    if not data.get("desc"):
        return JSONResponse(status_code=400, content={"error": "Не хватает поля: desc"})

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
        return JSONResponse(status_code=500, content={"error": "Не удалось сохранить товар"})


@app.delete("/api/products/{product_id}")
async def delete_product(product_id: int, request: Request):
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
                return JSONResponse(status_code=404, content={"error": "Товар не найден"})

            stored_owner = row["ownerId"]
            if stored_owner and owner_id and stored_owner != owner_id:
                return JSONResponse(status_code=403, content={"error": "Это не ваша карточка"})

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
        print(f"❌ Ошибка удаления: {e}")
        return JSONResponse(status_code=500, content={"error": "Не удалось удалить товар"})


# ==================== ЧАТЫ ====================

@app.post("/api/chats/{product_id}/join")
async def join_chat(product_id: int, request: Request):
    """
    Определяет роль пользователя в чате по товару.
    Тело: { ownerId, buyerId? }
    Ответ: { allowed, role, buyerId }
      - Продавец без buyerId → allowed=true, role='seller', buyerId=null
        (фронт покажет список покупателей)
      - Продавец с buyerId → конкретный чат
      - Покупатель → buyerId = его ownerId, role='buyer'
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    owner_id = body.get("ownerId")
    buyer_id = body.get("buyerId")

    if not owner_id:
        return JSONResponse(status_code=400,
                            content={"allowed": False, "reason": "Нет ownerId"})

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT ownerId FROM products WHERE id = ?",
            (product_id,)
        ).fetchone()

        if row is None:
            return JSONResponse(status_code=404,
                                content={"allowed": False, "reason": "Товар не найден"})

        product_owner = row["ownerId"]

        if product_owner and product_owner == owner_id:
            # Продавец
            return {"allowed": True, "role": "seller", "buyerId": buyer_id or None}
        else:
            # Покупатель — всегда свой buyerId
            return {"allowed": True, "role": "buyer", "buyerId": owner_id}
    finally:
        conn.close()


@app.get("/api/chats")
async def get_chats(ownerId: str = None):
    """
    Список чатов пользователя.
    Покупатель видит свои чаты; продавец — все чаты по своим товарам.
    """
    conn = get_db()
    try:
        if ownerId:
            rows = conn.execute("""
                SELECT
                    m.productId AS productId,
                    m.buyerId   AS buyerId,
                    p.title     AS title,
                    p.image     AS image,
                    p.ownerId   AS productOwnerId,
                    m.text      AS lastText,
                    m.role      AS lastFrom,
                    m.time      AS lastTime
                FROM messages m
                JOIN products p ON p.id = m.productId
                WHERE m.id = (
                    SELECT id FROM messages
                    WHERE productId = m.productId AND buyerId = m.buyerId
                    ORDER BY time DESC LIMIT 1
                )
                AND (p.ownerId = ? OR m.buyerId = ?)
                ORDER BY m.time DESC
            """, (ownerId, ownerId)).fetchall()
        else:
            rows = conn.execute("""
                SELECT
                    m.productId AS productId,
                    m.buyerId   AS buyerId,
                    p.title     AS title,
                    p.image     AS image,
                    p.ownerId   AS productOwnerId,
                    m.text      AS lastText,
                    m.role      AS lastFrom,
                    m.time      AS lastTime
                FROM messages m
                JOIN products p ON p.id = m.productId
                WHERE m.id = (
                    SELECT id FROM messages
                    WHERE productId = m.productId AND buyerId = m.buyerId
                    ORDER BY time DESC LIMIT 1
                )
                ORDER BY m.time DESC
            """).fetchall()

        return [dict(r) for r in rows]
    except Exception as e:
        print(f"❌ GET /api/chats: {e}")
        return JSONResponse(status_code=500, content={"error": "Ошибка чтения чатов"})
    finally:
        conn.close()


@app.get("/api/chats/{product_id}/{buyer_id}")
async def get_chat_history(product_id: int, buyer_id: str):
    """История конкретного чата (товар + покупатель)."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT role, text, time
            FROM messages
            WHERE productId = ? AND buyerId = ?
            ORDER BY time ASC
        """, (product_id, buyer_id)).fetchall()
        return [{"role": r["role"], "text": r["text"], "time": r["time"]} for r in rows]
    except Exception as e:
        print(f"❌ Ошибка чтения чата #{product_id}/{buyer_id}: {e}")
        return JSONResponse(status_code=500, content={"error": "Ошибка чтения чата"})
    finally:
        conn.close()


@app.delete("/api/chats/{product_id}/{buyer_id}")
async def clear_chat(product_id: int, buyer_id: str):
    """Очистка конкретного чата."""
    try:
        conn = get_db()
        try:
            conn.execute(
                "DELETE FROM messages WHERE productId = ? AND buyerId = ?",
                (product_id, buyer_id)
            )
            conn.commit()
        finally:
            conn.close()

        print(f"🧹 Очищен чат {product_id}/{buyer_id}")

        await manager.send_to_room(room_key(product_id, buyer_id), {
            "type": "chat_cleared",
            "productId": product_id,
            "buyerId": buyer_id,
        })
        await manager.broadcast({"type": "chats_updated"})
        return {"ok": True}
    except Exception as e:
        print(f"❌ Ошибка очистки чата: {e}")
        return JSONResponse(status_code=500, content={"error": "Не удалось очистить чат"})


# ==================== WEBSOCKET ====================

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
                bid = data.get("buyerId")
                if pid is None or not bid:
                    print(f"⚠️ join: не хватает productId/buyerId: {data}")
                    continue
                try:
                    pid_int = int(pid)
                except (ValueError, TypeError):
                    continue
                key = room_key(pid_int, str(bid))
                manager.join(key, ws)
                print(f"→ join room {key}")

            # --- LEAVE ---
            elif msg_type == "leave":
                pid = data.get("productId")
                bid = data.get("buyerId")
                if pid is None or not bid:
                    continue
                try:
                    pid_int = int(pid)
                except (ValueError, TypeError):
                    continue
                manager.leave(room_key(pid_int, str(bid)), ws)

            # --- MESSAGE ---
            elif msg_type == "message":
                pid = data.get("productId")
                bid = data.get("buyerId")
                role = data.get("role")
                text = data.get("text")
                sender_owner = data.get("ownerId")

                if pid is None or not bid or role is None or text is None:
                    print(f"⚠️ message: не хватает полей: {data}")
                    continue

                try:
                    pid_int = int(pid)
                except (ValueError, TypeError):
                    continue

                if role not in ALLOWED_ROLES:
                    print(f"⚠️ Неизвестная роль: {role}")
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
                            INSERT INTO messages
                                (productId, buyerId, role, senderOwnerId, text, time)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (pid_int, str(bid), role, sender_owner, text, ts))
                        conn.commit()
                    finally:
                        conn.close()
                except Exception as e:
                    print(f"❌ Ошибка сохранения сообщения: {e}")
                    continue

                await manager.send_to_room(room_key(pid_int, str(bid)), {
                    "type": "message",
                    "productId": pid_int,
                    "buyerId": str(bid),
                    "role": role,
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
    return JSONResponse({"error": "index.html не найден в static/"}, status_code=404)


app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 3000))
    print(f"🚀 Запуск на порту {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
