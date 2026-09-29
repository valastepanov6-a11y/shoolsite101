# -*- coding: utf-8 -*-
"""
Бэкенд для маркетплейса «Сеть обмена 101».
FastAPI + SQLite + WebSocket.

Правила чата:
- В чат товара могут зайти только 2 человека:
  продавец (создатель карточки, ownerId) и покупатель
  (первый, кто открыл чат — buyerOwnerId).
- В БД хранится реальная роль отправителя: 'seller' или 'buyer'.
- Фронт сам решает, где показывать сообщение —
  справа (своё) или слева (чужое).
"""

import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

# ==================== ПУТИ ====================

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"
DB_PATH = DATA_DIR / "marketplace.db"

DATA_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

ALLOWED_ROLES = ("seller", "buyer")
MAX_MESSAGE_LENGTH = 1000


# ==================== БАЗА ДАННЫХ ====================

def init_db():
    """Создаёт таблицы, если их нет."""
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS products (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            title         TEXT NOT NULL,
            description   TEXT NOT NULL,
            category      TEXT NOT NULL,
            categoryName  TEXT,
            price         REAL DEFAULT 0,
            oldPrice      REAL,
            rating        REAL DEFAULT 5,
            badge         TEXT,
            custom        INTEGER DEFAULT 1,
            image         TEXT,
            ownerId       TEXT,
            buyerOwnerId  TEXT,
            createdAt     INTEGER
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

        CREATE INDEX IF NOT EXISTS idx_products_owner
            ON products(ownerId);
    """)
    conn.commit()

    # Миграция: добавить buyerOwnerId, если её нет
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()]
        if "buyerOwnerId" not in cols:
            conn.execute("ALTER TABLE products ADD COLUMN buyerOwnerId TEXT")
            conn.commit()
            print("🛠 Миграция: добавлена колонка buyerOwnerId")
    except Exception as e:
        print(f"⚠️ Ошибка миграции: {e}")

    conn.close()
    print(f"📁 База данных готова: {DB_PATH}")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def product_exists(product_id: int) -> bool:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT 1 FROM products WHERE id = ?", (product_id,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


# ==================== WEB-SOCKET МЕНЕДЖЕР ====================

class ChatManager:
    def __init__(self):
        self.rooms = {}
        self.clients = set()

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
            except Exception:
                dead.append(ws)
        for ws in dead:
            room.discard(ws)

    async def broadcast(self, data: dict):
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
    print("=" * 55)
    print("✅ Сервер запущен")
    print("🌐 Открой: http://localhost:3000")
    print("=" * 55)
    print()
    yield
    print("👋 Сервер остановлен")


app = FastAPI(lifespan=lifespan)

# CORS — на случай если фронт открыт с другого origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


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
                "id": int(r["id"]),
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
        print(f"❌ Ошибка в GET /api/products: {e}")
        return JSONResponse(status_code=500, content={"error": "Ошибка чтения"})
    finally:
        conn.close()


@app.post("/api/products")
async def create_product(request: Request):
    try:
        data = await request.json()
    except Exception as e:
        print(f"❌ POST /api/products: некорректный JSON: {e}")
        return JSONResponse(status_code=400, content={"error": "Некорректный JSON"})

    title = (data.get("title") or "").strip()
    desc = (data.get("desc") or "").strip()
    category = (data.get("category") or "set").strip()

    if not title:
        return JSONResponse(status_code=400, content={"error": "Не хватает поля: title"})
    if not desc:
        return JSONResponse(status_code=400, content={"error": "Не хватает поля: desc"})

    try:
        conn = get_db()
        try:
            cur = conn.execute("""
                INSERT INTO products
                    (title, description, category, categoryName, price,
                     oldPrice, rating, badge, custom, image, ownerId, createdAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                title[:200],
                desc[:2000],
                category[:100],
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
            new_id = int(cur.lastrowid)
        finally:
            conn.close()

        print(f"✅ Создан товар #{new_id}: {title!r}  (ownerId={data.get('ownerId')!r})")
        await manager.broadcast({"type": "products_updated"})

        # Возвращаем товар целиком — с гарантированно валидным id
        return {
            "id": new_id,
            "title": title,
            "desc": desc,
            "category": category,
            "categoryName": data.get("categoryName"),
            "price": float(data.get("price") or 0),
            "oldPrice": data.get("oldPrice"),
            "rating": float(data.get("rating") or 5),
            "badge": data.get("badge"),
            "custom": bool(data.get("custom", True)),
            "image": data.get("image"),
            "icon": None,
            "ownerId": data.get("ownerId"),
        }

    except Exception as e:
        print(f"❌ Ошибка создания: {e}")
        return JSONResponse(status_code=500, content={"error": "Не удалось сохранить"})


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
                return JSONResponse(status_code=404, content={"error": "Не найдено"})

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
        print(f"❌ Ошибка удаления: {e}")
        return JSONResponse(status_code=500, content={"error": "Не удалось удалить"})


# ==================== API: ЧАТЫ ====================

@app.post("/api/chats/{product_id}/join")
async def join_chat(product_id: int, request: Request):
    """
    Проверяет, может ли пользователь зайти в чат по товару.
    Роли:
      - seller — владелец карточки (ownerId)
      - buyer — первый, кто открыл чат
    Никто третий зайти не может.
    """
    try:
        body = await request.json()
        owner_id = body.get("ownerId")
    except Exception:
        owner_id = None

    if not owner_id:
        return {"allowed": False, "reason": "Не указан ownerId"}

    try:
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT ownerId, buyerOwnerId FROM products WHERE id = ?",
                (product_id,)
            ).fetchone()

            if row is None:
                # Диагностика: покажем, какие id реально есть в БД
                ids = [r["id"] for r in conn.execute(
                    "SELECT id FROM products ORDER BY createdAt DESC LIMIT 10"
                ).fetchall()]
                print(f"⚠️ join: товар #{product_id} не найден. "
                      f"Последние id в БД: {ids}")
                return {
                    "allowed": False,
                    "reason": f"Товар #{product_id} не найден на сервере",
                    "debug_known_ids": ids,
                }

            seller_id = row["ownerId"]
            buyer_id = row["buyerOwnerId"]

            # Продавец — всегда пускаем
            if seller_id and owner_id == seller_id:
                return {"allowed": True, "role": "seller"}

            # Ещё нет покупателя — регистрируем
            if not buyer_id:
                conn.execute(
                    "UPDATE products SET buyerOwnerId = ? WHERE id = ?",
                    (owner_id, product_id)
                )
                conn.commit()
                print(f"👤 Новый покупатель для товара #{product_id}: {owner_id}")
                return {"allowed": True, "role": "buyer"}

            # Этот же покупатель — пускаем
            if buyer_id == owner_id:
                return {"allowed": True, "role": "buyer"}

            # Другой человек — отказ
            return {
                "allowed": False,
                "reason": "Этот чат уже ведётся другим покупателем"
            }
        finally:
            conn.close()
    except Exception as e:
        print(f"❌ Ошибка join_chat: {e}")
        return {"allowed": False, "reason": "Ошибка сервера"}


@app.get("/api/chats")
async def get_chats():
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
                ORDER BY time DESC, id DESC LIMIT 1
            )
            ORDER BY m.time DESC
        """).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"❌ Ошибка в GET /api/chats: {e}")
        return JSONResponse(status_code=500, content={"error": "Ошибка чтения"})
    finally:
        conn.close()


@app.get("/api/chats/{product_id}")
async def get_chat_history(product_id: int):
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT fromUser AS "from", text, time
            FROM messages
            WHERE productId = ?
            ORDER BY time ASC, id ASC
        """, (product_id,)).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"❌ Ошибка чтения чата #{product_id}: {e}")
        return JSONResponse(status_code=500, content={"error": "Ошибка чтения"})
    finally:
        conn.close()


@app.delete("/api/chats/{product_id}")
async def clear_chat(product_id: int):
    try:
        conn = get_db()
        try:
            conn.execute("DELETE FROM messages WHERE productId = ?", (product_id,))
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
        return JSONResponse(status_code=500, content={"error": "Не удалось очистить"})


# ==================== DEBUG ====================

@app.get("/api/debug/products")
async def debug_products():
    """Быстрая диагностика: что реально лежит в БД."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT id, title, ownerId, buyerOwnerId, createdAt
            FROM products ORDER BY createdAt DESC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/health")
async def health():
    """Простой health-check: если открывается — сервер жив."""
    return {"status": "ok", "time": int(time.time() * 1000)}


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
                continue

            if not isinstance(data, dict):
                continue

            msg_type = data.get("type")

            # --- JOIN ---
            if msg_type == "join":
                pid = data.get("productId")
                try:
                    pid_int = int(pid)
                except (ValueError, TypeError):
                    continue

                if not product_exists(pid_int):
                    print(f"⚠️ WS join: товар #{pid_int} не существует")
                    continue

                manager.join(pid_int, ws)
                print(f"→ Клиент зашёл в комнату {pid_int}")

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
                role = data.get("role")
                text = data.get("text")

                if pid is None or role is None or text is None:
                    continue

                try:
                    pid_int = int(pid)
                except (ValueError, TypeError):
                    continue

                if not product_exists(pid_int):
                    print(f"⚠️ WS message: товар #{pid_int} не существует")
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
                            INSERT INTO messages (productId, fromUser, text, time)
                            VALUES (?, ?, ?, ?)
                        """, (pid_int, role, text, ts))
                        conn.commit()
                    finally:
                        conn.close()
                except Exception as e:
                    print(f"❌ Ошибка сохранения: {e}")
                    continue

                await manager.send_to_room(pid_int, {
                    "type": "message",
                    "productId": pid_int,
                    "from": role,
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
        {"error": "index.html не найден. Положите файл в static/index.html"},
        status_code=404
    )


app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


# ==================== ЗАПУСК ====================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 3000))
    print(f"🚀 Запуск на порту {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
