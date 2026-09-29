# -*- coding: utf-8 -*-
"""
Бэкенд «Сеть обмена 101». FastAPI + SQLite + WebSocket.

Чат = (productId, buyerId). У продавца по одному товару
может быть МНОГО чатов — по одному на покупателя.
"""
import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"
DB_PATH = DATA_DIR / "marketplace.db"

DATA_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

ALLOWED_ROLES = ("seller", "buyer")
MAX_MESSAGE_LENGTH = 1000

MESSAGES_REQUIRED_COLS = {
    "id", "productId", "buyerId", "role", "senderOwnerId", "text", "time",
}


def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(messages)").fetchall()]
        except Exception:
            cols = []

        if cols and not MESSAGES_REQUIRED_COLS.issubset(set(cols)):
            print(f"🔄 Старая схема messages ({sorted(cols)}) — пересоздаём")
            conn.execute("DROP TABLE IF EXISTS messages")
            conn.commit()

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
    finally:
        conn.close()
    print(f"📁 БД готова: {DB_PATH}")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def room_key(product_id, buyer_id: str) -> str:
    return f"{product_id}:{buyer_id}"


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


async def save_and_broadcast_message(product_id, buyer_id, role, text, sender_owner):
    if role not in ALLOWED_ROLES:
        print(f"⚠️ Отклонено: неизвестная роль '{role}'")
        return None

    text = str(text or "").strip()
    if not text:
        return None
    if len(text) > MAX_MESSAGE_LENGTH:
        text = text[:MAX_MESSAGE_LENGTH]

    ts = int(time.time() * 1000)
    try:
        conn = get_db()
        try:
            cur = conn.execute("""
                INSERT INTO messages
                    (productId, buyerId, role, senderOwnerId, text, time)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (product_id, str(buyer_id), role, sender_owner, text, ts))
            conn.commit()
            msg_id = cur.lastrowid
        finally:
            conn.close()
    except Exception as e:
        print(f"❌ Ошибка INSERT в messages: {e}")
        import traceback
        traceback.print_exc()
        return None

    payload = {
        "type": "message",
        "id": msg_id,
        "productId": product_id,
        "buyerId": str(buyer_id),
        "role": role,
        "text": text,
        "time": ts,
    }
    await manager.send_to_room(room_key(product_id, str(buyer_id)), payload)
    await manager.broadcast({"type": "chats_updated"})
    return payload


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    print()
    print("=" * 55)
    print("✅ Сервер запущен: http://localhost:3000")
    print("   Тест в двух вкладках:")
    print("   • http://localhost:3000/?as=seller")
    print("   • http://localhost:3000/?as=buyer1")
    print("=" * 55)
    print()
    yield
    print("👋 Остановлен")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/ping")
async def ping():
    return {"ok": True, "time": int(time.time() * 1000)}


@app.get("/api/products")
async def get_products():
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM products ORDER BY createdAt DESC").fetchall()
        return [{
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
        } for r in rows]
    except Exception as e:
        print(f"❌ GET /api/products: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        conn.close()


@app.post("/api/products")
async def create_product(request: Request):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Некорректный JSON"})

    if not data.get("title"):
        return JSONResponse(status_code=400, content={"error": "Нет поля: title"})
    if not data.get("desc"):
        return JSONResponse(status_code=400, content={"error": "Нет поля: desc"})

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
        await manager.broadcast({"type": "products_updated"})
        return {**data, "id": new_id}
    except Exception as e:
        print(f"❌ create product: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


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
                "SELECT ownerId FROM products WHERE id = ?", (product_id,)
            ).fetchone()
            if row is None:
                return JSONResponse(status_code=404, content={"error": "Не найдено"})
            stored = row["ownerId"]
            if stored and owner_id and stored != owner_id:
                return JSONResponse(status_code=403, content={"error": "Не ваша карточка"})
            conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
            conn.execute("DELETE FROM messages WHERE productId = ?", (product_id,))
            conn.commit()
        finally:
            conn.close()
        await manager.broadcast({"type": "products_updated"})
        return {"ok": True}
    except Exception as e:
        print(f"❌ delete: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/chats/{product_id}/join")
async def join_chat(product_id: int, request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    owner_id = body.get("ownerId")
    buyer_id = body.get("buyerId")

    if not owner_id:
        return {"allowed": False, "reason": "Нет ownerId"}

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT ownerId FROM products WHERE id = ?", (product_id,)
        ).fetchone()
        if row is None:
            return {"allowed": False, "reason": "Товар не найден"}
        product_owner = row["ownerId"]
        if product_owner and product_owner == owner_id:
            return {"allowed": True, "role": "seller", "buyerId": buyer_id or None}
        return {"allowed": True, "role": "buyer", "buyerId": owner_id}
    finally:
        conn.close()


@app.get("/api/chats")
async def get_chats(ownerId: str = None):
    conn = get_db()
    try:
        base_sql = """
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
        """
        if ownerId:
            rows = conn.execute(base_sql + """
                AND (p.ownerId = ? OR m.buyerId = ?)
                ORDER BY m.time DESC
            """, (ownerId, ownerId)).fetchall()
        else:
            rows = conn.execute(base_sql + " ORDER BY m.time DESC").fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"❌ GET /api/chats: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        conn.close()


@app.get("/api/chats/{product_id}/{buyer_id}")
async def get_chat_history(product_id: int, buyer_id: str):
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT id, role, text, time
            FROM messages
            WHERE productId = ? AND buyerId = ?
            ORDER BY time ASC
        """, (product_id, buyer_id)).fetchall()
        return [{"id": r["id"], "role": r["role"], "text": r["text"], "time": r["time"]} for r in rows]
    except Exception as e:
        print(f"❌ GET history: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        conn.close()


@app.post("/api/chats/{product_id}/{buyer_id}/messages")
async def post_message(product_id: int, buyer_id: str, request: Request):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Некорректный JSON"})

    payload = await save_and_broadcast_message(
        product_id=product_id,
        buyer_id=buyer_id,
        role=data.get("role"),
        text=data.get("text"),
        sender_owner=data.get("ownerId"),
    )
    if payload is None:
        return JSONResponse(status_code=400, content={
            "error": "Не удалось сохранить сообщение (проверь role/text)"
        })
    return payload


@app.delete("/api/chats/{product_id}/{buyer_id}")
async def clear_chat(product_id: int, buyer_id: str):
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

        await manager.send_to_room(room_key(product_id, buyer_id), {
            "type": "chat_cleared",
            "productId": product_id,
            "buyerId": buyer_id,
        })
        await manager.broadcast({"type": "chats_updated"})
        return {"ok": True}
    except Exception as e:
        print(f"❌ clear chat: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.websocket("/ws/chat")
async def websocket_chat(ws: WebSocket):
    await manager.connect(ws)
    print(f"🔌 WS connect: {ws.client}")

    try:
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(data, dict):
                continue

            t = data.get("type")

            if t == "join":
                pid = data.get("productId")
                bid = data.get("buyerId")
                if pid is None or not bid:
                    continue
                try:
                    pid_int = int(pid)
                except (ValueError, TypeError):
                    continue
                key = room_key(pid_int, str(bid))
                manager.join(key, ws)
                print(f"→ join {key}")

            elif t == "leave":
                pid = data.get("productId")
                bid = data.get("buyerId")
                if pid is None or not bid:
                    continue
                try:
                    pid_int = int(pid)
                except (ValueError, TypeError):
                    continue
                manager.leave(room_key(pid_int, str(bid)), ws)

            elif t == "message":
                pid = data.get("productId")
                bid = data.get("buyerId")
                if pid is None or not bid:
                    continue
                try:
                    pid_int = int(pid)
                except (ValueError, TypeError):
                    continue
                await save_and_broadcast_message(
                    product_id=pid_int,
                    buyer_id=str(bid),
                    role=data.get("role"),
                    text=data.get("text"),
                    sender_owner=data.get("ownerId"),
                )

    except WebSocketDisconnect:
        manager.disconnect(ws)
        print(f"❌ WS disconnect: {ws.client}")
    except Exception as e:
        manager.disconnect(ws)
        print(f"⚠️ WS error: {e}")


@app.get("/favicon.ico")
async def favicon():
    return JSONResponse(status_code=204, content=None)


@app.get("/")
async def root():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    alt = BASE_DIR / "index.html"
    if alt.exists():
        return FileResponse(alt)
    return JSONResponse(
        {"error": "index.html не найден. Положи его в static/index.html"},
        status_code=404
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 3000))
    print(f"🚀 Порт {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
