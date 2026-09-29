@app.post("/api/chats/{product_id}/join")
async def join_chat(product_id: int, request: Request):
    try:
        body = await request.json()
        owner_id = body.get("ownerId")
    except Exception:
        owner_id = None

    if not owner_id:
        return {"allowed": False, "reason": "Не указан ownerId"}

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT ownerId, buyerOwnerId FROM products WHERE id = ?",
            (product_id,)
        ).fetchone()

        if row is None:
            # Диагностика: покажем, что есть в БД
            ids = [r["id"] for r in conn.execute(
                "SELECT id FROM products ORDER BY createdAt DESC LIMIT 5"
            ).fetchall()]
            print(f"⚠️ join: товар #{product_id} не найден. Последние id в БД: {ids}")
            return {
                "allowed": False,
                "reason": f"Товар #{product_id} не найден на сервере"
            }

        seller_id = row["ownerId"]
        buyer_id = row["buyerOwnerId"]

        if seller_id and owner_id == seller_id:
            return {"allowed": True, "role": "seller"}

        if not buyer_id:
            conn.execute(
                "UPDATE products SET buyerOwnerId = ? WHERE id = ?",
                (owner_id, product_id)
            )
            conn.commit()
            return {"allowed": True, "role": "buyer"}

        if buyer_id == owner_id:
            return {"allowed": True, "role": "buyer"}

        return {"allowed": False, "reason": "Этот чат уже ведётся другим покупателем"}
    finally:
        conn.close()