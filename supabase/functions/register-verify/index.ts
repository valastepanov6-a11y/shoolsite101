import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const cors = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};
const json = (b: unknown, s = 200) =>
  new Response(JSON.stringify(b), { status: s, headers: { ...cors, 'Content-Type': 'application/json' } });

async function sha256(str: string): Promise<string> {
  const buf = new TextEncoder().encode(str);
  const h = await crypto.subtle.digest('SHA-256', buf);
  return Array.from(new Uint8Array(h)).map(x => x.toString(16).padStart(2, '0')).join('');
}

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: cors });

  try {
    const { email, code, username } = await req.json();

    if (!email || !code || !username)      return json({ error: 'Не все поля заполнены' }, 400);
    if (!/^\d{4}$/.test(String(code)))     return json({ error: 'Код — ровно 4 цифры' }, 400);
    if (!/^[a-zA-Z0-9_]{3,20}$/.test(username))
      return json({ error: 'Ник: 3–20 символов, латиница, цифры, _' }, 400);

    const emailLc = email.trim().toLowerCase();
    const admin = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!,
    );

    const { data: ver } = await admin
      .from('email_verifications').select('*').eq('email', emailLc).maybeSingle();

    if (!ver)                                 return json({ error: 'Код не найден. Запросите новый.' }, 400);
    if (new Date(ver.expires_at) < new Date()) return json({ error: 'Код истёк. Запросите новый.' }, 400);
    if (ver.attempts >= 5)                     return json({ error: 'Слишком много попыток. Запросите новый код.' }, 429);

    const codeHash = await sha256(String(code));
    if (codeHash !== ver.code_hash) {
      await admin.from('email_verifications')
        .update({ attempts: ver.attempts + 1 }).eq('id', ver.id);
      return json({ error: 'Неверный код' }, 400);
    }

    // Проверка свободности ника (регистронезависимо)
    const { data: nickTaken } = await admin
      .from('profiles').select('id').ilike('username', username).maybeSingle();
    if (nickTaken) return json({ error: 'Этот ник уже занят' }, 409);

    // Подтверждаем email
    await admin.auth.admin.updateUserById(ver.user_id, { email_confirm: true });

    // Создаём профиль
    const { error: pErr } = await admin.from('profiles').insert([{
      id: ver.user_id,
      username,
    }]);
    if (pErr) return json({ error: pErr.message }, 500);

    // Удаляем верификацию
    await admin.from('email_verifications').delete().eq('id', ver.id);

    return json({ ok: true });
  } catch (e) {
    console.error(e);
    return json({ error: (e as Error).message || 'Ошибка сервера' }, 500);
  }
});
