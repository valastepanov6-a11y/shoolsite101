import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
import { SMTPClient } from 'https://deno.land/x/denomailer@1.6.0/mod.ts';

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
    const { email, password } = await req.json();

    if (typeof email !== 'string' || !/^[\w.+-]+@gmail\.com$/i.test(email.trim()))
      return json({ error: 'Нужен адрес @gmail.com' }, 400);
    if (typeof password !== 'string' || password.length < 6)
      return json({ error: 'Пароль минимум 6 символов' }, 400);

    const emailLc = email.trim().toLowerCase();
    const admin = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!,
    );

    // Rate limit
    const { data: recent } = await admin
      .from('email_verifications')
      .select('created_at')
      .eq('email', emailLc)
      .gte('created_at', new Date(Date.now() - 60_000).toISOString())
      .maybeSingle();
    if (recent) return json({ error: 'Подождите минуту перед повторной отправкой' }, 429);

    // Создаём / находим auth-юзера
    let userId: string | undefined;
    const { data: created, error: cErr } = await admin.auth.admin.createUser({
      email: emailLc,
      password,
      email_confirm: false,
    });

    if (cErr) {
      const { data: list } = await admin.auth.admin.listUsers();
      const found = list?.users?.find((u: any) => u.email?.toLowerCase() === emailLc);
      if (!found) return json({ error: cErr.message }, 400);
      userId = found.id;
      await admin.auth.admin.updateUserById(userId, { password });
    } else {
      userId = created!.user!.id;
    }

    // Если профиль уже есть — регистрация завершена
    const { data: prof } = await admin
      .from('profiles').select('id').eq('id', userId).maybeSingle();
    if (prof) return json({ error: 'Этот email уже зарегистрирован' }, 409);

    // Генерация кода
    const code = String(Math.floor(1000 + Math.random() * 9000));
    const codeHash = await sha256(code);

    await admin.from('email_verifications').upsert({
      email: emailLc,
      code_hash: codeHash,
      user_id: userId,
      attempts: 0,
      expires_at: new Date(Date.now() + 10 * 60_000).toISOString(),
      created_at: new Date().toISOString(),
    }, { onConflict: 'email' });

    // Отправка через Gmail SMTP
    const gmailUser = Deno.env.get('GMAIL_USER')!;
    const gmailPass = Deno.env.get('GMAIL_APP_PASSWORD')!;

    const client = new SMTPClient({
      connection: {
        hostname: 'smtp.gmail.com',
        port: 465,
        tls: true,
        auth: { username: gmailUser, password: gmailPass },
      },
    });

    await client.send({
      from: `Обмен 101 <${gmailUser}>`,
      to: emailLc,
      subject: 'Код подтверждения — Обмен 101',
      content: 'auto',
      html: `
        <div style="font-family:-apple-system,'Segoe UI',sans-serif;max-width:480px;
                    margin:0 auto;padding:32px;background:#0e0518;color:#f5f3ff;
                    border-radius:16px;">
          <h2 style="color:#c4b5fd;margin:0 0 8px;">Обмен 101</h2>
          <p style="color:#c4b5fd;font-size:13px;margin:0 0 24px;">Подтверждение регистрации</p>
          <div style="background:linear-gradient(135deg,#a855f7,#6d28d9);padding:24px;
                      border-radius:12px;text-align:center;">
            <div style="font-size:40px;font-weight:800;letter-spacing:12px;color:#fff;">
              ${code}
            </div>
          </div>
          <p style="color:#8b7ba8;font-size:12px;margin:24px 0 0;">
            Код действует 10 минут. Если вы не регистрировались — проигнорируйте письмо.
          </p>
        </div>`,
    });

    await client.close();

    return json({ ok: true });
  } catch (e) {
    console.error(e);
    return json({ error: (e as Error).message || 'Ошибка сервера' }, 500);
  }
});
