/**
 * Echo Affiliate v1.0.0
 * AI-powered affiliate & referral program management
 * Cloudflare Worker — D1 + KV + Service Bindings
 */

interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  ENGINE_RUNTIME: Fetcher;
  SHARED_BRAIN: Fetcher;
  EMAIL_SENDER: Fetcher;
  ECHO_API_KEY: string;
  ENVIRONMENT: string;
}

interface RLState { c: number; t: number; }

function sanitize(s: string | null | undefined, max = 2000): string {
  if (!s) return '';
  return s.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '').trim().slice(0, max);
}

function uid(): string {
  return crypto.randomUUID().replace(/-/g, '').slice(0, 16);
}

function slug6(): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(4)))
    .map(b => b.toString(36).padStart(2, '0')).join('').slice(0, 8);
}

function json(data: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'DENY', 'X-XSS-Protection': '1; mode=block', 'Referrer-Policy': 'strict-origin-when-cross-origin', 'Permissions-Policy': 'camera=(), microphone=(), geolocation=()', 'Strict-Transport-Security': 'max-age=31536000; includeSubDomains', ...headers },
  });
}

function slog(level: 'info' | 'warn' | 'error', msg: string, data?: Record<string, unknown>) {
  const entry = { ts: new Date().toISOString(), level, worker: 'echo-affiliate', version: '1.0.0', msg, ...data };
  if (level === 'error') console.error(JSON.stringify(entry));
  else console.log(JSON.stringify(entry));
}

function cors(): Response {
  return new Response(null, {
    status: 204,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'X-Content-Type-Options': 'nosniff',
      'X-Frame-Options': 'DENY',
      'X-XSS-Protection': '1; mode=block',
      'Referrer-Policy': 'strict-origin-when-cross-origin',
      'Permissions-Policy': 'camera=(), microphone=(), geolocation=()',
      'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
      'Access-Control-Allow-Methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type,X-Echo-API-Key,X-Tenant-ID,Authorization',
      'Access-Control-Max-Age': '86400',
    },
  });
}

function authOk(req: Request, env: Env): boolean {
  const apiKey = req.headers.get('X-Echo-API-Key') || '';
  const bearer = (req.headers.get('Authorization') || '').replace(/^Bearer\s+/i, '');
  const expected = env.ECHO_API_KEY;
  if (!expected) return false;
  return apiKey === expected || bearer === expected;
}

function tid(req: Request): string {
  return req.headers.get('X-Tenant-ID') || new URL(req.url).searchParams.get('tenant_id') || 'default';
}

async function rateLimit(env: Env, key: string, max: number, windowSec: number): Promise<boolean> {
  const raw = await env.CACHE.get<RLState>(`rl:${key}`, 'json');
  const now = Math.floor(Date.now() / 1000);
  if (!raw || (now - raw.t) > windowSec) {
    await env.CACHE.put(`rl:${key}`, JSON.stringify({ c: 1, t: now }), { expirationTtl: windowSec * 2 });
    return true;
  }
  const elapsed = now - raw.t;
  const decay = (elapsed / windowSec) * max;
  const current = Math.max(0, raw.c - decay) + 1;
  await env.CACHE.put(`rl:${key}`, JSON.stringify({ c: current, t: now }), { expirationTtl: windowSec * 2 });
  return current <= max;
}

function ipHash(ip: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < ip.length; i++) { h ^= ip.charCodeAt(i); h = Math.imul(h, 0x01000193); }
  return (h >>> 0).toString(36);
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    if (req.method === 'OPTIONS') return cors();

    const url = new URL(req.url);
    const p = url.pathname;
    const m = req.method;
    const ip = req.headers.get('CF-Connecting-IP') || '0.0.0.0';

    // ── Public: Root + Health ──
    if (p === '/') return json({ service: 'echo-affiliate', version: '1.0.0', status: 'operational' });
    if (p === '/health') {
      const r = await env.DB.prepare('SELECT COUNT(*) as c FROM programs').first<{ c: number }>();
      return json({ status: 'healthy', service: 'echo-affiliate', version: '1.0.0', programs: r?.c || 0 });
    }

    try {
    // ── Public: Click Tracking (GET /go/:slug) ──
    if (m === 'GET' && p.startsWith('/go/')) {
      const linkSlug = p.split('/')[2];
      if (!linkSlug) return json({ error: 'Missing slug' }, 400);

      const link = await env.DB.prepare('SELECT * FROM links WHERE slug = ? AND is_active = 1').bind(linkSlug).first();
      if (!link) return json({ error: 'Link not found' }, 404);

      // Rate limit: 60 clicks/min per IP
      if (!(await rateLimit(env, `click:${ip}`, 60, 60))) {
        return Response.redirect(link.destination_url as string, 302);
      }

      const iph = ipHash(ip);
      const ua = sanitize(req.headers.get('User-Agent'), 500);
      const ref = sanitize(req.headers.get('Referer'), 500);
      const country = req.headers.get('CF-IPCountry') || '';
      const device = /mobile/i.test(ua) ? 'mobile' : /tablet/i.test(ua) ? 'tablet' : 'desktop';

      // Check uniqueness (24h window)
      const existing = await env.DB.prepare(
        'SELECT id FROM clicks WHERE link_id = ? AND visitor_ip_hash = ? AND created_at > datetime("now", "-1 day")'
      ).bind(link.id, iph).first();
      const isUnique = existing ? 0 : 1;

      // Fire and forget: record click + update counters
      (async () => {
        try {
          await env.DB.batch([
            env.DB.prepare('INSERT INTO clicks (link_id, affiliate_id, program_id, visitor_ip_hash, visitor_ua, referrer, country, device, is_unique) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
              .bind(link.id, link.affiliate_id, link.program_id, iph, ua, ref, country, device, isUnique),
            env.DB.prepare('UPDATE links SET total_clicks = total_clicks + 1, unique_clicks = unique_clicks + ? WHERE id = ?')
              .bind(isUnique, link.id),
            env.DB.prepare('UPDATE affiliates SET total_clicks = total_clicks + 1, last_active_at = datetime("now") WHERE id = ?')
              .bind(link.affiliate_id),
          ]);
        } catch (_) { /* non-blocking */ }
      })();

      // Set affiliate cookie and redirect
      const dest = new URL(link.destination_url as string);
      if (link.utm_source) dest.searchParams.set('utm_source', link.utm_source as string);
      if (link.utm_medium) dest.searchParams.set('utm_medium', link.utm_medium as string);
      if (link.utm_campaign) dest.searchParams.set('utm_campaign', link.utm_campaign as string);

      const program = await env.DB.prepare('SELECT cookie_days FROM programs WHERE id = ?').bind(link.program_id).first();
      const cookieDays = (program?.cookie_days as number) || 30;

      return new Response(null, {
        status: 302,
        headers: {
          'Location': dest.toString(),
          'Set-Cookie': `echo_ref=${link.affiliate_id};Path=/;Max-Age=${cookieDays * 86400};SameSite=Lax`,
          'Access-Control-Allow-Origin': '*',
      'X-Content-Type-Options': 'nosniff',
      'X-Frame-Options': 'DENY',
      'X-XSS-Protection': '1; mode=block',
      'Referrer-Policy': 'strict-origin-when-cross-origin',
      'Permissions-Policy': 'camera=(), microphone=(), geolocation=()',
      'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
        },
      });
    }

    // ── Public: Affiliate Signup Page (GET /join/:slug) ──
    if (m === 'GET' && p.startsWith('/join/')) {
      const programSlug = p.split('/')[2];
      if (!programSlug) return json({ error: 'Missing program slug' }, 400);

      const program = await env.DB.prepare('SELECT * FROM programs WHERE slug = ? AND is_active = 1').bind(programSlug).first();
      if (!program) return json({ error: 'Program not found' }, 404);

      const commDisplay = program.commission_type === 'percentage' ? `${program.commission_value}%` : `$${program.commission_value}`;

      return new Response(`<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Join ${sanitize(program.name as string, 100)} Affiliate Program</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:system-ui,-apple-system,sans-serif;background:#0a0f1a;color:#e2e8f0;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px}
.card{background:#0c1220;border:1px solid #1e293b;border-radius:16px;padding:40px;max-width:480px;width:100%}
h1{font-size:24px;font-weight:800;margin-bottom:8px;color:#fff}
.sub{color:#94a3b8;margin-bottom:24px;font-size:14px}
.stats{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:24px}
.stat{background:#0f172a;border-radius:12px;padding:16px;text-align:center}
.stat-val{font-size:24px;font-weight:800;color:#14b8a6}
.stat-label{font-size:12px;color:#64748b;margin-top:4px}
label{display:block;font-size:13px;color:#94a3b8;margin-bottom:4px;margin-top:12px}
input{width:100%;padding:12px;background:#0f172a;border:1px solid #1e293b;border-radius:8px;color:#e2e8f0;font-size:14px;outline:none}
input:focus{border-color:#14b8a6}
textarea{width:100%;padding:12px;background:#0f172a;border:1px solid #1e293b;border-radius:8px;color:#e2e8f0;font-size:14px;outline:none;resize:vertical;min-height:60px}
.btn{width:100%;padding:14px;background:#14b8a6;color:#fff;border:none;border-radius:12px;font-size:16px;font-weight:700;cursor:pointer;margin-top:20px}
.btn:hover{background:#0d9488}
.msg{text-align:center;padding:16px;border-radius:8px;margin-top:16px;font-size:14px}
.ok{background:#064e3b;color:#6ee7b7}.err{background:#450a0a;color:#fca5a5}
</style></head><body>
<div class="card">
<h1>${sanitize(program.name as string, 100)}</h1>
<p class="sub">${sanitize(program.description as string || 'Join our affiliate program and earn commissions on every referral.', 300)}</p>
<div class="stats">
<div class="stat"><div class="stat-val">${commDisplay}</div><div class="stat-label">Commission</div></div>
<div class="stat"><div class="stat-val">${program.cookie_days}d</div><div class="stat-label">Cookie Window</div></div>
</div>
<form id="af">
<label>Full Name *</label><input name="name" required maxlength="200">
<label>Email *</label><input name="email" type="email" required maxlength="200">
<label>Company</label><input name="company" maxlength="200">
<label>Website</label><input name="website" type="url" maxlength="500">
<label>How will you promote us?</label><textarea name="notes" maxlength="1000"></textarea>
<button type="submit" class="btn">Apply to Join</button>
</form>
<div id="msg" class="msg" style="display:none"></div>
</div>
<script>
document.getElementById('af').onsubmit=async e=>{e.preventDefault();const f=new FormData(e.target);const d=Object.fromEntries(f);d.program_id='${program.id}';
try{const r=await fetch('/apply',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});const j=await r.json();
const m=document.getElementById('msg');m.style.display='block';if(r.ok){m.className='msg ok';m.textContent=j.message||'Application submitted!';e.target.style.display='none';}
else{m.className='msg err';m.textContent=j.error||'Failed';}}catch(err){const m=document.getElementById('msg');m.style.display='block';m.className='msg err';m.textContent='Network error';}};
</script></body></html>`, {
        status: 200,
        headers: { 'Content-Type': 'text/html;charset=UTF-8', 'Access-Control-Allow-Origin': '*' },
      });
    }

    // ── Public: Affiliate Application (POST /apply) ──
    if (m === 'POST' && p === '/apply') {
      if (!(await rateLimit(env, `apply:${ip}`, 5, 3600))) return json({ error: 'Rate limited' }, 429);

      const body = await req.json<Record<string, string>>().catch(() => null);
      if (!body?.name || !body?.email || !body?.program_id) return json({ error: 'name, email, program_id required' }, 400);

      const program = await env.DB.prepare('SELECT * FROM programs WHERE id = ? AND is_active = 1').bind(body.program_id).first();
      if (!program) return json({ error: 'Program not found' }, 404);

      // Check duplicate
      const dup = await env.DB.prepare('SELECT id FROM affiliates WHERE email = ? AND program_id = ?').bind(sanitize(body.email, 200), body.program_id).first();
      if (dup) return json({ error: 'Already applied to this program' }, 409);

      const id = uid();
      const refCode = slug6();
      const status = program.auto_approve ? 'approved' : 'pending';

      await env.DB.prepare(
        'INSERT INTO affiliates (id, program_id, tenant_id, name, email, company, website, ref_code, status, approved_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
      ).bind(id, body.program_id, program.tenant_id, sanitize(body.name, 200), sanitize(body.email, 200),
        sanitize(body.company, 200), sanitize(body.website, 500), refCode, status,
        status === 'approved' ? new Date().toISOString() : null
      ).run();

      await env.DB.prepare('UPDATE programs SET total_affiliates = total_affiliates + 1 WHERE id = ?').bind(body.program_id).run();

      // Fire and forget: send welcome email
      (async () => {
        try {
          await env.EMAIL_SENDER.fetch('https://email/send', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              to: body.email,
              subject: `Welcome to ${program.name} Affiliate Program`,
              html: `<h2>You're ${status === 'approved' ? 'Approved' : 'Under Review'}!</h2>
<p>Hi ${sanitize(body.name, 100)},</p>
<p>Thank you for joining the ${program.name} affiliate program.</p>
${status === 'approved' ? `<p>Your referral code: <strong>${refCode}</strong></p>` : '<p>We will review your application shortly.</p>'}`,
            }),
          });
        } catch (_) { /* non-blocking */ }
      })();

      return json({ id, ref_code: refCode, status, message: status === 'approved' ? 'Approved! Check your email for your referral code.' : 'Application submitted. We will review it shortly.' }, 201);
    }

    // ── Public: Record Conversion (POST /convert) ──
    if (m === 'POST' && p === '/convert') {
      if (!(await rateLimit(env, `conv:${ip}`, 30, 60))) return json({ error: 'Rate limited' }, 429);

      const body = await req.json<Record<string, unknown>>().catch(() => null);
      if (!body?.affiliate_ref || !body?.program_id) return json({ error: 'affiliate_ref, program_id required' }, 400);

      const affiliate = await env.DB.prepare('SELECT * FROM affiliates WHERE ref_code = ? AND status IN ("approved","active")')
        .bind(body.affiliate_ref).first();
      if (!affiliate) return json({ error: 'Invalid affiliate' }, 404);

      const program = await env.DB.prepare('SELECT * FROM programs WHERE id = ? AND is_active = 1')
        .bind(body.program_id).first();
      if (!program) return json({ error: 'Program not found' }, 404);

      const revenue = Number(body.revenue) || 0;
      const commType = (affiliate.custom_commission_type as string) || (program.commission_type as string);
      const commValue = (affiliate.custom_commission_value as number) || (program.commission_value as number);

      let commission = 0;
      if (commType === 'percentage') commission = revenue * (commValue / 100);
      else if (commType === 'flat') commission = commValue;
      else if (commType === 'tiered') {
        const tiers = JSON.parse((program.commission_tiers as string) || '[]');
        for (const t of tiers) {
          if (revenue >= (t.min || 0) && revenue <= (t.max || Infinity)) {
            commission = t.type === 'percentage' ? revenue * (t.value / 100) : t.value;
            break;
          }
        }
      }

      commission = Math.round(commission * 100) / 100;

      // Fraud check: same order_id
      if (body.order_id) {
        const dupConv = await env.DB.prepare('SELECT id FROM conversions WHERE order_id = ? AND program_id = ?')
          .bind(body.order_id, body.program_id).first();
        if (dupConv) return json({ error: 'Duplicate conversion' }, 409);
      }

      const convId = uid();
      const fraudFlags: string[] = [];
      let fraudScore = 0;
      if (revenue > 10000) { fraudFlags.push('high_value'); fraudScore += 30; }
      if (!body.customer_email) { fraudFlags.push('no_customer_email'); fraudScore += 10; }

      await env.DB.batch([
        env.DB.prepare(
          'INSERT INTO conversions (id, affiliate_id, program_id, tenant_id, link_id, order_id, customer_email, revenue, commission, commission_type, status, fraud_score, fraud_flags, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        ).bind(convId, affiliate.id, program.id, program.tenant_id, body.link_id || null, body.order_id || null,
          sanitize(body.customer_email as string, 200), revenue, commission, commType,
          fraudScore > 50 ? 'pending' : (program.auto_approve ? 'approved' : 'pending'),
          fraudScore, JSON.stringify(fraudFlags), JSON.stringify(body.metadata || {})
        ),
        env.DB.prepare('UPDATE affiliates SET total_referrals = total_referrals + 1, total_conversions = total_conversions + 1, total_revenue = total_revenue + ?, total_earned = total_earned + ?, balance = balance + ? WHERE id = ?')
          .bind(revenue, commission, commission, affiliate.id),
        env.DB.prepare('UPDATE programs SET total_revenue = total_revenue + ?, total_commissions = total_commissions + ? WHERE id = ?')
          .bind(revenue, commission, program.id),
      ]);

      // Multi-tier: credit parent affiliate
      if (affiliate.parent_id) {
        (async () => {
          try {
            const parent = await env.DB.prepare('SELECT * FROM affiliates WHERE id = ?').bind(affiliate.parent_id).first();
            if (parent) {
              const subComm = Math.round(commission * 0.1 * 100) / 100; // 10% of sub-affiliate commission
              if (subComm > 0) {
                await env.DB.batch([
                  env.DB.prepare(
                    'INSERT INTO conversions (id, affiliate_id, program_id, tenant_id, order_id, revenue, commission, commission_type, status, sub_affiliate_id, parent_conversion_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                  ).bind(uid(), parent.id, program.id, program.tenant_id, body.order_id || null, 0, subComm, 'sub_commission', 'approved', affiliate.id, convId),
                  env.DB.prepare('UPDATE affiliates SET total_earned = total_earned + ?, balance = balance + ? WHERE id = ?')
                    .bind(subComm, subComm, parent.id),
                ]);
              }
            }
          } catch (_) { /* non-blocking */ }
        })();
      }

      return json({ conversion_id: convId, commission, fraud_score: fraudScore, status: fraudScore > 50 ? 'pending_review' : 'recorded' }, 201);
    }

    // ── Public: Affiliate Dashboard Data (GET /dashboard/:ref_code) ──
    if (m === 'GET' && p.startsWith('/dashboard/')) {
      const refCode = p.split('/')[2];
      if (!refCode) return json({ error: 'Missing ref code' }, 400);

      const affiliate = await env.DB.prepare('SELECT * FROM affiliates WHERE ref_code = ?').bind(refCode).first();
      if (!affiliate) return json({ error: 'Not found' }, 404);

      const links = await env.DB.prepare('SELECT * FROM links WHERE affiliate_id = ? ORDER BY created_at DESC LIMIT 20').bind(affiliate.id).all();
      const recentConv = await env.DB.prepare('SELECT * FROM conversions WHERE affiliate_id = ? ORDER BY created_at DESC LIMIT 20').bind(affiliate.id).all();
      const payouts = await env.DB.prepare('SELECT * FROM payouts WHERE affiliate_id = ? ORDER BY created_at DESC LIMIT 10').bind(affiliate.id).all();

      return json({
        affiliate: { id: affiliate.id, name: affiliate.name, ref_code: affiliate.ref_code, tier: affiliate.tier, status: affiliate.status,
          total_clicks: affiliate.total_clicks, total_conversions: affiliate.total_conversions, total_revenue: affiliate.total_revenue,
          total_earned: affiliate.total_earned, total_paid: affiliate.total_paid, balance: affiliate.balance },
        links: links.results,
        conversions: recentConv.results,
        payouts: payouts.results,
      });
    }

    // ═══════════════════════════════════════
    // AUTH REQUIRED BELOW
    // ═══════════════════════════════════════
    if (!authOk(req, env)) return json({ error: 'Unauthorized — X-Echo-API-Key or Bearer token required' }, 401);
    const tenantId = tid(req);

    // ── Programs CRUD ──
    if (p === '/programs' && m === 'GET') {
      const rows = await env.DB.prepare('SELECT * FROM programs WHERE tenant_id = ? ORDER BY created_at DESC').bind(tenantId).all();
      return json({ programs: rows.results });
    }
    if (p === '/programs' && m === 'POST') {
      const body = await req.json<Record<string, unknown>>().catch(() => null);
      if (!body?.name) return json({ error: 'name required' }, 400);
      const id = uid();
      const slug = sanitize(body.slug as string, 100) || slug6();
      await env.DB.prepare(
        'INSERT INTO programs (id, tenant_id, name, slug, description, commission_type, commission_value, commission_tiers, recurring_months, cookie_days, min_payout, payout_frequency, currency, auto_approve, terms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
      ).bind(id, tenantId, sanitize(body.name as string, 200), slug, sanitize(body.description as string, 2000),
        body.commission_type || 'percentage', Number(body.commission_value) || 10, JSON.stringify(body.commission_tiers || []),
        Number(body.recurring_months) || 0, Number(body.cookie_days) || 30, Number(body.min_payout) || 50,
        body.payout_frequency || 'monthly', body.currency || 'USD', body.auto_approve !== false ? 1 : 0,
        sanitize(body.terms as string, 5000)
      ).run();
      return json({ id, slug }, 201);
    }
    if (p.match(/^\/programs\/[^/]+$/) && m === 'GET') {
      const id = p.split('/')[2];
      const row = await env.DB.prepare('SELECT * FROM programs WHERE id = ? AND tenant_id = ?').bind(id, tenantId).first();
      return row ? json(row) : json({ error: 'Not found' }, 404);
    }
    if (p.match(/^\/programs\/[^/]+$/) && m === 'PATCH') {
      const id = p.split('/')[2];
      const body = await req.json<Record<string, unknown>>().catch(() => ({}));
      const fields: string[] = [];
      const vals: unknown[] = [];
      for (const [k, v] of Object.entries(body as Record<string, unknown>)) {
        if (['name','description','commission_type','commission_value','commission_tiers','cookie_days','min_payout','payout_frequency','auto_approve','terms','is_active'].includes(k)) {
          fields.push(`${k} = ?`);
          vals.push(k === 'commission_tiers' ? JSON.stringify(v) : k === 'auto_approve' ? (v ? 1 : 0) : k === 'is_active' ? (v ? 1 : 0) : typeof v === 'string' ? sanitize(v, 5000) : v);
        }
      }
      if (fields.length === 0) return json({ error: 'No fields' }, 400);
      vals.push(id, tenantId);
      await env.DB.prepare(`UPDATE programs SET ${fields.join(', ')} WHERE id = ? AND tenant_id = ?`).bind(...vals).run();
      return json({ updated: true });
    }
    if (p.match(/^\/programs\/[^/]+$/) && m === 'DELETE') {
      const id = p.split('/')[2];
      await env.DB.prepare('DELETE FROM programs WHERE id = ? AND tenant_id = ?').bind(id, tenantId).run();
      return json({ deleted: true });
    }

    // ── Affiliates CRUD ──
    if (p === '/affiliates' && m === 'GET') {
      const programId = url.searchParams.get('program_id');
      const status = url.searchParams.get('status');
      let q = 'SELECT * FROM affiliates WHERE tenant_id = ?';
      const binds: unknown[] = [tenantId];
      if (programId) { q += ' AND program_id = ?'; binds.push(programId); }
      if (status) { q += ' AND status = ?'; binds.push(status); }
      q += ' ORDER BY created_at DESC LIMIT 100';
      const rows = await env.DB.prepare(q).bind(...binds).all();
      return json({ affiliates: rows.results });
    }
    if (p.match(/^\/affiliates\/[^/]+$/) && m === 'GET') {
      const id = p.split('/')[2];
      const row = await env.DB.prepare('SELECT * FROM affiliates WHERE id = ? AND tenant_id = ?').bind(id, tenantId).first();
      return row ? json(row) : json({ error: 'Not found' }, 404);
    }
    if (p.match(/^\/affiliates\/[^/]+$/) && m === 'PATCH') {
      const id = p.split('/')[2];
      const body = await req.json<Record<string, unknown>>().catch(() => ({}));
      const fields: string[] = [];
      const vals: unknown[] = [];
      for (const [k, v] of Object.entries(body as Record<string, unknown>)) {
        if (['status','tier','custom_commission_type','custom_commission_value','payment_method','payment_details'].includes(k)) {
          fields.push(`${k} = ?`);
          vals.push(k === 'payment_details' ? JSON.stringify(v) : typeof v === 'string' ? sanitize(v, 1000) : v);
        }
      }
      if ((body as Record<string, unknown>).status === 'approved') { fields.push('approved_at = ?'); vals.push(new Date().toISOString()); }
      if (fields.length === 0) return json({ error: 'No fields' }, 400);
      vals.push(id, tenantId);
      await env.DB.prepare(`UPDATE affiliates SET ${fields.join(', ')} WHERE id = ? AND tenant_id = ?`).bind(...vals).run();
      return json({ updated: true });
    }

    // ── Links CRUD ──
    if (p === '/links' && m === 'POST') {
      const body = await req.json<Record<string, unknown>>().catch(() => null);
      if (!body?.affiliate_id || !body?.program_id || !body?.destination_url) return json({ error: 'affiliate_id, program_id, destination_url required' }, 400);
      const id = uid();
      const linkSlug = sanitize(body.slug as string, 50) || slug6();
      await env.DB.prepare(
        'INSERT INTO links (id, affiliate_id, program_id, tenant_id, destination_url, slug, utm_source, utm_medium, utm_campaign) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
      ).bind(id, body.affiliate_id, body.program_id, tenantId, sanitize(body.destination_url as string, 2000), linkSlug,
        sanitize(body.utm_source as string, 100), sanitize(body.utm_medium as string, 100), sanitize(body.utm_campaign as string, 100)
      ).run();
      return json({ id, slug: linkSlug, tracking_url: `https://echo-affiliate.bmcii1976.workers.dev/go/${linkSlug}` }, 201);
    }
    if (p === '/links' && m === 'GET') {
      const affId = url.searchParams.get('affiliate_id');
      let q = 'SELECT * FROM links WHERE tenant_id = ?';
      const binds: unknown[] = [tenantId];
      if (affId) { q += ' AND affiliate_id = ?'; binds.push(affId); }
      q += ' ORDER BY created_at DESC LIMIT 100';
      const rows = await env.DB.prepare(q).bind(...binds).all();
      return json({ links: rows.results });
    }

    // ── Conversions ──
    if (p === '/conversions' && m === 'GET') {
      const affId = url.searchParams.get('affiliate_id');
      const status = url.searchParams.get('status');
      let q = 'SELECT * FROM conversions WHERE tenant_id = ?';
      const binds: unknown[] = [tenantId];
      if (affId) { q += ' AND affiliate_id = ?'; binds.push(affId); }
      if (status) { q += ' AND status = ?'; binds.push(status); }
      q += ' ORDER BY created_at DESC LIMIT 100';
      const rows = await env.DB.prepare(q).bind(...binds).all();
      return json({ conversions: rows.results });
    }
    if (p.match(/^\/conversions\/[^/]+$/) && m === 'PATCH') {
      const id = p.split('/')[2];
      const body = await req.json<{ status: string }>().catch(() => null);
      if (!body?.status || !['approved','rejected','reversed'].includes(body.status)) return json({ error: 'Invalid status' }, 400);
      const conv = await env.DB.prepare('SELECT * FROM conversions WHERE id = ? AND tenant_id = ?').bind(id, tenantId).first();
      if (!conv) return json({ error: 'Not found' }, 404);

      const stmts = [
        env.DB.prepare('UPDATE conversions SET status = ?, approved_at = ? WHERE id = ?')
          .bind(body.status, body.status === 'approved' ? new Date().toISOString() : null, id),
      ];

      // If rejecting/reversing, deduct from affiliate balance
      if (body.status === 'rejected' || body.status === 'reversed') {
        stmts.push(
          env.DB.prepare('UPDATE affiliates SET total_earned = total_earned - ?, balance = balance - ? WHERE id = ?')
            .bind(conv.commission, conv.commission, conv.affiliate_id)
        );
      }
      await env.DB.batch(stmts);
      return json({ updated: true });
    }

    // ── Payouts ──
    if (p === '/payouts' && m === 'GET') {
      const status = url.searchParams.get('status');
      let q = 'SELECT p.*, a.name as affiliate_name, a.email as affiliate_email FROM payouts p LEFT JOIN affiliates a ON p.affiliate_id = a.id WHERE p.tenant_id = ?';
      const binds: unknown[] = [tenantId];
      if (status) { q += ' AND p.status = ?'; binds.push(status); }
      q += ' ORDER BY p.created_at DESC LIMIT 100';
      const rows = await env.DB.prepare(q).bind(...binds).all();
      return json({ payouts: rows.results });
    }
    if (p === '/payouts/generate' && m === 'POST') {
      const body = await req.json<{ program_id: string }>().catch(() => null);
      if (!body?.program_id) return json({ error: 'program_id required' }, 400);

      const program = await env.DB.prepare('SELECT * FROM programs WHERE id = ? AND tenant_id = ?').bind(body.program_id, tenantId).first();
      if (!program) return json({ error: 'Program not found' }, 404);

      const eligibleAffiliates = await env.DB.prepare(
        'SELECT * FROM affiliates WHERE program_id = ? AND balance >= ? AND status IN ("approved","active")'
      ).bind(body.program_id, program.min_payout).all();

      const payoutIds: string[] = [];
      for (const aff of eligibleAffiliates.results) {
        const payoutId = uid();
        await env.DB.batch([
          env.DB.prepare(
            'INSERT INTO payouts (id, affiliate_id, program_id, tenant_id, amount, currency, method, status, period_start, period_end) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
          ).bind(payoutId, aff.id, body.program_id, tenantId, aff.balance, program.currency, aff.payment_method || 'paypal', 'pending',
            new Date(Date.now() - 30 * 86400000).toISOString(), new Date().toISOString()),
          env.DB.prepare('UPDATE affiliates SET balance = 0, total_paid = total_paid + ? WHERE id = ?')
            .bind(aff.balance, aff.id),
        ]);
        payoutIds.push(payoutId);
      }

      return json({ generated: payoutIds.length, payout_ids: payoutIds });
    }
    if (p.match(/^\/payouts\/[^/]+$/) && m === 'PATCH') {
      const id = p.split('/')[2];
      const body = await req.json<{ status: string; reference?: string }>().catch(() => null);
      if (!body?.status) return json({ error: 'status required' }, 400);
      await env.DB.prepare('UPDATE payouts SET status = ?, reference = ?, processed_at = ? WHERE id = ? AND tenant_id = ?')
        .bind(body.status, body.reference || null, body.status === 'completed' ? new Date().toISOString() : null, id, tenantId).run();
      return json({ updated: true });
    }

    // ── Creatives CRUD ──
    if (p === '/creatives' && m === 'GET') {
      const progId = url.searchParams.get('program_id');
      let q = 'SELECT * FROM creatives WHERE tenant_id = ?';
      const binds: unknown[] = [tenantId];
      if (progId) { q += ' AND program_id = ?'; binds.push(progId); }
      q += ' ORDER BY created_at DESC';
      const rows = await env.DB.prepare(q).bind(...binds).all();
      return json({ creatives: rows.results });
    }
    if (p === '/creatives' && m === 'POST') {
      const body = await req.json<Record<string, unknown>>().catch(() => null);
      if (!body?.name || !body?.program_id) return json({ error: 'name, program_id required' }, 400);
      const id = uid();
      await env.DB.prepare(
        'INSERT INTO creatives (id, program_id, tenant_id, name, type, content, dimensions, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
      ).bind(id, body.program_id, tenantId, sanitize(body.name as string, 200), body.type || 'banner',
        JSON.stringify(body.content || {}), sanitize(body.dimensions as string, 50), sanitize(body.url as string, 2000)
      ).run();
      return json({ id }, 201);
    }

    // ── Fraud Rules ──
    if (p === '/fraud-rules' && m === 'GET') {
      const rows = await env.DB.prepare('SELECT * FROM fraud_rules WHERE tenant_id = ? ORDER BY created_at DESC').bind(tenantId).all();
      return json({ rules: rows.results });
    }
    if (p === '/fraud-rules' && m === 'POST') {
      const body = await req.json<Record<string, unknown>>().catch(() => null);
      if (!body?.name) return json({ error: 'name required' }, 400);
      const id = uid();
      await env.DB.prepare('INSERT INTO fraud_rules (id, tenant_id, name, type, rule, action) VALUES (?, ?, ?, ?, ?, ?)')
        .bind(id, tenantId, sanitize(body.name as string, 200), body.type || 'auto', JSON.stringify(body.rule || {}), body.action || 'flag').run();
      return json({ id }, 201);
    }

    // ── Analytics ──
    if (p === '/analytics/overview' && m === 'GET') {
      const cacheKey = `analytics:${tenantId}:overview`;
      const cached = await env.CACHE.get(cacheKey, 'json');
      if (cached) return json(cached);

      const programs = await env.DB.prepare('SELECT COUNT(*) as c, SUM(total_revenue) as rev, SUM(total_commissions) as comm, SUM(total_affiliates) as affs FROM programs WHERE tenant_id = ?').bind(tenantId).first();
      const pending = await env.DB.prepare('SELECT COUNT(*) as c, SUM(commission) as total FROM conversions WHERE tenant_id = ? AND status = "pending"').bind(tenantId).first();
      const payoutsPending = await env.DB.prepare('SELECT COUNT(*) as c, SUM(amount) as total FROM payouts WHERE tenant_id = ? AND status = "pending"').bind(tenantId).first();

      const result = {
        programs: programs?.c || 0,
        total_affiliates: programs?.affs || 0,
        total_revenue: programs?.rev || 0,
        total_commissions: programs?.comm || 0,
        pending_conversions: pending?.c || 0,
        pending_commission_value: pending?.total || 0,
        pending_payouts: payoutsPending?.c || 0,
        pending_payout_value: payoutsPending?.total || 0,
      };
      await env.CACHE.put(cacheKey, JSON.stringify(result), { expirationTtl: 300 });
      return json(result);
    }

    if (p === '/analytics/trends' && m === 'GET') {
      const days = Number(url.searchParams.get('days')) || 30;
      const rows = await env.DB.prepare(
        'SELECT * FROM analytics_daily WHERE tenant_id = ? AND date >= date("now", ? || " days") ORDER BY date DESC'
      ).bind(tenantId, `-${days}`).all();
      return json({ trends: rows.results });
    }

    if (p === '/analytics/leaderboard' && m === 'GET') {
      const programId = url.searchParams.get('program_id');
      let q = 'SELECT id, name, email, ref_code, tier, total_clicks, total_conversions, total_revenue, total_earned FROM affiliates WHERE tenant_id = ?';
      const binds: unknown[] = [tenantId];
      if (programId) { q += ' AND program_id = ?'; binds.push(programId); }
      q += ' ORDER BY total_revenue DESC LIMIT 50';
      const rows = await env.DB.prepare(q).bind(...binds).all();
      return json({ leaderboard: rows.results });
    }

    // ── AI Insights ──
    if (p === '/ai/insights' && m === 'GET') {
      const overview = await env.DB.prepare(
        'SELECT SUM(total_revenue) as rev, SUM(total_commissions) as comm, SUM(total_affiliates) as affs FROM programs WHERE tenant_id = ?'
      ).bind(tenantId).first();
      const topAffs = await env.DB.prepare(
        'SELECT name, total_clicks, total_conversions, total_revenue FROM affiliates WHERE tenant_id = ? ORDER BY total_revenue DESC LIMIT 5'
      ).bind(tenantId).all();
      const convRate = await env.DB.prepare(
        'SELECT COUNT(CASE WHEN status = "approved" THEN 1 END) as approved, COUNT(*) as total FROM conversions WHERE tenant_id = ?'
      ).bind(tenantId).first();

      try {
        const aiResp = await env.ENGINE_RUNTIME.fetch('https://engine/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            engine_id: 'GEN-01',
            query: `Analyze this affiliate program data and provide 3-5 actionable insights: Revenue: $${overview?.rev || 0}, Commissions: $${overview?.comm || 0}, Affiliates: ${overview?.affs || 0}, Conversion rate: ${convRate?.total ? Math.round(((convRate?.approved as number) / (convRate?.total as number)) * 100) : 0}%, Top affiliates: ${JSON.stringify(topAffs.results)}. Focus on growth opportunities, optimization, and risk areas.`,
          }),
        });
        const aiData = await aiResp.json() as { response?: string };
        return json({ insights: aiData.response || 'No insights available', data: { overview, top_affiliates: topAffs.results, conversion_rate: convRate } });
      } catch (_) {
        return json({ insights: 'AI insights temporarily unavailable', data: { overview, top_affiliates: topAffs.results, conversion_rate: convRate } });
      }
    }

    // ── Export ──
    if (p === '/export' && m === 'GET') {
      const type = url.searchParams.get('type') || 'conversions';
      const format = url.searchParams.get('format') || 'json';
      let rows;
      if (type === 'affiliates') {
        rows = await env.DB.prepare('SELECT * FROM affiliates WHERE tenant_id = ? ORDER BY created_at DESC').bind(tenantId).all();
      } else if (type === 'payouts') {
        rows = await env.DB.prepare('SELECT * FROM payouts WHERE tenant_id = ? ORDER BY created_at DESC').bind(tenantId).all();
      } else {
        rows = await env.DB.prepare('SELECT * FROM conversions WHERE tenant_id = ? ORDER BY created_at DESC').bind(tenantId).all();
      }
      if (format === 'csv') {
        const data = rows.results;
        if (data.length === 0) return new Response('', { headers: { 'Content-Type': 'text/csv' } });
        const headers = Object.keys(data[0]);
        const csv = [headers.join(','), ...data.map(r => headers.map(h => `"${String((r as Record<string, unknown>)[h] ?? '').replace(/"/g, '""')}"`).join(','))].join('\n');
        return new Response(csv, { headers: { 'Content-Type': 'text/csv', 'Content-Disposition': `attachment; filename=${type}_export.csv` } });
      }
      return json({ [type]: rows.results });
    }

    // ── Activity Log ──
    if (p === '/activity' && m === 'GET') {
      const rows = await env.DB.prepare('SELECT * FROM activity_log WHERE tenant_id = ? ORDER BY created_at DESC LIMIT 100').bind(tenantId).all();
      return json({ activity: rows.results });
    }

    return json({ error: 'Not found' }, 404);
    } catch (e: any) {
      if (e.message?.includes('JSON')) {
        return json({ error: 'Invalid JSON body' }, 400);
      }
      slog('error', 'Unhandled request error', { error: e.message, stack: e.stack });
      return json({ error: 'Internal server error' }, 500);
    }
  },

  async scheduled(_event: ScheduledEvent, env: Env, _ctx: ExecutionContext): Promise<void> {
    // Daily analytics aggregation
    const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
    const programs = await env.DB.prepare('SELECT DISTINCT tenant_id, id as program_id FROM programs').all();

    for (const prog of programs.results) {
      const clicks = await env.DB.prepare(
        'SELECT COUNT(*) as total, SUM(is_unique) as uniq FROM clicks WHERE program_id = ? AND date(created_at) = ?'
      ).bind(prog.program_id, yesterday).first();
      const convs = await env.DB.prepare(
        'SELECT COUNT(*) as total, SUM(revenue) as rev, SUM(commission) as comm FROM conversions WHERE program_id = ? AND date(created_at) = ?'
      ).bind(prog.program_id, yesterday).first();
      const signups = await env.DB.prepare(
        'SELECT COUNT(*) as total FROM affiliates WHERE program_id = ? AND date(created_at) = ?'
      ).bind(prog.program_id, yesterday).first();
      const payoutsTotal = await env.DB.prepare(
        'SELECT SUM(amount) as total FROM payouts WHERE program_id = ? AND date(created_at) = ? AND status = "completed"'
      ).bind(prog.program_id, yesterday).first();

      await env.DB.prepare(
        'INSERT OR REPLACE INTO analytics_daily (tenant_id, program_id, date, clicks, unique_clicks, conversions, revenue, commissions, signups, payouts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
      ).bind(prog.tenant_id, prog.program_id, yesterday,
        clicks?.total || 0, clicks?.uniq || 0, convs?.total || 0, convs?.rev || 0, convs?.comm || 0,
        signups?.total || 0, payoutsTotal?.total || 0
      ).run();
    }
  },
};
