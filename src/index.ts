/**
 * LoveSlut Media Proxy v3.0 ULTIMATE — Cloudflare Worker
 * Proxies image/video/img2img/variations/enhance/catalog to RunPod Serverless.
 * Rate limiting, usage tracking, gallery, favorites, cost estimation.
 * D1 for tracking, KV for caching + rate limits, Shared Brain for memory.
 */

export interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  SHARED_BRAIN: Fetcher;
  AI: any;
  RUNPOD_API_KEY: string;
  RUNPOD_ENDPOINT_ID: string;
  ECHO_API_KEY: string;
}

// ═══════════════════════════════════════════════════
// TYPES
// ═══════════════════════════════════════════════════

type GenerationMode = 'image' | 'video' | 'img2img' | 'variations' | 'enhance' | 'catalog';

interface GenerateRequest {
  mode: GenerationMode;
  character: string;
  clothing?: string;
  pose?: string;
  scene?: string;
  style_preset?: string;
  prompt?: string;
  negative_prompt?: string;
  expression?: string;
  accessories?: string;
  camera_angle?: string;
  lighting?: string;
  motion?: string;
  width?: number;
  height?: number;
  steps?: number;
  guidance_scale?: number;
  seed?: number;
  batch_size?: number;
  num_frames?: number;
  fps?: number;
  image?: string;
  strength?: number;
  count?: number;
  sharpen?: number;
  contrast?: number;
  color?: number;
  brightness?: number;
  upscale?: number;
  user_id?: string;
}

interface RunPodResponse {
  id: string;
  status: string;
  output?: Record<string, unknown>;
  error?: string;
}

// ═══════════════════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════════════════

const VERSION = '3.2.0';

// ═══════════════════════════════════════════════════
// CHAT TYPES
// ═══════════════════════════════════════════════════

interface ChatRequest {
  message: string;
  system_prompt?: string;
  conversation_history?: { role: string; content: string }[];
  user_id?: string;
  options?: { temperature?: number; max_tokens?: number };
}

const VALID_MODES: GenerationMode[] = ['image', 'video', 'img2img', 'variations', 'enhance', 'catalog'];

const CHARACTERS = [
  'luna', 'scarlett', 'ivy', 'raven', 'chloe', 'mistress',
  'sakura', 'valentina', 'natasha', 'ember', 'diamond', 'pixie',
];

const CLOTHING_LEVELS = [
  'dressed', 'casual', 'workout', 'swimsuit', 'lingerie',
  'sheer', 'topless', 'bottomless', 'nude', 'bondage',
];

const POSES = [
  'standing', 'sitting', 'lying', 'kneeling', 'bending', 'spreading',
  'doggy', 'riding', 'shower', 'mirror', 'sleeping', 'dancing',
];

const SCENES = [
  'bedroom', 'bathroom', 'pool', 'beach', 'office',
  'dungeon', 'studio', 'forest', 'penthouse', 'onsen',
];

const STYLES = [
  'photorealistic', 'anime', 'oil_painting', 'noir',
  'cyberpunk', 'fantasy', 'polaroid', 'magazine',
];

const COST_PER_MODE: Record<GenerationMode, number> = {
  image: 0.004,
  video: 0.025,
  img2img: 0.005,
  variations: 0.016,
  enhance: 0.002,
  catalog: 0,
};

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, X-Echo-API-Key, X-User-Id',
};

const RATE_LIMIT_WINDOW = 60;
const RATE_LIMIT_MAX = 30;
const RATE_LIMIT_MAX_VIDEO = 5;

// ═══════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
  });
}

function log(level: string, msg: string, meta?: Record<string, unknown>): void {
  const entry = { ts: new Date().toISOString(), level, service: 'loveslut-media-proxy', msg, ...meta };
  console.log(JSON.stringify(entry));
}

function getUserId(request: Request): string {
  return request.headers.get('X-User-Id') || request.headers.get('CF-Connecting-IP') || 'anon';
}

// ═══════════════════════════════════════════════════
// D1 SCHEMA INIT
// ═══════════════════════════════════════════════════

async function ensureSchema(db: D1Database): Promise<void> {
  await db.batch([
    db.prepare(`CREATE TABLE IF NOT EXISTS generations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      job_id TEXT NOT NULL UNIQUE,
      user_id TEXT NOT NULL DEFAULT 'anon',
      mode TEXT NOT NULL,
      character TEXT NOT NULL,
      clothing TEXT DEFAULT 'dressed',
      pose TEXT DEFAULT 'standing',
      scene TEXT DEFAULT '',
      style_preset TEXT DEFAULT 'photorealistic',
      prompt TEXT DEFAULT '',
      status TEXT DEFAULT 'PENDING',
      cost_estimate REAL DEFAULT 0,
      duration_ms INTEGER DEFAULT 0,
      result_cached INTEGER DEFAULT 0,
      error TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      completed_at TEXT
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS favorites (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT NOT NULL,
      job_id TEXT NOT NULL,
      character TEXT NOT NULL,
      mode TEXT NOT NULL,
      thumbnail TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(user_id, job_id)
    )`),
    db.prepare(`CREATE TABLE IF NOT EXISTS usage_tracking (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT NOT NULL,
      mode TEXT NOT NULL,
      cost REAL DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    )`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_gen_user ON generations(user_id, created_at DESC)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_gen_status ON generations(status)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_fav_user ON favorites(user_id, created_at DESC)`),
    db.prepare(`CREATE INDEX IF NOT EXISTS idx_usage_user ON usage_tracking(user_id, created_at DESC)`),
  ]);
}

// ═══════════════════════════════════════════════════
// RATE LIMITING
// ═══════════════════════════════════════════════════

async function checkRateLimit(
  userId: string, mode: GenerationMode, kv: KVNamespace
): Promise<{ allowed: boolean; remaining: number; resetIn: number }> {
  const window = Math.floor(Date.now() / (RATE_LIMIT_WINDOW * 1000));
  const key = `rl:${userId}:${window}`;
  const current = parseInt(await kv.get(key) || '0');
  const limit = mode === 'video' ? RATE_LIMIT_MAX_VIDEO : RATE_LIMIT_MAX;

  if (current >= limit) {
    return { allowed: false, remaining: 0, resetIn: RATE_LIMIT_WINDOW - (Math.floor(Date.now() / 1000) % RATE_LIMIT_WINDOW) };
  }

  await kv.put(key, String(current + 1), { expirationTtl: RATE_LIMIT_WINDOW * 2 });
  return { allowed: true, remaining: limit - current - 1, resetIn: 0 };
}

// ═══════════════════════════════════════════════════
// RUNPOD API HELPERS
// ═══════════════════════════════════════════════════

async function submitToRunPod(input: Record<string, unknown>, env: Env): Promise<RunPodResponse> {
  const url = `https://api.runpod.ai/v2/${env.RUNPOD_ENDPOINT_ID}/run`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${env.RUNPOD_API_KEY}`,
    },
    body: JSON.stringify({ input }),
  });

  if (!res.ok) {
    const err = await res.text();
    log('error', 'RunPod submission failed', { status: res.status, error: err });
    throw new Error(`RunPod error ${res.status}: ${err}`);
  }

  return res.json();
}

async function checkRunPodStatus(jobId: string, env: Env): Promise<RunPodResponse> {
  const url = `https://api.runpod.ai/v2/${env.RUNPOD_ENDPOINT_ID}/status/${jobId}`;
  const res = await fetch(url, {
    headers: { 'Authorization': `Bearer ${env.RUNPOD_API_KEY}` },
  });

  if (!res.ok) {
    throw new Error(`Status check failed: ${res.status}`);
  }

  return res.json();
}

async function cancelRunPodJob(jobId: string, env: Env): Promise<boolean> {
  const url = `https://api.runpod.ai/v2/${env.RUNPOD_ENDPOINT_ID}/cancel/${jobId}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${env.RUNPOD_API_KEY}` },
  });
  return res.ok;
}

// ═══════════════════════════════════════════════════
// ROUTE HANDLERS
// ═══════════════════════════════════════════════════

async function handleHealth(env: Env): Promise<Response> {
  let dbStatus = 'unknown';
  let genCount = 0;
  try {
    const row = await env.DB.prepare('SELECT COUNT(*) as c FROM generations').first<{ c: number }>();
    genCount = row?.c || 0;
    dbStatus = 'ok';
  } catch { dbStatus = 'error'; }

  return json({
    status: 'ok',
    version: VERSION,
    service: 'loveslut-media-proxy',
    runpod_endpoint: env.RUNPOD_ENDPOINT_ID ? 'configured' : 'missing',
    database: dbStatus,
    total_generations: genCount,
    characters: CHARACTERS.length,
    modes: VALID_MODES,
    uptime: 'edge',
  });
}

async function handleGenerate(request: Request, env: Env): Promise<Response> {
  if (request.method !== 'POST') return json({ error: 'POST required' }, 405);

  const body: GenerateRequest = await request.json();
  const userId = body.user_id || getUserId(request);
  const mode = body.mode || 'image';

  // Validate mode
  if (!VALID_MODES.includes(mode)) {
    return json({ error: `Invalid mode. Available: ${VALID_MODES.join(', ')}` }, 400);
  }

  // Catalog is instant — no RunPod needed
  if (mode === 'catalog') {
    return json({
      characters: CHARACTERS,
      clothing_levels: CLOTHING_LEVELS,
      poses: POSES,
      scenes: SCENES,
      style_presets: STYLES,
      total_characters: CHARACTERS.length,
      total_combinations: CHARACTERS.length * CLOTHING_LEVELS.length * POSES.length * SCENES.length * STYLES.length,
      cost_per_mode: COST_PER_MODE,
    });
  }

  // Validate character
  if (!body.character || !CHARACTERS.includes(body.character)) {
    return json({ error: `character required. Available: ${CHARACTERS.join(', ')}` }, 400);
  }

  // Validate img2img/enhance require image
  if ((mode === 'img2img' || mode === 'enhance') && !body.image) {
    return json({ error: `${mode} mode requires 'image' (base64)` }, 400);
  }

  // Rate limit check
  const rl = await checkRateLimit(userId, mode, env.CACHE);
  if (!rl.allowed) {
    return json({
      error: 'Rate limit exceeded',
      retry_after: rl.resetIn,
      limit: mode === 'video' ? RATE_LIMIT_MAX_VIDEO : RATE_LIMIT_MAX,
    }, 429);
  }

  // Cache check (deterministic seed only)
  if (body.seed !== undefined && body.seed >= 0) {
    const cacheKey = `gen:${JSON.stringify({ mode, character: body.character, clothing: body.clothing, pose: body.pose, scene: body.scene, style_preset: body.style_preset, seed: body.seed, width: body.width, height: body.height })}`;
    const cached = await env.CACHE.get(cacheKey);
    if (cached) {
      log('info', 'Cache hit', { mode, character: body.character, userId });
      return json({ status: 'COMPLETED', cached: true, output: JSON.parse(cached) });
    }
  }

  // Estimate cost
  const costEstimate = COST_PER_MODE[mode] * (body.batch_size || 1);

  // Submit to RunPod
  try {
    const rpData = await submitToRunPod(body as unknown as Record<string, unknown>, env);

    // Log to D1
    try {
      await env.DB.prepare(
        `INSERT INTO generations (job_id, user_id, mode, character, clothing, pose, scene, style_preset, prompt, status, cost_estimate, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`
      ).bind(
        rpData.id, userId, mode, body.character,
        body.clothing || 'dressed', body.pose || 'standing',
        body.scene || '', body.style_preset || 'photorealistic',
        body.prompt || '', rpData.status, costEstimate,
      ).run();
    } catch (e) { log('warn', 'D1 insert failed', { error: String(e) }); }

    // Track usage
    try {
      await env.DB.prepare(
        `INSERT INTO usage_tracking (user_id, mode, cost, created_at) VALUES (?, ?, ?, datetime('now'))`
      ).bind(userId, mode, costEstimate).run();
    } catch {}

    log('info', 'Job submitted', { jobId: rpData.id, mode, character: body.character, userId, cost: costEstimate });

    return json({
      job_id: rpData.id,
      status: rpData.status,
      mode,
      character: body.character,
      cost_estimate: costEstimate,
      rate_limit_remaining: rl.remaining,
      poll_url: `/status?job_id=${rpData.id}`,
      result_url: `/result?job_id=${rpData.id}`,
    });
  } catch (err) {
    log('error', 'Generation failed', { error: String(err), mode, character: body.character });
    return json({ error: 'RunPod submission failed', detail: String(err) }, 502);
  }
}

async function handleStatus(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const jobId = url.searchParams.get('job_id');
  if (!jobId) return json({ error: 'job_id required' }, 400);

  try {
    const data = await checkRunPodStatus(jobId, env);

    // Update D1
    try {
      if (data.status === 'COMPLETED') {
        await env.DB.prepare(
          `UPDATE generations SET status = ?, completed_at = datetime('now') WHERE job_id = ?`
        ).bind(data.status, jobId).run();
      } else {
        await env.DB.prepare(
          `UPDATE generations SET status = ? WHERE job_id = ?`
        ).bind(data.status, jobId).run();
      }
    } catch {}

    // Cache completed results
    if (data.status === 'COMPLETED' && data.output) {
      await env.CACHE.put(`result:${jobId}`, JSON.stringify(data.output), { expirationTtl: 7200 });
    }

    // Update D1 with error if failed
    if (data.status === 'FAILED' && data.error) {
      try {
        await env.DB.prepare(
          `UPDATE generations SET status = 'FAILED', error = ? WHERE job_id = ?`
        ).bind(data.error, jobId).run();
      } catch {}
    }

    return json({
      job_id: jobId,
      status: data.status,
      output: data.output || null,
      error: data.error || null,
    });
  } catch (err) {
    return json({ error: 'Status check failed', detail: String(err) }, 502);
  }
}

async function handleResult(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const jobId = url.searchParams.get('job_id');
  if (!jobId) return json({ error: 'job_id required' }, 400);

  // Check KV cache first
  const cached = await env.CACHE.get(`result:${jobId}`);
  if (cached) {
    return json({ status: 'COMPLETED', cached: true, output: JSON.parse(cached) });
  }

  // Poll RunPod
  try {
    const data = await checkRunPodStatus(jobId, env);

    if (data.status === 'COMPLETED' && data.output) {
      await env.CACHE.put(`result:${jobId}`, JSON.stringify(data.output), { expirationTtl: 7200 });
      return json({ status: 'COMPLETED', output: data.output });
    }

    return json({ status: data.status, output: null, error: data.error || null });
  } catch (err) {
    return json({ error: 'Result fetch failed', detail: String(err) }, 502);
  }
}

async function handleCancel(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const jobId = url.searchParams.get('job_id');
  if (!jobId) return json({ error: 'job_id required' }, 400);

  const cancelled = await cancelRunPodJob(jobId, env);
  if (cancelled) {
    try {
      await env.DB.prepare(
        `UPDATE generations SET status = 'CANCELLED' WHERE job_id = ?`
      ).bind(jobId).run();
    } catch {}
  }

  return json({ job_id: jobId, cancelled });
}

async function handleHistory(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 200);
  const userId = url.searchParams.get('user_id') || getUserId(request);
  const character = url.searchParams.get('character');
  const mode = url.searchParams.get('mode');
  const offset = parseInt(url.searchParams.get('offset') || '0');

  let query = 'SELECT * FROM generations WHERE user_id = ?';
  const params: unknown[] = [userId];

  if (character) {
    query += ' AND character = ?';
    params.push(character);
  }
  if (mode) {
    query += ' AND mode = ?';
    params.push(mode);
  }

  query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);

  const result = await env.DB.prepare(query).bind(...params).all();

  // Get total count
  let countQuery = 'SELECT COUNT(*) as total FROM generations WHERE user_id = ?';
  const countParams: unknown[] = [userId];
  if (character) { countQuery += ' AND character = ?'; countParams.push(character); }
  if (mode) { countQuery += ' AND mode = ?'; countParams.push(mode); }
  const countRow = await env.DB.prepare(countQuery).bind(...countParams).first<{ total: number }>();

  return json({
    generations: result.results,
    total: countRow?.total || 0,
    limit,
    offset,
    has_more: (countRow?.total || 0) > offset + limit,
  });
}

async function handleFavorites(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const userId = url.searchParams.get('user_id') || getUserId(request);

  if (request.method === 'POST') {
    const body = await request.json() as { job_id: string; character: string; mode: string; thumbnail?: string };
    if (!body.job_id) return json({ error: 'job_id required' }, 400);

    try {
      await env.DB.prepare(
        `INSERT OR IGNORE INTO favorites (user_id, job_id, character, mode, thumbnail, created_at) VALUES (?, ?, ?, ?, ?, datetime('now'))`
      ).bind(userId, body.job_id, body.character || '', body.mode || 'image', body.thumbnail || '').run();
      return json({ favorited: true, job_id: body.job_id });
    } catch (e) {
      return json({ error: 'Failed to favorite', detail: String(e) }, 500);
    }
  }

  if (request.method === 'DELETE') {
    const jobId = url.searchParams.get('job_id');
    if (!jobId) return json({ error: 'job_id required' }, 400);
    await env.DB.prepare('DELETE FROM favorites WHERE user_id = ? AND job_id = ?').bind(userId, jobId).run();
    return json({ unfavorited: true, job_id: jobId });
  }

  // GET — list favorites
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 200);
  const result = await env.DB.prepare(
    'SELECT * FROM favorites WHERE user_id = ? ORDER BY created_at DESC LIMIT ?'
  ).bind(userId, limit).all();

  return json({ favorites: result.results, total: result.results.length });
}

async function handleUsage(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const userId = url.searchParams.get('user_id') || getUserId(request);
  const period = url.searchParams.get('period') || 'day';

  let since: string;
  const now = new Date();
  if (period === 'hour') {
    since = new Date(now.getTime() - 3600000).toISOString();
  } else if (period === 'day') {
    since = new Date(now.getTime() - 86400000).toISOString();
  } else if (period === 'week') {
    since = new Date(now.getTime() - 604800000).toISOString();
  } else {
    since = new Date(now.getTime() - 2592000000).toISOString();
  }

  const stats = await env.DB.prepare(`
    SELECT mode, COUNT(*) as count, SUM(cost) as total_cost
    FROM usage_tracking WHERE user_id = ? AND created_at >= ?
    GROUP BY mode
  `).bind(userId, since).all();

  const totals = await env.DB.prepare(`
    SELECT COUNT(*) as total_requests, SUM(cost) as total_cost
    FROM usage_tracking WHERE user_id = ? AND created_at >= ?
  `).bind(userId, since).first<{ total_requests: number; total_cost: number }>();

  return json({
    user_id: userId,
    period,
    since,
    by_mode: stats.results,
    total_requests: totals?.total_requests || 0,
    total_cost: totals?.total_cost || 0,
  });
}

async function handleStats(env: Env): Promise<Response> {
  const today = new Date().toISOString().split('T')[0];

  const todayStats = await env.DB.prepare(`
    SELECT mode, COUNT(*) as count, AVG(duration_ms) as avg_duration
    FROM generations WHERE created_at >= ? GROUP BY mode
  `).bind(today).all();

  const totalStats = await env.DB.prepare(`
    SELECT COUNT(*) as total, COUNT(DISTINCT user_id) as unique_users,
           SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
           SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
           SUM(cost_estimate) as total_cost
    FROM generations
  `).first();

  const charStats = await env.DB.prepare(`
    SELECT character, COUNT(*) as count FROM generations
    GROUP BY character ORDER BY count DESC LIMIT 12
  `).all();

  const clothingStats = await env.DB.prepare(`
    SELECT clothing, COUNT(*) as count FROM generations
    GROUP BY clothing ORDER BY count DESC
  `).all();

  return json({
    date: today,
    today: todayStats.results,
    all_time: totalStats,
    by_character: charStats.results,
    by_clothing: clothingStats.results,
    version: VERSION,
  });
}

async function handleCostEstimate(request: Request): Promise<Response> {
  const url = new URL(request.url);
  const mode = (url.searchParams.get('mode') || 'image') as GenerationMode;
  const batchSize = parseInt(url.searchParams.get('batch_size') || '1');
  const count = parseInt(url.searchParams.get('count') || '1');

  const baseCost = COST_PER_MODE[mode] || 0;
  const multiplier = mode === 'variations' ? count : batchSize;
  const total = baseCost * multiplier;

  return json({
    mode,
    base_cost_usd: baseCost,
    multiplier,
    total_cost_usd: total,
    note: 'Estimated. Actual cost depends on GPU time.',
    pricing: COST_PER_MODE,
  });
}

async function handleCharacters(): Promise<Response> {
  const CHARACTER_DATA: Record<string, { name: string; personality: string; body_type: string; style: string }> = {
    luna: { name: 'Luna', personality: 'Sweet, shy, romantic', body_type: 'petite, slender', style: 'Romantic pink tones' },
    scarlett: { name: 'Scarlett', personality: 'Dominant, confident, fierce', body_type: 'athletic, toned', style: 'Dramatic red lighting' },
    ivy: { name: 'Ivy', personality: 'Smart, teasing, curious', body_type: 'slender, elegant', style: 'Warm amber candlelight' },
    raven: { name: 'Raven', personality: 'Dark, intense, passionate', body_type: 'curvy, gothic', style: 'Dark purple moody' },
    chloe: { name: 'Chloe', personality: 'Bubbly, playful, adventurous', body_type: 'fit, toned', style: 'Golden sunlight' },
    mistress: { name: 'Mistress Kira', personality: 'Commanding, strict, seductive', body_type: 'tall, statuesque', style: 'Dramatic dungeon' },
    sakura: { name: 'Sakura', personality: 'Gentle, graceful, devoted', body_type: 'petite, delicate', style: 'Cherry blossom pink' },
    valentina: { name: 'Valentina', personality: 'Passionate, fiery, sensual', body_type: 'voluptuous, curvy', style: 'Golden sunset tropical' },
    natasha: { name: 'Natasha', personality: 'Cold exterior, passionate interior', body_type: 'tall, athletic', style: 'Cool blue minimalist' },
    ember: { name: 'Ember', personality: 'Wild, free-spirited, teasing', body_type: 'dancer, flexible', style: 'Warm firelight bohemian' },
    diamond: { name: 'Diamond', personality: 'Regal, confident, classy', body_type: 'model, long legs', style: 'Studio high fashion' },
    pixie: { name: 'Pixie', personality: 'Cute, energetic, submissive', body_type: 'tiny, petite', style: 'Neon kawaii pastel' },
  };

  return json({
    characters: CHARACTER_DATA,
    total: Object.keys(CHARACTER_DATA).length,
    clothing_levels: CLOTHING_LEVELS,
    poses: POSES,
    scenes: SCENES,
    style_presets: STYLES,
  });
}

// ═══════════════════════════════════════════════════
// CHAT (Workers AI — free, uncensored via system prompt)
// ═══════════════════════════════════════════════════

async function handleChat(request: Request, env: Env): Promise<Response> {
  if (request.method !== 'POST') return json({ error: 'POST required' }, 405);

  const body: ChatRequest = await request.json();
  if (!body.message) return json({ error: 'message required' }, 400);

  const userId = body.user_id || getUserId(request);

  // Rate limit chat: 30 per minute
  const rl = await checkRateLimit(userId, 'image', env.CACHE);
  if (!rl.allowed) {
    return json({ error: 'Rate limit exceeded', retry_after: rl.resetIn }, 429);
  }

  // Build message array for the LLM
  const messages: { role: string; content: string }[] = [];

  if (body.system_prompt) {
    messages.push({ role: 'system', content: body.system_prompt });
  }

  if (body.conversation_history?.length) {
    for (const msg of body.conversation_history.slice(-16)) {
      messages.push({ role: msg.role, content: msg.content });
    }
  }

  // Ensure current message is included
  const lastMsg = messages[messages.length - 1];
  if (!lastMsg || lastMsg.role !== 'user' || lastMsg.content !== body.message) {
    messages.push({ role: 'user', content: body.message });
  }

  // Model cascade: try best model first, fallback to smaller
  const MODELS = [
    '@cf/meta/llama-3.3-70b-instruct-fp8-fast',
    '@cf/meta/llama-3.1-8b-instruct',
  ];

  let response = '';
  let modelUsed = '';

  for (const model of MODELS) {
    try {
      const result = await env.AI.run(model, {
        messages,
        temperature: body.options?.temperature ?? 0.95,
        max_tokens: body.options?.max_tokens ?? 1024,
      });
      response = result?.response || '';
      modelUsed = model;
      if (response) break;
    } catch (err) {
      log('warn', `Model ${model} failed, trying next`, { error: String(err) });
      continue;
    }
  }

  if (!response) {
    log('error', 'All chat models failed');
    return json({ error: 'AI generation failed — all models exhausted' }, 502);
  }

  log('info', 'Chat generated', {
    user_id: userId,
    response_length: response.length,
    model: modelUsed,
  });

  return json({ response, model: modelUsed, personality: 'custom' });
}

// ═══════════════════════════════════════════════════
// MAIN ROUTER
// ═══════════════════════════════════════════════════

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    const url = new URL(request.url);

    // Public endpoints (no auth)
    if (url.pathname === '/') return json({ service: 'loveslut-media-proxy', status: 'operational' });
    if (url.pathname === '/health') return handleHealth(env);
    if (url.pathname === '/characters') return handleCharacters();
    if (url.pathname === '/cost') return handleCostEstimate(request);

    // Auth check for all other endpoints
    const apiKey = request.headers.get('X-Echo-API-Key');
    if (apiKey !== env.ECHO_API_KEY) {
      return json({ error: 'Unauthorized' }, 401);
    }

    // Ensure D1 schema
    try { await ensureSchema(env.DB); } catch {}

    try {
      switch (url.pathname) {
        case '/chat':
          return handleChat(request, env);

        case '/generate':
          return handleGenerate(request, env);

        case '/status':
          return handleStatus(request, env);

        case '/result':
          return handleResult(request, env);

        case '/cancel':
          return handleCancel(request, env);

        case '/history':
          return handleHistory(request, env);

        case '/favorites':
          return handleFavorites(request, env);

        case '/usage':
          return handleUsage(request, env);

        case '/stats':
          return handleStats(env);

        default:
          return json({
            error: 'Not found',
            endpoints: [
              'GET  /health — Service health',
              'GET  /characters — Character catalog',
              'GET  /cost?mode=image — Cost estimation',
              'POST /chat — AI chat with custom system prompt',
              'POST /generate — Submit generation job',
              'GET  /status?job_id=X — Check job status',
              'GET  /result?job_id=X — Get completed result',
              'POST /cancel?job_id=X — Cancel job',
              'GET  /history — Generation history',
              'GET/POST/DELETE /favorites — Manage favorites',
              'GET  /usage — Usage & cost tracking',
              'GET  /stats — Global statistics',
            ],
          }, 404);
      }
    } catch (err) {
      log('error', 'Unhandled error', { error: String(err), path: url.pathname });
      return json({ error: 'Internal server error', detail: String(err) }, 500);
    }
  },
};
