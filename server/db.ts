import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import pg from 'pg';
import { attachDatabasePool } from '@vercel/functions';

type SqliteDatabaseSync = import('node:sqlite').DatabaseSync;

export interface StoredDocRow {
  collection: string;
  id: string;
  pos: number;
  data: unknown;
}

export interface StoredEventRow {
  id: number;
  event: string;
  payload: unknown;
}

export interface PendingEventWrite {
  audience: string;
  event: string;
  payload: unknown;
}

function isLocalPgHost(host: string): boolean {
  const lower = host.trim().toLowerCase();
  return (
    lower === 'localhost' ||
    lower === '127.0.0.1' ||
    lower === '::1' ||
    lower.endsWith('.local') ||
    lower.startsWith('/cloudsql/') ||
    lower.startsWith('/var/run/')
  );
}

function normalizeRawConnectionString(input?: string): string {
  if (!input) return '';
  let value = input.trim();
  // Handle accidental copy-paste like: psql 'postgresql://...'
  if (/^psql\s+/i.test(value)) {
    value = value.replace(/^psql\s+/i, '').trim();
  }
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    value = value.slice(1, -1).trim();
  }
  return value;
}

function resolvePostgresConfig(): pg.PoolConfig | null {
  const rawUrl = normalizeRawConnectionString(
    process.env.DATABASE_URL ||
      process.env.POSTGRES_URL ||
      process.env.POSTGRES_URL_NON_POOLING
  );

  const isServerless = process.env.VERCEL === '1';
  const defaultPoolMax = isServerless ? 5 : 20;
  const poolMax = Math.max(2, Math.min(50, Number(process.env.PG_POOL_MAX) || defaultPoolMax));

  if (rawUrl) {
    let cleanedUrl = rawUrl;
    let hostname = '';
    let sslMode = (process.env.PGSSLMODE || '').trim().toLowerCase();

    try {
      const parsed = new URL(rawUrl);
      hostname = parsed.hostname.toLowerCase();
      const urlSslMode = (parsed.searchParams.get('sslmode') || '').trim().toLowerCase();
      if (urlSslMode) {
        sslMode = urlSslMode;
        if (urlSslMode !== 'verify-full' && urlSslMode !== 'verify-ca' && urlSslMode !== 'disable') {
          parsed.searchParams.delete('sslmode');
          cleanedUrl = parsed.toString();
        }
      }
    } catch {
      // keep rawUrl if non-standard URI
    }

    const forceNoSsl = sslMode === 'disable';
    const remoteHost = hostname ? !isLocalPgHost(hostname) : false;
    const useSsl =
      !forceNoSsl &&
      (remoteHost || sslMode === 'require' || sslMode === 'verify-full' || sslMode === 'verify-ca');

    return {
      connectionString: cleanedUrl,
      max: poolMax,
      connectionTimeoutMillis: 6_000,
      idleTimeoutMillis: isServerless ? 10_000 : 20_000,
      query_timeout: 8_000,
      keepAlive: true,
      allowExitOnIdle: isServerless,
      ...(useSsl
        ? {
            ssl: {
              rejectUnauthorized: sslMode === 'verify-full' || sslMode === 'verify-ca',
            },
          }
        : {}),
    };
  }

  const host = process.env.PGHOST?.trim();
  const database = process.env.PGDATABASE?.trim();
  const user = process.env.PGUSER?.trim();
  const password = process.env.PGPASSWORD;

  if (host && database && user) {
    const sslMode = (process.env.PGSSLMODE || '').trim().toLowerCase();
    const forceNoSsl = sslMode === 'disable';
    const useSsl =
      !forceNoSsl &&
      (!isLocalPgHost(host) || sslMode === 'require' || sslMode === 'verify-full' || sslMode === 'verify-ca');

    return {
      host,
      port: Number(process.env.PGPORT) || 5432,
      database,
      user,
      password,
      max: poolMax,
      connectionTimeoutMillis: 6_000,
      idleTimeoutMillis: isServerless ? 10_000 : 20_000,
      query_timeout: 8_000,
      keepAlive: true,
      allowExitOnIdle: isServerless,
      ...(useSsl
        ? {
            ssl: {
              rejectUnauthorized: sslMode === 'verify-full' || sslMode === 'verify-ca',
            },
          }
        : {}),
    };
  }

  return null;
}

const pgConfig = resolvePostgresConfig();
export const isUsingPostgres = Boolean(pgConfig);
const usePostgres = isUsingPostgres;
const CAN_FAILOVER_TO_LOCAL = process.env.VERCEL !== '1';
let pgFailedOver = false;
let pgFailoverReason = '';
const failoverListeners = new Set<() => void>();

export function onDbEngineFailover(listener: () => void): () => void {
  failoverListeners.add(listener);
  return () => {
    failoverListeners.delete(listener);
  };
}

export function getIsUsingPostgres(): boolean {
  return usePostgres && Boolean(pgPool) && !pgFailedOver;
}

declare global {
  var __candyCbtPgPool: pg.Pool | undefined;
}

let pgPool: pg.Pool | null = null;
if (usePostgres && pgConfig) {
  if (!globalThis.__candyCbtPgPool) {
    globalThis.__candyCbtPgPool = new pg.Pool(pgConfig);
    globalThis.__candyCbtPgPool.on('error', (err) => {
      if (!pgFailedOver) {
        console.warn('PostgreSQL pool background notice:', classifyDbError(err).code);
      }
    });
    attachDatabasePool(globalThis.__candyCbtPgPool);
  }
  pgPool = globalThis.__candyCbtPgPool;
}

const TRANSIENT_PG_CODES = new Set([
  '40001', // serialization_failure
  '40P01', // deadlock_detected
  '53300', // too_many_connections
  '53400', // configuration_limit_exceeded
  '57P01', // admin_shutdown
  '57P02', // crash_shutdown
  '57P03', // cannot_connect_now
  '08000',
  '08003',
  '08006',
  '08001',
  '08004',
  '08P01',
  'ECONNRESET',
  'ETIMEDOUT',
  'EPIPE',
  'ECONNREFUSED',
  'EAI_AGAIN',
  'ECONNABORTED',
  'ENETUNREACH',
  'EHOSTUNREACH',
]);

function extractErrorDetails(err: unknown): { code: string; message: string } {
  if (!err || typeof err !== 'object') {
    return { code: '', message: String(err || '') };
  }
  const e = err as { code?: unknown; message?: unknown; errors?: unknown; cause?: unknown };
  let code = typeof e.code === 'string' ? e.code : '';
  let message = typeof e.message === 'string' ? e.message : '';

  if (Array.isArray(e.errors) && e.errors.length > 0) {
    const nested = e.errors.map((item) => extractErrorDetails(item));
    if (!code) {
      code = nested.find((n) => n.code)?.code || '';
    }
    if (!message) {
      message = nested.map((n) => n.message).filter(Boolean).join('; ');
    }
  }
  if ((!code || !message) && e.cause) {
    const causeDetails = extractErrorDetails(e.cause);
    if (!code) code = causeDetails.code;
    if (!message) message = causeDetails.message;
  }
  return { code, message: message || String(err) };
}

function isTransientPgError(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const { code, message } = extractErrorDetails(err);
  if (code && (TRANSIENT_PG_CODES.has(code) || code.startsWith('08'))) {
    return true;
  }
  const msg = message.toLowerCase();
  if (
    msg.includes('tenant or user not found') ||
    msg.includes('password authentication failed') ||
    msg.includes('no pg_hba.conf entry')
  ) {
    return false;
  }
  return (
    msg.includes('connection terminated') ||
    msg.includes('connection ended') ||
    msg.includes('terminating connection') ||
    msg.includes('db_termination') ||
    msg.includes('client_termination') ||
    msg.includes('max clients reached') ||
    msg.includes('maxclientsinsessionmode') ||
    msg.includes('connection pool') ||
    msg.includes('ssl connection has been closed') ||
    msg.includes('server closed the connection unexpectedly') ||
    msg.includes('timeout exceeded') ||
    msg.includes('connection timeout') ||
    msg.includes('query read timeout') ||
    msg.includes('too many clients') ||
    msg.includes('remaining connection slots') ||
    msg.includes('econnreset') ||
    msg.includes('etimedout') ||
    msg.includes('epipe') ||
    msg.includes('econnrefused') ||
    msg.includes('eai_again') ||
    msg.includes('socket hang up') ||
    msg.includes('could not connect') ||
    msg.includes('client has encountered a connection error') ||
    msg.includes('database system is starting up') ||
    msg.includes('cannot use a pool after calling end')
  );
}

function sanitizeJsonForPg(value: unknown): string {
  // PostgreSQL jsonb rejects \u0000 null characters (SQLSTATE 22P05).
  return JSON.stringify(value).replace(/\\u0000/g, '');
}

function createVercelMissingDbError(): Error & { code: string } {
  const err = new Error(
    'DATABASE_URL belum dikonfigurasi pada Environment Variables Vercel. Tambahkan DATABASE_URL PostgreSQL lalu Redeploy.'
  ) as Error & { code: string };
  err.code = 'MISSING_DATABASE_URL';
  return err;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withPgRetry<T>(fn: () => Promise<T>, attempts = 3): Promise<T> {
  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (i < attempts - 1 && isTransientPgError(err)) {
        await sleep(100 * (i + 1) + Math.floor(Math.random() * 80));
        continue;
      }
      throw err;
    }
  }
  throw lastErr;
}

const PG_SCHEMA_STATEMENTS = [
  `CREATE TABLE IF NOT EXISTS cbt_docs (
    collection text NOT NULL,
    id text NOT NULL,
    pos double precision NOT NULL DEFAULT 0,
    data jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (collection, id)
  )`,
  `CREATE INDEX IF NOT EXISTS cbt_docs_collection_pos_idx ON cbt_docs (collection, pos, id)`,
  `CREATE TABLE IF NOT EXISTS cbt_events (
    id bigserial PRIMARY KEY,
    audience text NOT NULL,
    event text NOT NULL,
    payload jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
  )`,
  `CREATE INDEX IF NOT EXISTS cbt_events_audience_id_idx ON cbt_events (audience, id)`,
  `CREATE TABLE IF NOT EXISTS cbt_presence (
    user_id text PRIMARY KEY,
    role text NOT NULL,
    seen_at timestamptz NOT NULL DEFAULT now()
  )`,
  `CREATE TABLE IF NOT EXISTS cbt_meta (
    key text PRIMARY KEY,
    value text NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS cbt_docs_student_nis_idx ON cbt_docs ((data->>'nis')) WHERE collection = 'students'`,
  `CREATE INDEX IF NOT EXISTS cbt_docs_session_student_exam_idx ON cbt_docs ((data->>'studentId'), (data->>'examId')) WHERE collection = 'sessions'`,
];

let pgSchemaReady: Promise<void> | null = null;

function ensurePgSchema(): Promise<void> {
  if (!pgPool) {
    return Promise.reject(new Error('PostgreSQL pool belum diinisialisasi.'));
  }
  if (!pgSchemaReady) {
    pgSchemaReady = withPgRetry(async () => {
      const check = await pgPool!.query<{
        docs: string | null;
        events: string | null;
        presence: string | null;
        meta: string | null;
        stu_idx: string | null;
        sess_idx: string | null;
      }>(
        `SELECT
           to_regclass('public.cbt_docs')::text AS docs,
           to_regclass('public.cbt_events')::text AS events,
           to_regclass('public.cbt_presence')::text AS presence,
           to_regclass('public.cbt_meta')::text AS meta,
           to_regclass('public.cbt_docs_student_nis_idx')::text AS stu_idx,
           to_regclass('public.cbt_docs_session_student_exam_idx')::text AS sess_idx`
      );
      const row = check.rows[0];
      if (row?.docs && row?.events && row?.presence && row?.meta && row?.stu_idx && row?.sess_idx) {
        return;
      }

      const client = await pgPool!.connect();
      try {
        for (const statement of PG_SCHEMA_STATEMENTS) {
          try {
            await client.query(statement);
          } catch (stmtErr) {
            const code = extractErrorDetails(stmtErr).code;
            // Ignore benign concurrent CREATE TABLE IF NOT EXISTS races across serverless instances.
            if (code !== '42710' && code !== '23505') {
              throw stmtErr;
            }
          }
        }
      } finally {
        client.release();
      }
    }).catch((err) => {
      pgSchemaReady = null;
      throw err;
    });
  }
  return pgSchemaReady;
}

export interface DbErrorClassification {
  code: string;
  retryable: boolean;
  userMessage: string;
  logSummary: string;
}

export function classifyDbError(err: unknown): DbErrorClassification {
  const { code: pgCode, message: extractedMsg } = extractErrorDetails(err);
  const rawMessage = (extractedMsg || 'Unknown database error').replace(
    /postgres(?:ql)?:\/\/\S+/gi,
    '[redacted]'
  );
  const lower = rawMessage.toLowerCase();

  if (pgCode === 'MISSING_DATABASE_URL') {
    return {
      code: 'DB_CONFIG_ERROR',
      retryable: false,
      userMessage: rawMessage,
      logSummary: rawMessage,
    };
  }

  if (pgCode === '53000' || lower.includes('exceeded the quota') || lower.includes('compute time quota')) {
    return {
      code: 'DB_QUOTA_EXCEEDED',
      retryable: false,
      userMessage:
        'Kuota database PostgreSQL pada DATABASE_URL sudah habis/terlampaui. Perbarui DATABASE_URL di Secrets dengan project PostgreSQL baru, lalu restart server.',
      logSummary: `PostgreSQL quota exceeded (${pgCode || '53000'}): ${rawMessage}`,
    };
  }

  if (
    pgCode === '28P01' ||
    pgCode === '28000' ||
    pgCode === '3D000' ||
    pgCode === 'ERR_INVALID_URL' ||
    lower.includes('password authentication failed') ||
    lower.includes('tenant or user not found') ||
    lower.includes('no pg_hba.conf entry') ||
    (lower.includes('database') && lower.includes('does not exist'))
  ) {
    return {
      code: 'DB_CONFIG_ERROR',
      retryable: false,
      userMessage:
        'Konfigurasi DATABASE_URL PostgreSQL tidak valid atau autentikasi ditolak. Periksa kembali DATABASE_URL di Secrets / Environment Variables.',
      logSummary: `PostgreSQL config/auth error (${pgCode || 'auth'}): ${rawMessage}`,
    };
  }

  if (pgCode && /^(22|23|42)/.test(pgCode)) {
    return {
      code: `PG_${pgCode}`,
      retryable: false,
      userMessage: `Terjadi kesalahan data/skema database (${pgCode}). Silakan periksa input lalu coba kembali.`,
      logSummary: `PostgreSQL data/schema error (${pgCode}): ${rawMessage}`,
    };
  }

  return {
    code: 'DB_UNAVAILABLE',
    retryable: true,
    userMessage: 'Database CBT tidak dapat dihubungi. Coba beberapa saat lagi.',
    logSummary: `${pgCode ? `[${pgCode}] ` : ''}${rawMessage}`,
  };
}

function shouldFailoverPgError(err: unknown): boolean {
  if (!CAN_FAILOVER_TO_LOCAL) return false;
  const info = classifyDbError(err);
  if (info.code === 'DB_QUOTA_EXCEEDED' || info.code === 'DB_CONFIG_ERROR') {
    return true;
  }
  const { code: pgCode } = extractErrorDetails(err);
  // Do not silently hide SQL data/syntax bugs (22xxx, 23xxx, 42xxx),
  // but fail over cleanly on any connection, network, quota, or server availability error.
  if (pgCode && /^(22|23|42)/.test(pgCode)) {
    return false;
  }
  return info.code === 'DB_UNAVAILABLE';
}

function activatePgFailover(err: unknown): void {
  if (pgFailedOver || !CAN_FAILOVER_TO_LOCAL) return;
  const info = classifyDbError(err);
  pgFailedOver = true;
  pgFailoverReason = info.logSummary;
  console.info(
    `[CBT DB] Menggunakan penyimpanan lokal SQLite (fallback aktif dari PostgreSQL: ${info.code}).`
  );
  if (pgPool) {
    const poolToClose = pgPool;
    globalThis.__candyCbtPgPool = undefined;
    poolToClose.end().catch(() => undefined);
  }
  for (const listener of failoverListeners) {
    try {
      listener();
    } catch {
      // ignore listener error
    }
  }
}

export async function dbCheckHealth(): Promise<{
  ok: boolean;
  engine: 'postgresql' | 'sqlite';
  code?: string;
  message?: string;
  failoverFromPostgres?: boolean;
}> {
  if (!usePostgres && !CAN_FAILOVER_TO_LOCAL) {
    const err = createVercelMissingDbError();
    return {
      ok: false,
      engine: 'postgresql',
      code: err.code,
      message: err.message,
    };
  }
  if (getIsUsingPostgres() && pgPool) {
    try {
      await ensurePgSchema();
      await pgPool.query('SELECT 1');
      return { ok: true, engine: 'postgresql' };
    } catch (err) {
      if (pgFailedOver || shouldFailoverPgError(err)) {
        activatePgFailover(err);
      } else {
        const info = classifyDbError(err);
        return {
          ok: false,
          engine: 'postgresql',
          code: info.code,
          message: info.userMessage,
        };
      }
    }
  }
  try {
    const db = await getSqliteDb();
    db.prepare('SELECT 1').get();
    return {
      ok: true,
      engine: 'sqlite',
      ...(pgFailedOver ? { failoverFromPostgres: true, message: pgFailoverReason } : {}),
    };
  } catch (err) {
    const e = err as { code?: string; message?: string };
    return { ok: false, engine: 'sqlite', code: e?.code, message: e?.message || String(err) };
  }
}

// Lazy SQLite fallback loader — only imported when PostgreSQL is not configured
// or during one-time legacy SQLite -> PostgreSQL migration.
let sqliteModulePromise: Promise<typeof import('node:sqlite')> | null = null;
function loadSqliteModule(): Promise<typeof import('node:sqlite')> {
  if (!sqliteModulePromise) {
    sqliteModulePromise = import('node:sqlite');
  }
  return sqliteModulePromise;
}

let sqliteDb: SqliteDatabaseSync | null = null;
let sqliteDbPromise: Promise<SqliteDatabaseSync> | null = null;

const SQLITE_SCHEMA_SQL = `
  CREATE TABLE IF NOT EXISTS cbt_docs (
    collection TEXT NOT NULL,
    id TEXT NOT NULL,
    pos REAL NOT NULL DEFAULT 0,
    data TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (collection, id)
  );
  CREATE TABLE IF NOT EXISTS cbt_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    audience TEXT NOT NULL,
    event TEXT NOT NULL,
    payload TEXT,
    created_at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS cbt_events_audience_id_idx ON cbt_events (audience, id);
  CREATE TABLE IF NOT EXISTS cbt_presence (
    user_id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    seen_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS cbt_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );
`;

function resolveWritableDbCandidates(): string[] {
  const paths: string[] = [];
  const dirs = [
    path.resolve(process.cwd(), 'data'),
    path.join(os.tmpdir(), 'candycbt-data'),
  ];
  const probeSuffix = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  for (const dir of dirs) {
    try {
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      const probe = path.join(dir, `.write-check-${probeSuffix}`);
      fs.writeFileSync(probe, 'ok');
      fs.unlinkSync(probe);
      paths.push(path.join(dir, 'candycbt-runtime.db'));
    } catch {
      // ignore unwritable directory
    }
  }
  paths.push(':memory:');
  return paths;
}

function tryOpenSqlite(
  DatabaseSyncCtor: typeof import('node:sqlite').DatabaseSync,
  dbPath: string,
  journalMode: 'WAL' | 'DELETE'
): SqliteDatabaseSync {
  const db = new DatabaseSyncCtor(dbPath);
  try {
    db.exec(`PRAGMA busy_timeout = 10000;`);
    if (dbPath !== ':memory:') {
      db.exec(`PRAGMA journal_mode = ${journalMode};`);
    }
    db.exec(`PRAGMA synchronous = NORMAL;`);
    db.exec(SQLITE_SCHEMA_SQL);
    // Verify real write capability so read-only files or broken WAL sidecars are rejected immediately.
    db.exec('BEGIN IMMEDIATE');
    try {
      db.prepare(
        `INSERT INTO cbt_meta (key, value) VALUES ('_write_probe', ?)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value`
      ).run(String(Date.now()));
      db.exec('COMMIT');
    } catch (writeErr) {
      try {
        db.exec('ROLLBACK');
      } catch {
        // ignore rollback failure
      }
      throw writeErr;
    }
    return db;
  } catch (err) {
    try {
      db.close();
    } catch {
      // ignore close failure
    }
    throw err;
  }
}

async function getSqliteDb(): Promise<SqliteDatabaseSync> {
  if (!usePostgres && !CAN_FAILOVER_TO_LOCAL) {
    throw createVercelMissingDbError();
  }
  if (sqliteDb) {
    return sqliteDb;
  }
  if (!sqliteDbPromise) {
    sqliteDbPromise = (async () => {
      const { DatabaseSync } = await loadSqliteModule();
      const candidates = resolveWritableDbCandidates();
      const primaryDbPath = path.resolve(process.cwd(), 'data', 'candycbt-runtime.db');
      let lastError: unknown;
      for (const dbPath of candidates) {
        // If falling back away from a read-only primary SQLite file to tmpdir, copy it if destination doesn't exist yet.
        if (
          dbPath !== primaryDbPath &&
          dbPath !== ':memory:' &&
          !fs.existsSync(dbPath) &&
          fs.existsSync(primaryDbPath)
        ) {
          try {
            fs.copyFileSync(primaryDbPath, dbPath);
          } catch {
            // ignore copy failure and start fresh
          }
        }
        for (const mode of (dbPath === ':memory:' ? ['DELETE'] : ['WAL', 'DELETE']) as ('WAL' | 'DELETE')[]) {
          try {
            sqliteDb = tryOpenSqlite(DatabaseSync, dbPath, mode);
            return sqliteDb;
          } catch (err) {
            lastError = err;
            console.warn(`Gagal membuka SQLite (${dbPath}, mode ${mode}):`, err);
          }
        }
      }
      throw lastError instanceof Error
        ? lastError
        : new Error('Gagal menginisialisasi database fallback lokal.');
    })().catch((err) => {
      sqliteDbPromise = null;
      throw err;
    });
  }
  return sqliteDbPromise;
}

function isSqliteBusyError(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const e = err as { code?: string; errcode?: number; message?: string };
  const msg = (e.message || '').toLowerCase();
  return (
    e.errcode === 5 ||
    e.errcode === 6 ||
    (e.code === 'ERR_SQLITE_ERROR' && (msg.includes('locked') || msg.includes('busy'))) ||
    msg.includes('database is locked') ||
    msg.includes('sqlite_busy')
  );
}

async function withSqliteRetry<T>(fn: (db: SqliteDatabaseSync) => T, attempts = 5): Promise<T> {
  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      const db = await getSqliteDb();
      return fn(db);
    } catch (err) {
      lastErr = err;
      if (i < attempts - 1 && isSqliteBusyError(err)) {
        await sleep(35 * (i + 1) + Math.floor(Math.random() * 25));
        continue;
      }
      throw err;
    }
  }
  throw lastErr;
}

function findExistingLegacySqlitePath(): string | null {
  const candidates = [
    path.resolve(process.cwd(), 'data', 'candycbt-runtime.db'),
    path.join(os.tmpdir(), 'candycbt-data', 'candycbt-runtime.db'),
  ];
  for (const candidate of candidates) {
    try {
      if (fs.existsSync(candidate) && fs.statSync(candidate).size > 0) {
        return candidate;
      }
    } catch {
      // ignore inaccessible candidate
    }
  }
  return null;
}

async function readLegacySqliteSnapshot(): Promise<{
  docs: StoredDocRow[];
  sessionSecret?: string;
} | null> {
  const sqliteFile = findExistingLegacySqlitePath();
  if (!sqliteFile) return null;

  try {
    const { DatabaseSync } = await loadSqliteModule();
    const legacyDb = new DatabaseSync(sqliteFile, { readOnly: true });
    try {
      const hasDocsTable = legacyDb
        .prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='cbt_docs'`)
        .get() as { name?: string } | undefined;
      if (!hasDocsTable?.name) {
        return null;
      }

      const rawDocs = legacyDb
        .prepare('SELECT collection, id, pos, data FROM cbt_docs ORDER BY collection, pos, id')
        .all() as { collection: string; id: string; pos: number; data: string }[];

      const docs: StoredDocRow[] = [];
      for (const row of rawDocs) {
        try {
          docs.push({
            collection: row.collection,
            id: row.id,
            pos: Number(row.pos),
            data: JSON.parse(row.data),
          });
        } catch {
          // skip malformed legacy row
        }
      }

      let sessionSecret: string | undefined;
      try {
        const secretRow = legacyDb
          .prepare(`SELECT value FROM cbt_meta WHERE key = 'session_secret'`)
          .get() as { value?: string } | undefined;
        sessionSecret = secretRow?.value;
      } catch {
        // ignore missing cbt_meta
      }

      return { docs, sessionSecret };
    } finally {
      legacyDb.close();
    }
  } catch (err) {
    console.warn('Melewati pembacaan migrasi SQLite lama:', err);
    return null;
  }
}

export async function dbReadLocalSessionSecret(): Promise<string | null> {
  const sqliteFile = findExistingLegacySqlitePath();
  if (!sqliteFile) return null;
  try {
    const db = await getSqliteDb();
    const row = db.prepare(`SELECT value FROM cbt_meta WHERE key = 'session_secret'`).get() as
      | { value?: string }
      | undefined;
    return row?.value && row.value.length >= 16 ? row.value : null;
  } catch {
    return null;
  }
}

export async function dbInitSessionSecret(candidate: string): Promise<string> {
  if (getIsUsingPostgres() && pgPool) {
    try {
      return await withPgRetry(async () => {
        await ensurePgSchema();
        const existing = await pgPool!.query<{ value: string }>(
          `SELECT value FROM cbt_meta WHERE key = 'session_secret'`
        );
        if (existing.rows[0]?.value) {
          return existing.rows[0].value;
        }

        let initialSecret = candidate;
        const legacy = await readLegacySqliteSnapshot();
        if (legacy?.sessionSecret && legacy.sessionSecret.length >= 16) {
          initialSecret = legacy.sessionSecret;
        }

        await pgPool!.query(
          `INSERT INTO cbt_meta (key, value) VALUES ('session_secret', $1) ON CONFLICT (key) DO NOTHING`,
          [initialSecret]
        );
        const { rows } = await pgPool!.query<{ value: string }>(
          `SELECT value FROM cbt_meta WHERE key = 'session_secret'`
        );
        return rows[0]?.value || initialSecret;
      });
    } catch (err) {
      if (pgFailedOver || shouldFailoverPgError(err)) {
        activatePgFailover(err);
      } else {
        throw err;
      }
    }
  }

  return withSqliteRetry((db) => {
    db.prepare(`INSERT OR IGNORE INTO cbt_meta (key, value) VALUES ('session_secret', ?)`).run(candidate);
    const row = db.prepare(`SELECT value FROM cbt_meta WHERE key = 'session_secret'`).get() as
      | { value?: string }
      | undefined;
    return row?.value || candidate;
  });
}

export async function dbTouchPresence(userId: string, role: string): Promise<void> {
  if (getIsUsingPostgres() && pgPool) {
    try {
      await withPgRetry(async () => {
        await ensurePgSchema();
        await pgPool!.query(
          `INSERT INTO cbt_presence (user_id, role, seen_at) VALUES ($1, $2, now())
           ON CONFLICT (user_id) DO UPDATE SET role = EXCLUDED.role, seen_at = now()`,
          [userId, role]
        );
      });
      return;
    } catch (err) {
      if (pgFailedOver || shouldFailoverPgError(err)) {
        activatePgFailover(err);
      } else {
        throw err;
      }
    }
  }

  await withSqliteRetry((db) => {
    db.prepare(
      `INSERT INTO cbt_presence (user_id, role, seen_at) VALUES (?, ?, ?)
       ON CONFLICT (user_id) DO UPDATE SET role = excluded.role, seen_at = excluded.seen_at`
    ).run(userId, role, Date.now());
  });
}

export async function dbGetDocsVersion(): Promise<number> {
  if (getIsUsingPostgres() && pgPool) {
    try {
      return await withPgRetry(async () => {
        await ensurePgSchema();
        const { rows } = await pgPool!.query<{ value: string }>(
          `SELECT value FROM cbt_meta WHERE key = 'docs_version'`
        );
        return Number(rows[0]?.value || 0);
      });
    } catch (err) {
      if (pgFailedOver || shouldFailoverPgError(err)) {
        activatePgFailover(err);
      } else {
        throw err;
      }
    }
  }
  return 0;
}

export async function dbLoadPresence(): Promise<{ students: number; staff: number }> {
  if (getIsUsingPostgres() && pgPool) {
    try {
      return await withPgRetry(async () => {
        await ensurePgSchema();
        const { rows } = await pgPool!.query<{ role: string; n: number }>(
          `SELECT role, count(*)::int AS n FROM cbt_presence WHERE seen_at > now() - interval '75 seconds' GROUP BY role`
        );
        const presence = { students: 0, staff: 0 };
        for (const row of rows) {
          if (row.role === 'student') presence.students += row.n;
          else presence.staff += row.n;
        }
        return presence;
      });
    } catch (err) {
      if (pgFailedOver || shouldFailoverPgError(err)) {
        activatePgFailover(err);
      } else {
        throw err;
      }
    }
  }

  return withSqliteRetry((db) => {
    const cutoff = Date.now() - 75_000;
    const presenceRows = db
      .prepare('SELECT role, count(*) AS n FROM cbt_presence WHERE seen_at > ? GROUP BY role')
      .all(cutoff) as { role: string; n: number }[];

    const presence = { students: 0, staff: 0 };
    for (const row of presenceRows) {
      if (row.role === 'student') presence.students += Number(row.n) || 0;
      else presence.staff += Number(row.n) || 0;
    }
    return presence;
  });
}

export async function dbLoadDocsAndPresence(): Promise<{
  docs: StoredDocRow[];
  presence: { students: number; staff: number };
  version: number;
}> {
  if (getIsUsingPostgres() && pgPool) {
    try {
      return await withPgRetry(async () => {
        await ensurePgSchema();
        const [docsRes, presenceRes, versionRes] = await Promise.all([
          pgPool!.query<StoredDocRow>(
            'SELECT collection, id, pos, data FROM cbt_docs ORDER BY collection, pos, id'
          ),
          pgPool!.query<{ role: string; n: number }>(
            `SELECT role, count(*)::int AS n FROM cbt_presence WHERE seen_at > now() - interval '75 seconds' GROUP BY role`
          ),
          pgPool!.query<{ value: string }>(
            `SELECT value FROM cbt_meta WHERE key = 'docs_version'`
          ),
        ]);
        const presence = { students: 0, staff: 0 };
        for (const row of presenceRes.rows) {
          if (row.role === 'student') presence.students += row.n;
          else presence.staff += row.n;
        }
        let version = Number(versionRes.rows[0]?.value || 0);

        if (docsRes.rows.length === 0) {
          const legacy = await readLegacySqliteSnapshot();
          if (legacy && legacy.docs.length > 0) {
            await pgPool!.query(
              `INSERT INTO cbt_docs (collection, id, pos, data)
               SELECT x.collection, x.id, x.pos, x.data
               FROM jsonb_to_recordset($1::jsonb) AS x(collection text, id text, pos double precision, data jsonb)
               ON CONFLICT (collection, id) DO NOTHING`,
              [sanitizeJsonForPg(legacy.docs)]
            );
            const vBump = await pgPool!.query<{ value: string }>(
              `INSERT INTO cbt_meta (key, value) VALUES ('docs_version', '1')
               ON CONFLICT (key) DO UPDATE SET value = ((CASE WHEN cbt_meta.value ~ '^[0-9]+$' THEN cbt_meta.value::bigint ELSE 0 END) + 1)::text
               RETURNING value`
            );
            version = Number(vBump.rows[0]?.value || 1);
            const migratedRes = await pgPool!.query<StoredDocRow>(
              'SELECT collection, id, pos, data FROM cbt_docs ORDER BY collection, pos, id'
            );
            return { docs: migratedRes.rows, presence, version };
          }
        }

        return { docs: docsRes.rows, presence, version };
      });
    } catch (err) {
      if (pgFailedOver || shouldFailoverPgError(err)) {
        activatePgFailover(err);
      } else {
        throw err;
      }
    }
  }

  return withSqliteRetry((db) => {
    const docRows = db
      .prepare('SELECT collection, id, pos, data FROM cbt_docs ORDER BY collection, pos, id')
      .all() as { collection: string; id: string; pos: number; data: string }[];
    const cutoff = Date.now() - 75_000;
    const presenceRows = db
      .prepare('SELECT role, count(*) AS n FROM cbt_presence WHERE seen_at > ? GROUP BY role')
      .all(cutoff) as { role: string; n: number }[];

    const presence = { students: 0, staff: 0 };
    for (const row of presenceRows) {
      if (row.role === 'student') presence.students += Number(row.n) || 0;
      else presence.staff += Number(row.n) || 0;
    }

    const docs: StoredDocRow[] = [];
    for (const row of docRows) {
      try {
        docs.push({
          collection: row.collection,
          id: row.id,
          pos: Number(row.pos),
          data: JSON.parse(row.data),
        });
      } catch (err) {
        console.error(`Melewati dokumen korup (${row.collection}/${row.id}):`, err);
      }
    }

    return { docs, presence, version: 0 };
  });
}

export async function dbFlushChanges(
  upserts: StoredDocRow[],
  deletes: { collection: string; id: string }[],
  events: PendingEventWrite[]
): Promise<number | null> {
  if (getIsUsingPostgres() && pgPool) {
    try {
      return await withPgRetry(async () => {
        await ensurePgSchema();
        const hasDocChanges = upserts.length > 0 || deletes.length > 0;
        const upsertsJson = upserts.length > 0 ? sanitizeJsonForPg(upserts) : '[]';
        const deletesJson = deletes.length > 0 ? sanitizeJsonForPg(deletes) : '[]';
        const eventsJson =
          events.length > 0
            ? sanitizeJsonForPg(events.map((e) => ({ ...e, payload: e.payload ?? null })))
            : '[]';

        const { rows } = await pgPool!.query<{ version: string | null }>(
          `WITH
             u AS (
               INSERT INTO cbt_docs (collection, id, pos, data)
               SELECT x.collection, x.id, x.pos, x.data
               FROM jsonb_to_recordset($1::jsonb) AS x(collection text, id text, pos double precision, data jsonb)
               ON CONFLICT (collection, id) DO UPDATE
               SET pos = EXCLUDED.pos, data = EXCLUDED.data, updated_at = now()
               RETURNING 1
             ),
             d AS (
               DELETE FROM cbt_docs doc
               USING jsonb_to_recordset($2::jsonb) AS x(collection text, id text)
               WHERE doc.collection = x.collection AND doc.id = x.id
               RETURNING 1
             ),
             e AS (
               INSERT INTO cbt_events (audience, event, payload)
               SELECT x.audience, x.event, x.payload
               FROM jsonb_to_recordset($3::jsonb) AS x(audience text, event text, payload jsonb)
               RETURNING 1
             ),
             v AS (
               INSERT INTO cbt_meta (key, value)
               SELECT 'docs_version', '1'
               WHERE $4::boolean
               ON CONFLICT (key) DO UPDATE
               SET value = ((CASE WHEN cbt_meta.value ~ '^[0-9]+$' THEN cbt_meta.value::bigint ELSE 0 END) + 1)::text
               RETURNING value
             )
           SELECT
             (SELECT count(*) FROM u) AS upserted,
             (SELECT count(*) FROM d) AS deleted,
             (SELECT count(*) FROM e) AS emitted,
             (SELECT value FROM v LIMIT 1) AS version`,
          [upsertsJson, deletesJson, eventsJson, hasDocChanges]
        );
        const rawVer = rows[0]?.version;
        return rawVer !== null && rawVer !== undefined ? Number(rawVer) : null;
      });
    } catch (err) {
      if (pgFailedOver || shouldFailoverPgError(err)) {
        activatePgFailover(err);
      } else {
        throw err;
      }
    }
  }

  await withSqliteRetry((db) => {
    const now = Date.now();
    const upsertStmt = db.prepare(
      `INSERT INTO cbt_docs (collection, id, pos, data, updated_at)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (collection, id) DO UPDATE
       SET pos = excluded.pos, data = excluded.data, updated_at = excluded.updated_at`
    );
    const deleteStmt = db.prepare(`DELETE FROM cbt_docs WHERE collection = ? AND id = ?`);
    const insertEventStmt = db.prepare(
      `INSERT INTO cbt_events (audience, event, payload, created_at) VALUES (?, ?, ?, ?)`
    );

    db.exec('BEGIN IMMEDIATE');
    try {
      for (const item of upserts) {
        upsertStmt.run(item.collection, item.id, item.pos, JSON.stringify(item.data), now);
      }
      for (const del of deletes) {
        deleteStmt.run(del.collection, del.id);
      }
      for (const ev of events) {
        insertEventStmt.run(ev.audience, ev.event, JSON.stringify(ev.payload ?? null), now);
      }
      db.exec('COMMIT');
    } catch (err) {
      try {
        db.exec('ROLLBACK');
      } catch {
        // ignore rollback failure
      }
      throw err;
    }
  });
  return null;
}

export function dbPurgeOldEvents(): void {
  if (getIsUsingPostgres() && pgPool) {
    pgPool
      .query(`DELETE FROM cbt_events WHERE created_at < now() - interval '30 minutes'`)
      .catch((err) => console.error('Gagal membersihkan cbt_events:', err));
    pgPool
      .query(`DELETE FROM cbt_presence WHERE seen_at < now() - interval '5 minutes'`)
      .catch(() => undefined);
    return;
  }

  getSqliteDb()
    .then((db) => {
      const eventCutoff = Date.now() - 30 * 60 * 1000;
      const presenceCutoff = Date.now() - 5 * 60 * 1000;
      db.prepare(`DELETE FROM cbt_events WHERE created_at < ?`).run(eventCutoff);
      db.prepare(`DELETE FROM cbt_presence WHERE seen_at < ?`).run(presenceCutoff);
      try {
        db.exec(`PRAGMA wal_checkpoint(PASSIVE)`);
      } catch {
        // ignore when not in WAL mode
      }
    })
    .catch((err) => {
      console.error('Gagal membersihkan cbt_events:', err);
    });
}

export async function dbGetMaxEventId(): Promise<number> {
  if (getIsUsingPostgres() && pgPool) {
    try {
      return await withPgRetry(async () => {
        await ensurePgSchema();
        const { rows } = await pgPool!.query<{ max: string | null }>('SELECT max(id) AS max FROM cbt_events');
        return Number(rows[0]?.max || 0);
      });
    } catch (err) {
      if (pgFailedOver || shouldFailoverPgError(err)) {
        activatePgFailover(err);
      } else {
        throw err;
      }
    }
  }

  return withSqliteRetry((db) => {
    const row = db.prepare('SELECT max(id) AS max FROM cbt_events').get() as
      | { max?: number | null }
      | undefined;
    return Number(row?.max || 0);
  });
}

export async function dbGetEventsAfter(
  afterId: number,
  audiences: string[]
): Promise<StoredEventRow[]> {
  const res = await dbPollEventsAfter(afterId, audiences);
  return res.events;
}

export async function dbPollEventsAfter(
  afterId: number,
  audiences: string[]
): Promise<{ cursor: number; events: StoredEventRow[] }> {
  if (!Number.isFinite(afterId) || afterId < 0) {
    const maxId = await dbGetMaxEventId();
    return { cursor: maxId, events: [] };
  }

  if (audiences.length === 0) {
    return { cursor: afterId, events: [] };
  }

  if (getIsUsingPostgres() && pgPool) {
    try {
      return await withPgRetry(async () => {
        await ensurePgSchema();
        const { rows } = await pgPool!.query<{
          max_id: string;
          id: string | null;
          event: string | null;
          payload: unknown;
        }>(
          `WITH latest AS (SELECT COALESCE(max(id), 0) AS max_id FROM cbt_events)
           SELECT l.max_id, e.id, e.event, e.payload
           FROM latest l
           LEFT JOIN LATERAL (
             SELECT id, event, payload
             FROM cbt_events
             WHERE id > $1 AND audience = ANY($2::text[])
             ORDER BY id ASC
             LIMIT 200
           ) e ON true
           ORDER BY e.id ASC NULLS LAST`,
          [afterId, audiences]
        );

        let cursor = afterId;
        const events: StoredEventRow[] = [];
        for (const r of rows) {
          cursor = Math.max(cursor, Number(r.max_id || 0));
          if (r.id !== null && r.event !== null) {
            const evId = Number(r.id);
            cursor = Math.max(cursor, evId);
            events.push({ id: evId, event: r.event, payload: r.payload });
          }
        }
        return { cursor, events };
      });
    } catch (err) {
      if (pgFailedOver || shouldFailoverPgError(err)) {
        activatePgFailover(err);
      } else {
        throw err;
      }
    }
  }

  return withSqliteRetry((db) => {
    const placeholders = audiences.map(() => '?').join(', ');
    const rows = db
      .prepare(
        `SELECT id, event, payload FROM cbt_events
         WHERE id > ? AND audience IN (${placeholders})
         ORDER BY id ASC LIMIT 200`
      )
      .all(afterId, ...audiences) as { id: number; event: string; payload: string | null }[];

    const maxRow = db.prepare('SELECT max(id) AS max FROM cbt_events').get() as
      | { max?: number | null }
      | undefined;

    let cursor = Math.max(afterId, Number(maxRow?.max || 0));
    const events = rows.map((r) => {
      let payload: unknown = null;
      if (r.payload) {
        try {
          payload = JSON.parse(r.payload);
        } catch {
          payload = null;
        }
      }
      const evId = Number(r.id);
      cursor = Math.max(cursor, evId);
      return {
        id: evId,
        event: r.event,
        payload,
      };
    });
    return { cursor, events };
  });
}

// ---------- POSTGRESQL FAST-PATH PER-DOCUMENT OPERATIONS ----------
// These bypass full-store loading and use atomic PostgreSQL jsonb updates
// so 36+ concurrent students never overwrite each other's answers or block on
// downloading unrelated collections.

export async function dbPgGetPublicInfo(): Promise<{
  settings: Record<string, unknown>;
  adminCount: number;
  sampleStudents: Record<string, unknown>[];
  sampleTeachers: Record<string, unknown>[];
} | null> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      const { rows } = await pgPool!.query<{
        settings: Record<string, unknown> | null;
        admin_count: number;
        sample_students: Record<string, unknown>[];
        sample_teachers: Record<string, unknown>[];
      }>(
        `SELECT
           (SELECT data FROM cbt_docs WHERE collection = 'settings' AND id = 'main' LIMIT 1) AS settings,
           (SELECT count(*)::int FROM cbt_docs WHERE collection = 'admins') AS admin_count,
           (SELECT COALESCE(jsonb_agg(d.data), '[]'::jsonb) FROM (
             SELECT data FROM cbt_docs WHERE collection = 'students' ORDER BY pos, id LIMIT 4
           ) d) AS sample_students,
           (SELECT COALESCE(jsonb_agg(d.data), '[]'::jsonb) FROM (
             SELECT data FROM cbt_docs
             WHERE collection = 'teachers' AND COALESCE((data->>'isActive')::boolean, true) = true
             ORDER BY pos, id LIMIT 4
           ) d) AS sample_teachers`
      );
      const row = rows[0];
      if (!row?.settings) {
        return null;
      }
      return {
        settings: row.settings,
        adminCount: Number(row.admin_count || 0),
        sampleStudents: Array.isArray(row.sample_students) ? row.sample_students : [],
        sampleTeachers: Array.isArray(row.sample_teachers) ? row.sample_teachers : [],
      };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

export async function dbPgStudentLoginLookup(nis: string): Promise<{
  maintenanceMode: boolean;
  student: Record<string, unknown> | null;
} | null> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      const { rows } = await pgPool!.query<{
        has_settings: boolean;
        maintenance_mode: boolean;
        student_count: number;
        student_data: Record<string, unknown> | null;
      }>(
        `SELECT
           EXISTS(SELECT 1 FROM cbt_docs WHERE collection = 'settings' AND id = 'main') AS has_settings,
           COALESCE((SELECT (data->>'maintenanceMode')::boolean FROM cbt_docs WHERE collection = 'settings' AND id = 'main' LIMIT 1), false) AS maintenance_mode,
           (SELECT count(*)::int FROM cbt_docs WHERE collection = 'students') AS student_count,
           (SELECT data FROM cbt_docs WHERE collection = 'students' AND data->>'nis' = $1 LIMIT 1) AS student_data`,
        [nis]
      );
      const row = rows[0];
      if (!row?.has_settings || Number(row.student_count || 0) === 0) {
        return null;
      }
      return {
        maintenanceMode: Boolean(row.maintenance_mode),
        student: row.student_data || null,
      };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

export async function dbPgFindDocById<T = Record<string, unknown>>(
  collection: string,
  id: string
): Promise<{ found: true; data: T } | { found: false } | null> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      const { rows } = await pgPool!.query<{ data: T }>(
        `SELECT data FROM cbt_docs WHERE collection = $1 AND id = $2 LIMIT 1`,
        [collection, id]
      );
      if (rows.length === 0) {
        return { found: false as const };
      }
      return { found: true as const, data: rows[0].data };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

export async function dbPgGetStudentExamsContext(studentId: string): Promise<{
  maintenanceMode: boolean;
  student: Record<string, unknown> | null;
  exams: Record<string, unknown>[];
  banksMeta: { id: string; code: string; kkm: number; questionCount: number }[];
  sessions: Record<string, unknown>[];
  broadcasts: Record<string, unknown>[];
} | null> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      const { rows } = await pgPool!.query<{
        has_settings: boolean;
        maintenance_mode: boolean;
        student_data: Record<string, unknown> | null;
        exams: Record<string, unknown>[];
        banks_meta: { id: string; code: string; kkm: number; questionCount: number }[];
        sessions: Record<string, unknown>[];
        broadcasts: Record<string, unknown>[];
      }>(
        `SELECT
           EXISTS(SELECT 1 FROM cbt_docs WHERE collection = 'settings' AND id = 'main') AS has_settings,
           COALESCE((SELECT (data->>'maintenanceMode')::boolean FROM cbt_docs WHERE collection = 'settings' AND id = 'main' LIMIT 1), false) AS maintenance_mode,
           (SELECT data FROM cbt_docs WHERE collection = 'students' AND id = $1 LIMIT 1) AS student_data,
           (SELECT COALESCE(jsonb_agg(data ORDER BY pos, id), '[]'::jsonb) FROM cbt_docs WHERE collection = 'exams') AS exams,
           (SELECT COALESCE(jsonb_agg(jsonb_build_object(
             'id', data->>'id',
             'code', COALESCE(data->>'code', '-'),
             'kkm', CASE WHEN (data->>'kkm') ~ '^[0-9]+(\\.[0-9]+)?$' THEN (data->>'kkm')::numeric ELSE 75 END,
             'questionCount', jsonb_array_length(COALESCE(data->'questions', '[]'::jsonb))
           )), '[]'::jsonb) FROM cbt_docs WHERE collection = 'banks') AS banks_meta,
           (SELECT COALESCE(jsonb_agg(data), '[]'::jsonb) FROM cbt_docs WHERE collection = 'sessions' AND data->>'studentId' = $1) AS sessions,
           (SELECT COALESCE(jsonb_agg(d.data), '[]'::jsonb) FROM (
             SELECT data FROM cbt_docs WHERE collection = 'broadcasts' ORDER BY pos, id LIMIT 5
           ) d) AS broadcasts`,
        [studentId]
      );
      const row = rows[0];
      if (!row?.has_settings) return null;
      return {
        maintenanceMode: Boolean(row.maintenance_mode),
        student: row.student_data || null,
        exams: Array.isArray(row.exams) ? row.exams : [],
        banksMeta: Array.isArray(row.banks_meta) ? row.banks_meta : [],
        sessions: Array.isArray(row.sessions) ? row.sessions : [],
        broadcasts: Array.isArray(row.broadcasts) ? row.broadcasts : [],
      };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

export async function dbPgGetStudentMaterialsContext(studentId: string): Promise<{
  student: Record<string, unknown> | null;
  materials: Record<string, unknown>[];
} | null> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      const { rows } = await pgPool!.query<{
        student_data: Record<string, unknown> | null;
        materials: Record<string, unknown>[];
      }>(
        `SELECT
           (SELECT data FROM cbt_docs WHERE collection = 'students' AND id = $1 LIMIT 1) AS student_data,
           (SELECT COALESCE(jsonb_agg(data ORDER BY pos, id), '[]'::jsonb)
            FROM cbt_docs
            WHERE collection = 'materials' AND COALESCE((data->>'isPublished')::boolean, false) = true) AS materials`,
        [studentId]
      );
      const row = rows[0];
      return {
        student: row?.student_data || null,
        materials: Array.isArray(row?.materials) ? row.materials : [],
      };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

export async function dbPgGetStartExamContext(
  studentId: string,
  examId: string
): Promise<{
  maintenanceMode: boolean;
  student: Record<string, unknown> | null;
  exam: Record<string, unknown> | null;
  bank: Record<string, unknown> | null;
  session: Record<string, unknown> | null;
} | null> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      const { rows } = await pgPool!.query<{
        has_settings: boolean;
        maintenance_mode: boolean;
        student_data: Record<string, unknown> | null;
        exam_data: Record<string, unknown> | null;
        bank_data: Record<string, unknown> | null;
        session_data: Record<string, unknown> | null;
      }>(
        `SELECT
           EXISTS(SELECT 1 FROM cbt_docs WHERE collection = 'settings' AND id = 'main') AS has_settings,
           COALESCE((SELECT (data->>'maintenanceMode')::boolean FROM cbt_docs WHERE collection = 'settings' AND id = 'main' LIMIT 1), false) AS maintenance_mode,
           (SELECT data FROM cbt_docs WHERE collection = 'students' AND id = $1 LIMIT 1) AS student_data,
           (SELECT data FROM cbt_docs WHERE collection = 'exams' AND id = $2 LIMIT 1) AS exam_data,
           (SELECT b.data FROM cbt_docs b
            JOIN cbt_docs e ON e.collection = 'exams' AND e.id = $2
            WHERE b.collection = 'banks' AND b.id = e.data->>'bankId'
            LIMIT 1) AS bank_data,
           (SELECT data FROM cbt_docs WHERE collection = 'sessions' AND data->>'studentId' = $1 AND data->>'examId' = $2 LIMIT 1) AS session_data`,
        [studentId, examId]
      );
      const row = rows[0];
      if (!row?.has_settings) return null;
      return {
        maintenanceMode: Boolean(row.maintenance_mode),
        student: row.student_data || null,
        exam: row.exam_data || null,
        bank: row.bank_data || null,
        session: row.session_data || null,
      };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

export async function dbPgInsertOrResumeSession(params: {
  newSession?: Record<string, unknown>;
  resumeSessionId?: string;
  studentId: string;
  examId: string;
  nowIso: string;
}): Promise<{ session: Record<string, unknown>; version: number | null } | null> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      if (params.newSession) {
        const sessionJson = sanitizeJsonForPg(params.newSession);
        const sessionId = String(params.newSession.id);
        const client = await pgPool!.connect();
        try {
          await client.query('BEGIN');
          await client.query(`SELECT pg_advisory_xact_lock(hashtext($1))`, [
            `sess:${params.studentId}:${params.examId}`,
          ]);
          const { rows } = await client.query<{
            session_data: Record<string, unknown> | null;
            version: string | null;
          }>(
            `WITH ins AS (
               INSERT INTO cbt_docs (collection, id, pos, data)
               SELECT 'sessions', $1, extract(epoch from now()), $2::jsonb
               WHERE NOT EXISTS (
                 SELECT 1 FROM cbt_docs WHERE collection = 'sessions' AND data->>'studentId' = $3 AND data->>'examId' = $4
               )
               ON CONFLICT (collection, id) DO NOTHING
               RETURNING data
             ),
             ev AS (
               INSERT INTO cbt_events (audience, event, payload)
               SELECT 'staff', 'session:updated', data FROM ins
             ),
             ver AS (
               INSERT INTO cbt_meta (key, value)
               SELECT 'docs_version', '1' WHERE EXISTS (SELECT 1 FROM ins)
               ON CONFLICT (key) DO UPDATE
               SET value = ((CASE WHEN cbt_meta.value ~ '^[0-9]+$' THEN cbt_meta.value::bigint ELSE 0 END) + 1)::text
               RETURNING value
             )
             SELECT
               COALESCE(
                 (SELECT data FROM ins LIMIT 1),
                 (SELECT data FROM cbt_docs WHERE collection = 'sessions' AND data->>'studentId' = $3 AND data->>'examId' = $4 LIMIT 1)
               ) AS session_data,
               (SELECT value FROM ver LIMIT 1) AS version`,
            [sessionId, sessionJson, params.studentId, params.examId]
          );
          await client.query('COMMIT');
          const row = rows[0];
          if (!row?.session_data) return null;
          return {
            session: row.session_data,
            version: row.version ? Number(row.version) : null,
          };
        } catch (txErr) {
          await client.query('ROLLBACK').catch(() => undefined);
          throw txErr;
        } finally {
          client.release();
        }
      }

      const { rows } = await pgPool!.query<{
        session_data: Record<string, unknown> | null;
        version: string | null;
      }>(
        `WITH upd AS (
           UPDATE cbt_docs
           SET data = jsonb_set(
             jsonb_set(data, '{isOnline}', 'true'::jsonb, true),
             '{lastHeartbeatAt}', to_jsonb($3::text), true
           ),
           updated_at = now()
           WHERE collection = 'sessions' AND id = $1 AND data->>'studentId' = $2
           RETURNING data
         ),
         ev AS (
           INSERT INTO cbt_events (audience, event, payload)
           SELECT 'staff', 'session:updated', data FROM upd
         ),
         ver AS (
           INSERT INTO cbt_meta (key, value)
           SELECT 'docs_version', '1' WHERE EXISTS (SELECT 1 FROM upd)
           ON CONFLICT (key) DO UPDATE
           SET value = ((CASE WHEN cbt_meta.value ~ '^[0-9]+$' THEN cbt_meta.value::bigint ELSE 0 END) + 1)::text
           RETURNING value
         )
         SELECT
           (SELECT data FROM upd LIMIT 1) AS session_data,
           (SELECT value FROM ver LIMIT 1) AS version`,
        [params.resumeSessionId || '', params.studentId, params.nowIso]
      );
      const row = rows[0];
      if (!row?.session_data) return null;
      return {
        session: row.session_data,
        version: row.version ? Number(row.version) : null,
      };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

export async function dbPgSaveStudentAnswers(params: {
  sessionId: string;
  studentId: string;
  answersPatch: Record<string, { questionId: string; answer: string; isDoubtful: boolean; updatedAt: string }>;
  currentQuestionIndex?: number;
  nowIso: string;
  emitAdminEvent: boolean;
}): Promise<
  | { kind: 'saved'; session: Record<string, unknown>; version: number | null }
  | { kind: 'not_found' }
  | { kind: 'invalid_question' }
  | { kind: 'conflict'; status: string }
  | null
> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      const patchJson = sanitizeJsonForPg(params.answersPatch);
      const idxParam =
        typeof params.currentQuestionIndex === 'number' && Number.isFinite(params.currentQuestionIndex)
          ? Math.max(0, Math.floor(params.currentQuestionIndex))
          : null;

      const { rows } = await pgPool!.query<{
        session_data: Record<string, unknown> | null;
        version: string | null;
      }>(
        `WITH upd AS (
           UPDATE cbt_docs doc
           SET data = jsonb_set(
             jsonb_set(
               jsonb_set(
                 jsonb_set(
                   doc.data,
                   '{answers}',
                   COALESCE(doc.data->'answers', '{}'::jsonb) || COALESCE((
                     SELECT jsonb_object_agg(kv.key, kv.value)
                     FROM jsonb_each($3::jsonb) AS kv
                     WHERE doc.data->'questionOrder' ? kv.key
                       AND (
                         (doc.data->'answers'->kv.key->>'updatedAt') IS NULL
                         OR COALESCE(kv.value->>'updatedAt', '') >= (doc.data->'answers'->kv.key->>'updatedAt')
                       )
                   ), '{}'::jsonb),
                   true
                 ),
                 '{currentQuestionIndex}',
                 CASE WHEN $4::int IS NOT NULL THEN to_jsonb($4::int) ELSE COALESCE(doc.data->'currentQuestionIndex', '0'::jsonb) END,
                 true
               ),
               '{isOnline}',
               'true'::jsonb,
               true
             ),
             '{lastHeartbeatAt}',
             to_jsonb($5::text),
             true
           ),
           updated_at = now()
           WHERE doc.collection = 'sessions'
             AND doc.id = $1
             AND doc.data->>'studentId' = $2
             AND doc.data->>'status' = 'SEDANG_UJIAN'
             AND EXISTS (
               SELECT 1 FROM jsonb_each($3::jsonb) AS kv
               WHERE doc.data->'questionOrder' ? kv.key
             )
           RETURNING doc.data
         ),
         ev AS (
           INSERT INTO cbt_events (audience, event, payload)
           SELECT 'staff', 'session:updated', data
           FROM upd
           WHERE $6::boolean = true
         ),
         ver AS (
           INSERT INTO cbt_meta (key, value)
           SELECT 'docs_version', '1'
           WHERE EXISTS (SELECT 1 FROM upd)
           ON CONFLICT (key) DO UPDATE
           SET value = ((CASE WHEN cbt_meta.value ~ '^[0-9]+$' THEN cbt_meta.value::bigint ELSE 0 END) + 1)::text
           RETURNING value
         )
         SELECT
           (SELECT data FROM upd LIMIT 1) AS session_data,
           (SELECT value FROM ver LIMIT 1) AS version`,
        [params.sessionId, params.studentId, patchJson, idxParam, params.nowIso, params.emitAdminEvent]
      );

      const row = rows[0];
      if (row?.session_data) {
        return {
          kind: 'saved' as const,
          session: row.session_data,
          version: row.version ? Number(row.version) : null,
        };
      }

      const diag = await pgPool!.query<{ status: string | null; valid: boolean }>(
        `SELECT
           data->>'status' AS status,
           EXISTS (SELECT 1 FROM jsonb_each($3::jsonb) AS kv WHERE data->'questionOrder' ? kv.key) AS valid
         FROM cbt_docs
         WHERE collection = 'sessions' AND id = $1 AND data->>'studentId' = $2
         LIMIT 1`,
        [params.sessionId, params.studentId, patchJson]
      );
      if (diag.rows.length === 0) {
        return { kind: 'not_found' as const };
      }
      const d = diag.rows[0];
      if (d.status && d.status !== 'SEDANG_UJIAN') {
        return { kind: 'conflict' as const, status: d.status };
      }
      return { kind: 'invalid_question' as const };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

export async function dbPgReportViolation(params: {
  sessionId: string;
  studentId: string;
  type: string;
  description: string;
  violationId: string;
  nowIso: string;
}): Promise<
  | {
      kind: 'not_found';
    }
  | {
      kind: 'skipped';
      status: string;
      violationCount: number;
    }
  | {
      kind: 'logged';
      session: Record<string, unknown>;
      violation: Record<string, unknown>;
      maxViolations: number;
      triggeredBlock: boolean;
      version: number | null;
    }
  | null
> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      const { rows: ctxRows } = await pgPool!.query<{
        session_data: Record<string, unknown> | null;
        exam_data: Record<string, unknown> | null;
      }>(
        `SELECT
           s.data AS session_data,
           (SELECT e.data FROM cbt_docs e WHERE e.collection = 'exams' AND e.id = s.data->>'examId' LIMIT 1) AS exam_data
         FROM cbt_docs s
         WHERE s.collection = 'sessions' AND s.id = $1 AND s.data->>'studentId' = $2
         LIMIT 1`,
        [params.sessionId, params.studentId]
      );
      const ctxRow = ctxRows[0];
      if (!ctxRow?.session_data) {
        return { kind: 'not_found' as const };
      }
      const sess = ctxRow.session_data;
      const exam = ctxRow.exam_data;
      if (sess.status === 'SELESAI' || !exam || !exam.antiCheatEnabled) {
        return {
          kind: 'skipped' as const,
          status: String(sess.status || 'SEDANG_UJIAN'),
          violationCount: Number(sess.violationCount || 0),
        };
      }

      const maxViolations = Math.max(1, Number(exam.maxViolations || 3));
      const { rows } = await pgPool!.query<{
        session_data: Record<string, unknown> | null;
        violation_data: Record<string, unknown> | null;
        version: string | null;
      }>(
        `WITH upd AS (
           UPDATE cbt_docs
           SET data = (
             WITH calc AS (
               SELECT
                 (COALESCE((data->>'violationCount')::int, 0) + 1) AS next_count
             )
             SELECT
               jsonb_set(
                 jsonb_set(
                   jsonb_set(
                     data,
                     '{violationCount}',
                     to_jsonb((SELECT next_count FROM calc)),
                     true
                   ),
                   '{lastViolationReason}',
                   to_jsonb($3::text),
                   true
                 ),
                 '{status}',
                 CASE WHEN (SELECT next_count FROM calc) >= $4::int THEN '"TERBLOKIR"'::jsonb ELSE COALESCE(data->'status', '"SEDANG_UJIAN"'::jsonb) END,
                 true
               ) ||
               CASE WHEN (COALESCE((data->>'violationCount')::int, 0) + 1) >= $4::int
                 THEN jsonb_build_object('blockedAt', $5::text)
                 ELSE '{}'::jsonb
               END
           ),
           updated_at = now()
           WHERE collection = 'sessions' AND id = $1 AND data->>'studentId' = $2 AND data->>'status' <> 'SELESAI'
           RETURNING data
         ),
         viol_obj AS (
           SELECT jsonb_build_object(
             'id', $6::text,
             'sessionId', u.data->>'id',
             'examId', u.data->>'examId',
             'examTitle', $7::text,
             'studentId', u.data->>'studentId',
             'studentNis', COALESCE(u.data->>'studentNis', ''),
             'studentName', COALESCE(u.data->>'studentName', ''),
             'className', COALESCE(u.data->>'className', ''),
             'room', COALESCE(u.data->>'room', ''),
             'type', $8::text,
             'description', $3::text,
             'countAtTime', COALESCE((u.data->>'violationCount')::int, 1),
             'triggeredBlock', (COALESCE((u.data->>'violationCount')::int, 1) >= $4::int),
             'timestamp', $5::text
           ) AS vdata
           FROM upd u
         ),
         ins_viol AS (
           INSERT INTO cbt_docs (collection, id, pos, data)
           SELECT 'violations', $6::text, -extract(epoch from now()), vdata
           FROM viol_obj
           ON CONFLICT (collection, id) DO NOTHING
           RETURNING data
         ),
         ev1 AS (
           INSERT INTO cbt_events (audience, event, payload)
           SELECT 'staff', 'violation:logged', data FROM ins_viol
         ),
         ev2 AS (
           INSERT INTO cbt_events (audience, event, payload)
           SELECT 'staff', 'session:updated', data FROM upd
         ),
         ver AS (
           INSERT INTO cbt_meta (key, value)
           SELECT 'docs_version', '1' WHERE EXISTS (SELECT 1 FROM upd)
           ON CONFLICT (key) DO UPDATE
           SET value = ((CASE WHEN cbt_meta.value ~ '^[0-9]+$' THEN cbt_meta.value::bigint ELSE 0 END) + 1)::text
           RETURNING value
         )
         SELECT
           (SELECT data FROM upd LIMIT 1) AS session_data,
           (SELECT data FROM ins_viol LIMIT 1) AS violation_data,
           (SELECT value FROM ver LIMIT 1) AS version`,
        [
          params.sessionId,
          params.studentId,
          params.description,
          maxViolations,
          params.nowIso,
          params.violationId,
          String(exam.title || 'Ujian CBT'),
          params.type,
        ]
      );

      const out = rows[0];
      if (!out?.session_data || !out?.violation_data) {
        return {
          kind: 'skipped' as const,
          status: String(sess.status || 'SELESAI'),
          violationCount: Number(sess.violationCount || 0),
        };
      }
      const vCount = Number(out.session_data.violationCount || 1);
      return {
        kind: 'logged' as const,
        session: out.session_data,
        violation: out.violation_data,
        maxViolations,
        triggeredBlock: vCount >= maxViolations,
        version: out.version ? Number(out.version) : null,
      };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

export async function dbPgFinishStudentSession(params: {
  sessionId: string;
  studentId: string;
  clientAnswersPatch: Record<string, { questionId: string; answer: string; isDoubtful: boolean; updatedAt: string }> | null;
  nowIso: string;
  scoreSession: (session: Record<string, unknown>, bank: Record<string, unknown> | undefined) => Record<string, unknown>;
}): Promise<
  | { kind: 'not_found' }
  | {
      kind: 'finished';
      session: Record<string, unknown>;
      exam: Record<string, unknown> | undefined;
      bank: Record<string, unknown> | undefined;
      version: number | null;
    }
  | null
> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      const hasClientPatch = Boolean(params.clientAnswersPatch && Object.keys(params.clientAnswersPatch).length > 0);
      const patchJson = hasClientPatch ? sanitizeJsonForPg(params.clientAnswersPatch) : '{}';

      // Step 1: Merge any client-side pending/unsynced answers atomically if still SEDANG_UJIAN,
      // and read back the authoritative session, exam, and bank from PG.
      const { rows: loadRows } = await pgPool!.query<{
        session_data: Record<string, unknown> | null;
        exam_data: Record<string, unknown> | null;
        bank_data: Record<string, unknown> | null;
      }>(
        `WITH target AS (
           SELECT id, data
           FROM cbt_docs
           WHERE collection = 'sessions' AND id = $1 AND data->>'studentId' = $2
           LIMIT 1
         ),
         filtered_patch AS (
           SELECT COALESCE(jsonb_object_agg(kv.key, kv.value), '{}'::jsonb) AS obj
           FROM jsonb_each($3::jsonb) AS kv
           CROSS JOIN target t
           WHERE $4::boolean = true
             AND t.data->>'status' = 'SEDANG_UJIAN'
             AND t.data->'questionOrder' ? kv.key
             AND (
               (t.data->'answers'->kv.key->>'updatedAt') IS NULL
               OR COALESCE(kv.value->>'updatedAt', '') >= (t.data->'answers'->kv.key->>'updatedAt')
             )
         ),
         merged AS (
           UPDATE cbt_docs doc
           SET data = jsonb_set(
             doc.data,
             '{answers}',
             COALESCE(doc.data->'answers', '{}'::jsonb) || fp.obj,
             true
           ),
           updated_at = now()
           FROM target t, filtered_patch fp
           WHERE doc.collection = 'sessions'
             AND doc.id = t.id
             AND t.data->>'status' = 'SEDANG_UJIAN'
             AND fp.obj <> '{}'::jsonb
           RETURNING doc.data
         ),
         latest_sess AS (
           SELECT COALESCE((SELECT data FROM merged LIMIT 1), (SELECT data FROM target LIMIT 1)) AS data
         )
         SELECT
           ls.data AS session_data,
           (SELECT e.data FROM cbt_docs e WHERE e.collection = 'exams' AND e.id = ls.data->>'examId' LIMIT 1) AS exam_data,
           (SELECT b.data FROM cbt_docs b
            JOIN cbt_docs e ON e.collection = 'exams' AND e.id = ls.data->>'examId'
            WHERE b.collection = 'banks' AND b.id = e.data->>'bankId'
            LIMIT 1) AS bank_data
         FROM latest_sess ls`,
        [params.sessionId, params.studentId, patchJson, hasClientPatch]
      );

      const loaded = loadRows[0];
      if (!loaded?.session_data) {
        return { kind: 'not_found' as const };
      }

      const session = loaded.session_data;
      const exam = loaded.exam_data || undefined;
      const bank = loaded.bank_data || undefined;

      if (session.status === 'SELESAI') {
        return {
          kind: 'finished' as const,
          session,
          exam,
          bank,
          version: null,
        };
      }

      session.status = 'SELESAI';
      session.finishedAt = params.nowIso;
      session.isOnline = false;
      const scored = params.scoreSession(session, bank);

      // Step 2: Patch ONLY finish/score fields via JSONB || so answers are preserved intact.
      const scorePatch = sanitizeJsonForPg({
        status: 'SELESAI',
        finishedAt: scored.finishedAt,
        isOnline: false,
        pgCorrect: scored.pgCorrect,
        pgTotal: scored.pgTotal,
        pgScore: scored.pgScore,
        essayScore: scored.essayScore,
        finalScore: scored.finalScore,
      });

      const { rows: finRows } = await pgPool!.query<{
        session_data: Record<string, unknown> | null;
        version: string | null;
      }>(
        `WITH upd AS (
           UPDATE cbt_docs
           SET data = data || $3::jsonb,
               updated_at = now()
           WHERE collection = 'sessions' AND id = $1 AND data->>'studentId' = $2
           RETURNING data
         ),
         ev1 AS (
           INSERT INTO cbt_events (audience, event, payload)
           SELECT 'staff', 'session:updated', data FROM upd
         ),
         ev2 AS (
           INSERT INTO cbt_events (audience, event, payload)
           SELECT 'staff', 'sync:results', jsonb_build_object('sessionId', data->>'id', 'examId', data->>'examId') FROM upd
         ),
         ver AS (
           INSERT INTO cbt_meta (key, value)
           SELECT 'docs_version', '1' WHERE EXISTS (SELECT 1 FROM upd)
           ON CONFLICT (key) DO UPDATE
           SET value = ((CASE WHEN cbt_meta.value ~ '^[0-9]+$' THEN cbt_meta.value::bigint ELSE 0 END) + 1)::text
           RETURNING value
         )
         SELECT
           (SELECT data FROM upd LIMIT 1) AS session_data,
           (SELECT value FROM ver LIMIT 1) AS version`,
        [params.sessionId, params.studentId, scorePatch]
      );

      return {
        kind: 'finished' as const,
        session: finRows[0]?.session_data || scored,
        exam,
        bank,
        version: finRows[0]?.version ? Number(finRows[0].version) : null,
      };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

export async function dbPgStudentHeartbeat(params: {
  sessionId: string;
  studentId: string;
  currentQuestionIndex?: number;
  nowIso: string;
  emitAdminEvent: boolean;
}): Promise<{ session: Record<string, unknown>; version: number | null } | null> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      const idxParam =
        typeof params.currentQuestionIndex === 'number' && params.currentQuestionIndex >= 0
          ? Math.floor(params.currentQuestionIndex)
          : null;

      const { rows } = await pgPool!.query<{
        session_data: Record<string, unknown> | null;
        structural_change: boolean;
        version: string | null;
      }>(
        `WITH prev AS (
           SELECT
             COALESCE((data->>'isOnline')::boolean, false) AS was_online,
             COALESCE((data->>'currentQuestionIndex')::int, 0) AS prev_idx
           FROM cbt_docs
           WHERE collection = 'sessions' AND id = $1 AND data->>'studentId' = $2
           LIMIT 1
         ),
         upd AS (
           UPDATE cbt_docs
           SET data = jsonb_set(
             jsonb_set(
               jsonb_set(data, '{isOnline}', 'true'::jsonb, true),
               '{lastHeartbeatAt}', to_jsonb($3::text), true
             ),
             '{currentQuestionIndex}',
             CASE WHEN $4::int IS NOT NULL THEN to_jsonb($4::int) ELSE COALESCE(data->'currentQuestionIndex', '0'::jsonb) END,
             true
           ),
           updated_at = now()
           WHERE collection = 'sessions' AND id = $1 AND data->>'studentId' = $2
           RETURNING data
         ),
         flags AS (
           SELECT
             COALESCE((SELECT (NOT was_online) OR ($4::int IS NOT NULL AND $4::int <> prev_idx) FROM prev LIMIT 1), false) AS changed
         ),
         ev AS (
           INSERT INTO cbt_events (audience, event, payload)
           SELECT 'staff', 'session:updated', u.data
           FROM upd u, flags f
           WHERE $5::boolean = true OR f.changed = true
         ),
         ver AS (
           INSERT INTO cbt_meta (key, value)
           SELECT 'docs_version', '1'
           FROM flags f
           WHERE EXISTS (SELECT 1 FROM upd) AND f.changed = true
           ON CONFLICT (key) DO UPDATE
           SET value = ((CASE WHEN cbt_meta.value ~ '^[0-9]+$' THEN cbt_meta.value::bigint ELSE 0 END) + 1)::text
           RETURNING value
         )
         SELECT
           (SELECT data FROM upd LIMIT 1) AS session_data,
           (SELECT changed FROM flags LIMIT 1) AS structural_change,
           (SELECT value FROM ver LIMIT 1) AS version`,
        [params.sessionId, params.studentId, params.nowIso, idxParam, params.emitAdminEvent]
      );
      const row = rows[0];
      if (!row?.session_data) return null;
      return {
        session: row.session_data,
        version: row.version ? Number(row.version) : null,
      };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

export async function dbPgMarkStaleSessionsOffline(cutoffIso: string): Promise<{
  updatedSessions: Record<string, unknown>[];
  version: number | null;
} | null> {
  if (!getIsUsingPostgres() || !pgPool) return null;
  try {
    return await withPgRetry(async () => {
      await ensurePgSchema();
      const { rows } = await pgPool!.query<{
        updated_sessions: Record<string, unknown>[];
        version: string | null;
      }>(
        `WITH upd AS (
           UPDATE cbt_docs
           SET data = jsonb_set(data, '{isOnline}', 'false'::jsonb, true),
               updated_at = now()
           WHERE collection = 'sessions'
             AND COALESCE((data->>'isOnline')::boolean, false) = true
             AND data->>'status' IN ('SEDANG_UJIAN', 'TERBLOKIR')
             AND COALESCE(data->>'lastHeartbeatAt', '') < $1
           RETURNING data
         ),
         ev AS (
           INSERT INTO cbt_events (audience, event, payload)
           SELECT 'staff', 'session:updated', data FROM upd
         ),
         ver AS (
           INSERT INTO cbt_meta (key, value)
           SELECT 'docs_version', '1' WHERE EXISTS (SELECT 1 FROM upd)
           ON CONFLICT (key) DO UPDATE
           SET value = ((CASE WHEN cbt_meta.value ~ '^[0-9]+$' THEN cbt_meta.value::bigint ELSE 0 END) + 1)::text
           RETURNING value
         )
         SELECT
           COALESCE((SELECT jsonb_agg(data) FROM upd), '[]'::jsonb) AS updated_sessions,
           (SELECT value FROM ver LIMIT 1) AS version`,
        [cutoffIso]
      );
      const row = rows[0];
      return {
        updatedSessions: Array.isArray(row?.updated_sessions) ? row.updated_sessions : [],
        version: row?.version ? Number(row.version) : null,
      };
    });
  } catch (err) {
    if (pgFailedOver || shouldFailoverPgError(err)) {
      activatePgFailover(err);
      return null;
    }
    throw err;
  }
}

