import type {
  AttendancePeriod,
  AttendanceRecord,
  AuthUser,
  BroadcastNotice,
  ExamSchedule,
  ExamSession,
  LearningMaterial,
  QuestionBank,
  SanitizedQuestion,
  SchoolSettings,
  ServerMetrics,
  Student,
  ViolationLog,
} from '../types/cbt';

const TOKEN_STORAGE_KEY = 'candycbt_tab_token';

function readStoredToken(): string {
  try {
    return typeof window !== 'undefined' ? window.sessionStorage.getItem(TOKEN_STORAGE_KEY) || '' : '';
  } catch {
    return '';
  }
}

let memoryToken = readStoredToken();
let unauthorizedHandler: ((message: string) => void) | null = null;
let unauthorizedNotifiedForToken = '';

export function setUnauthorizedHandler(handler: ((message: string) => void) | null): void {
  unauthorizedHandler = handler;
}

export function setAuthToken(token: string): void {
  memoryToken = token;
  unauthorizedNotifiedForToken = '';
  try {
    if (typeof window !== 'undefined') {
      if (token) {
        window.sessionStorage.setItem(TOKEN_STORAGE_KEY, token);
      } else {
        window.sessionStorage.removeItem(TOKEN_STORAGE_KEY);
      }
    }
  } catch {
    // ignore storage restriction inside sandboxed iframes
  }
}

export function getAuthToken(): string {
  return memoryToken;
}

export function clearAuthToken(): void {
  memoryToken = '';
  unauthorizedNotifiedForToken = '';
  try {
    if (typeof window !== 'undefined') {
      window.sessionStorage.removeItem(TOKEN_STORAGE_KEY);
    }
  } catch {
    // ignore storage restriction
  }
}

const TRANSIENT_HTTP_STATUS = new Set([502, 503, 504]);

function isLoginEndpoint(url: string): boolean {
  return (
    url.startsWith('/api/auth/admin/login') ||
    url.startsWith('/api/auth/admin/setup') ||
    url.startsWith('/api/auth/student/login') ||
    url.startsWith('/api/auth/teacher/login')
  );
}

function isIdempotentPost(url: string): boolean {
  return (
    isLoginEndpoint(url) ||
    /^\/api\/student\/sessions\/[^/]+\/(answer|finish)$/.test(url) ||
    url === '/api/events'
  );
}

function handleUnauthorizedResponse(url: string, errMsg: string): void {
  if (isLoginEndpoint(url) || !memoryToken) {
    return;
  }
  const staleToken = memoryToken;
  clearAuthToken();
  if (realtimeTimer !== null) {
    window.clearTimeout(realtimeTimer);
    realtimeTimer = null;
  }
  setRealtimeConnected(false);
  if (unauthorizedNotifiedForToken !== staleToken) {
    unauthorizedNotifiedForToken = staleToken;
    unauthorizedHandler?.(errMsg);
  }
}

let sessionVerifyPromise: Promise<'valid' | 'invalid' | 'error'> | null = null;

async function confirmSessionWithServer(): Promise<'valid' | 'invalid' | 'error'> {
  if (!memoryToken) return 'invalid';
  if (sessionVerifyPromise) {
    return sessionVerifyPromise;
  }
  const tokenChecked = memoryToken;
  sessionVerifyPromise = (async () => {
    try {
      const res = await fetch('/api/auth/me', {
        headers: {
          Authorization: `Bearer ${tokenChecked}`,
        },
      });
      const refreshed = res.headers.get('x-refreshed-token');
      if (refreshed && memoryToken === tokenChecked) {
        setAuthToken(refreshed);
      }
      if (res.ok) {
        return 'valid';
      }
      if (res.status === 401 || res.status === 403 || res.status === 404) {
        return 'invalid';
      }
      return 'error';
    } catch {
      return 'error';
    } finally {
      sessionVerifyPromise = null;
    }
  })();
  return sessionVerifyPromise;
}

function delayMs(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function isNetworkFetchError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  if (err instanceof TypeError) return true;
  if (err.name === 'AbortError') return true;
  const msg = err.message.toLowerCase();
  return (
    msg.includes('failed to fetch') ||
    msg.includes('load failed') ||
    msg.includes('networkerror') ||
    msg.includes('network request failed')
  );
}

function formatNetworkErrorMessage(err: unknown): string {
  if (err instanceof Error && err.name === 'AbortError') {
    return 'Waktu tunggu permintaan ke server habis. Silakan coba lagi.';
  }
  return 'Gagal terhubung ke server CandyCBT (koneksi terputus atau server sedang sibuk). Silakan coba beberapa saat lagi.';
}

export async function apiFetch<T>(url: string, options: RequestInit = {}): Promise<T> {
  const method = (options.method || 'GET').toUpperCase();
  const isReadMethod = method === 'GET' || method === 'HEAD';
  const idempotentWrite = method === 'POST' && isIdempotentPost(url);
  const canRetryNetwork = isReadMethod || idempotentWrite;
  const maxAttempts = isReadMethod ? 3 : idempotentWrite ? 3 : 2;
  const timeoutMs = isReadMethod ? 15_000 : 25_000;
  let lastError: Error | null = null;
  let verifiedSessionAfter401 = false;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const headers = new Headers(options.headers || {});
    if (!headers.has('Content-Type') && options.body && !(options.body instanceof FormData)) {
      headers.set('Content-Type', 'application/json');
    }
    if (memoryToken) {
      headers.set('Authorization', `Bearer ${memoryToken}`);
    }

    const controller = new AbortController();
    const externalSignal = options.signal;
    const onExternalAbort = () => controller.abort();
    if (externalSignal) {
      if (externalSignal.aborted) {
        controller.abort();
      } else {
        externalSignal.addEventListener('abort', onExternalAbort, { once: true });
      }
    }
    const timer = window.setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        ...options,
        headers,
        signal: controller.signal,
      });

      const refreshedToken = res.headers.get('x-refreshed-token');
      if (refreshedToken && memoryToken) {
        setAuthToken(refreshedToken);
      }

      const contentType = res.headers.get('content-type') || '';
      if (contentType.includes('application/json')) {
        const data = await res.json();
        if (!res.ok) {
          const errMsg = data?.error || `Terjadi kesalahan server (${res.status})`;
          if (res.status === 401) {
            if (!isLoginEndpoint(url) && memoryToken) {
              if (url === '/api/auth/me') {
                handleUnauthorizedResponse(url, errMsg);
                throw new Error(errMsg);
              }
              if (!verifiedSessionAfter401) {
                verifiedSessionAfter401 = true;
                const check = await confirmSessionWithServer();
                if (check === 'valid' && attempt < maxAttempts - 1) {
                  await delayMs(150);
                  continue;
                }
                if (check === 'error') {
                  throw new Error(
                    'Koneksi server atau database terganggu sementara. Silakan muat ulang atau coba lagi.'
                  );
                }
                handleUnauthorizedResponse(url, errMsg);
                throw new Error(errMsg);
              }
              handleUnauthorizedResponse(url, errMsg);
            }
            throw new Error(errMsg);
          }
          const canRetryStatus =
            isReadMethod || idempotentWrite
              ? TRANSIENT_HTTP_STATUS.has(res.status)
              : res.status === 503;
          const shouldRetry = data?.retryable !== false && canRetryStatus;
          if (attempt < maxAttempts - 1 && shouldRetry) {
            lastError = new Error(errMsg);
            await delayMs(250 * (attempt + 1) + Math.floor(Math.random() * 200));
            continue;
          }
          throw new Error(errMsg);
        }
        return data as T;
      }

      if (!res.ok) {
        const errMsg = `Permintaan gagal (${res.status})`;
        if (res.status === 401) {
          const sessionMsg = 'Sesi tidak valid atau telah kedaluwarsa. Silakan login kembali.';
          if (!isLoginEndpoint(url) && memoryToken) {
            if (url === '/api/auth/me') {
              handleUnauthorizedResponse(url, sessionMsg);
              throw new Error(sessionMsg);
            }
            if (!verifiedSessionAfter401) {
              verifiedSessionAfter401 = true;
              const check = await confirmSessionWithServer();
              if (check === 'valid' && attempt < maxAttempts - 1) {
                await delayMs(150);
                continue;
              }
              if (check === 'error') {
                throw new Error(
                  'Koneksi server atau database terganggu sementara. Silakan muat ulang atau coba lagi.'
                );
              }
              handleUnauthorizedResponse(url, sessionMsg);
            } else {
              handleUnauthorizedResponse(url, sessionMsg);
            }
          }
          throw new Error(errMsg);
        }
        if (
          attempt < maxAttempts - 1 &&
          (isReadMethod || idempotentWrite) &&
          TRANSIENT_HTTP_STATUS.has(res.status)
        ) {
          lastError = new Error(errMsg);
          await delayMs(250 * (attempt + 1) + Math.floor(Math.random() * 200));
          continue;
        }
        throw new Error(errMsg);
      }

      throw new Error(
        `Respon server tidak valid (${res.status}, bukan JSON). Pastikan endpoint API backend aktif.`
      );
    } catch (err) {
      if (externalSignal?.aborted) {
        throw err instanceof Error ? err : new Error('Permintaan dibatalkan.');
      }
      if (isNetworkFetchError(err)) {
        const friendlyErr = new Error(formatNetworkErrorMessage(err));
        if (attempt < maxAttempts - 1 && canRetryNetwork) {
          lastError = friendlyErr;
          await delayMs(300 * (attempt + 1) + Math.floor(Math.random() * 200));
          continue;
        }
        throw friendlyErr;
      }
      throw err instanceof Error ? err : lastError || new Error('Gagal terhubung ke server.');
    } finally {
      window.clearTimeout(timer);
      if (externalSignal) {
        externalSignal.removeEventListener('abort', onExternalAbort);
      }
    }
  }

  throw lastError || new Error('Gagal terhubung ke server.');
}

interface RealtimeListener {
  onEvent: (event: string, payload: unknown) => void;
  onStatusChange?: (connected: boolean) => void;
}

const POLL_INTERVAL_MS = 8000;
const POLL_ERROR_BACKOFF_MS = 10000;
const realtimeListeners = new Set<RealtimeListener>();
let realtimeCursor: number | null = null;
let realtimeToken = '';
let realtimeTimer: number | null = null;
let realtimeInFlight = false;
let realtimeConnected = false;

function setRealtimeConnected(connected: boolean): void {
  if (realtimeConnected === connected) return;
  realtimeConnected = connected;
  for (const listener of realtimeListeners) {
    listener.onStatusChange?.(connected);
  }
}

function scheduleRealtimePoll(delay?: number): void {
  if (realtimeTimer !== null) {
    window.clearTimeout(realtimeTimer);
  }
  const jitter = Math.floor(Math.random() * 1200) - 400;
  const effectiveDelay = delay !== undefined ? delay : Math.max(1500, POLL_INTERVAL_MS + jitter);
  realtimeTimer = window.setTimeout(pollRealtime, effectiveDelay);
}

async function pollRealtime(): Promise<void> {
  realtimeTimer = null;
  if (realtimeListeners.size === 0 || realtimeInFlight) return;
  if (!memoryToken) {
    setRealtimeConnected(false);
    scheduleRealtimePoll();
    return;
  }
  if (memoryToken !== realtimeToken) {
    realtimeToken = memoryToken;
    realtimeCursor = null;
  }

  realtimeInFlight = true;
  let hadError = false;
  try {
    const res = await apiFetch<{ cursor: number; events: { event: string; payload: unknown }[] }>(
      `/api/events?after=${realtimeCursor ?? -1}`
    );
    realtimeCursor = res.cursor;
    setRealtimeConnected(true);
    for (const item of res.events) {
      for (const listener of Array.from(realtimeListeners)) {
        try {
          listener.onEvent(item.event, item.payload);
        } catch (err) {
          console.error('Realtime listener error:', err);
        }
      }
    }
  } catch {
    hadError = true;
    setRealtimeConnected(false);
  } finally {
    realtimeInFlight = false;
    if (realtimeListeners.size > 0) {
      scheduleRealtimePoll(hadError ? POLL_ERROR_BACKOFF_MS : undefined);
    }
  }
}

// Same interface as the former WebSocket client; realtime events are delivered by
// short polling because Vercel Functions cannot keep WebSocket connections open.
export function createCbtWebSocket(
  onEvent: (event: string, payload: unknown) => void,
  onStatusChange?: (connected: boolean) => void
): { send: (event: string, payload?: unknown) => void; close: () => void } {
  const listener: RealtimeListener = { onEvent, onStatusChange };
  realtimeListeners.add(listener);
  if (realtimeConnected) {
    onStatusChange?.(true);
  }
  if (realtimeTimer === null && !realtimeInFlight) {
    scheduleRealtimePoll(0);
  }

  return {
    send: (event: string, payload?: unknown) => {
      if (!memoryToken) return;
      apiFetch('/api/events', {
        method: 'POST',
        body: JSON.stringify({ event, payload }),
      }).catch(() => {
        // heartbeat failures are retried on the next interval
      });
    },
    close: () => {
      realtimeListeners.delete(listener);
      if (realtimeListeners.size === 0 && realtimeTimer !== null) {
        window.clearTimeout(realtimeTimer);
        realtimeTimer = null;
      }
    },
  };
}

export interface PublicInfoResponse {
  settings: Pick<
    SchoolSettings,
    | 'schoolName'
    | 'npsn'
    | 'address'
    | 'academicYear'
    | 'semester'
    | 'serverName'
    | 'maintenanceMode'
    | 'themeColor'
    | 'sidebarStyle'
    | 'schoolLogo'
    | 'portalSlides'
  >;
  adminSetupRequired: boolean;
  sampleStudents: { nis: string; name: string; className: string }[];
  sampleTeachers: { nip: string; fullName: string; subject: string }[];
}

export interface StudentExamListItem {
  id: string;
  title: string;
  subject: string;
  bankCode: string;
  kkm: number;
  questionCount: number;
  durationMinutes: number;
  antiCheatEnabled: boolean;
  maxViolations: number;
  showScoreAfterFinish: boolean;
  startDate: string;
  endDate: string;
  session: {
    id: string;
    status: ExamSession['status'];
    startedAt: string;
    endsAt: string;
    finishedAt?: string;
    violationCount: number;
    lastViolationReason?: string;
    finalScore?: number;
    pgCorrect?: number;
    pgTotal?: number;
  } | null;
}

export interface StartExamResponse {
  session: {
    id: string;
    examId: string;
    status: ExamSession['status'];
    answers: ExamSession['answers'];
    currentQuestionIndex: number;
    startedAt: string;
    endsAt: string;
    violationCount: number;
    lastViolationReason?: string;
  };
  exam: {
    id: string;
    title: string;
    subject: string;
    durationMinutes: number;
    antiCheatEnabled: boolean;
    maxViolations: number;
    showScoreAfterFinish: boolean;
  };
  questions: SanitizedQuestion[];
}

export interface AdminDashboardResponse {
  settings: SchoolSettings;
  metrics: ServerMetrics;
  exams: ExamSchedule[];
  banksCount: number;
  bankKkmMap?: Record<string, number>;
  sessions: ExamSession[];
  recentViolations: ViolationLog[];
  broadcasts: BroadcastNotice[];
}

export interface EnrichedResultSession extends ExamSession {
  examTitle: string;
  subject: string;
  kkm: number;
  essayItems: {
    questionId: string;
    number: number;
    questionText: string;
    studentAnswer: string;
    essayScore: number;
  }[];
}

export interface AdminResultsResponse {
  results: EnrichedResultSession[];
  exams: ExamSchedule[];
  banks: QuestionBank[];
  violations: ViolationLog[];
  settings: SchoolSettings;
}

export type LoginResponse = {
  token: string;
  user: AuthUser;
};

export interface TeacherListItem {
  id: string;
  nip: string;
  fullName: string;
  subject: string;
  isActive: boolean;
  createdAt: string;
  banksCount: number;
  examsCount: number;
}

export type TeachersResponse = { teachers: TeacherListItem[] };
export type StudentsResponse = { students: Student[] };
export type BanksResponse = { banks: QuestionBank[] };
export type ExamsResponse = { exams: ExamSchedule[] };
export type MaterialsResponse = { materials: LearningMaterial[]; classes: string[] };
export type StudentMaterialsResponse = { materials: LearningMaterial[] };

export interface AdminAttendanceResponse {
  period: AttendancePeriod;
  date: string;
  month: string;
  startDate: string;
  endDate: string;
  records: AttendanceRecord[];
  students: Student[];
  exams: ExamSchedule[];
  settings: SchoolSettings;
}

export interface ServerStatusResponse {
  metrics: ServerMetrics;
  settings: SchoolSettings;
  counts?: import('../types/cbt').DatabaseCounts;
}

export interface PurgeDatabaseResponse {
  ok: boolean;
  message: string;
  counts: import('../types/cbt').DatabaseCounts;
  metrics: ServerMetrics;
}
