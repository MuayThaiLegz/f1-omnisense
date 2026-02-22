import type {
  OpenF1Session, OpenF1Position, OpenF1Lap, OpenF1Weather,
  OpenF1Interval, OpenF1Pit, OpenF1Stint, OpenF1Driver, OpenF1RaceControl,
} from '../types';

const BASE = 'https://api.openf1.org/v1';
const LOCAL_BASE = '/api/local/openf1';

// Request queue to avoid 429s
let lastRequestTime = 0;
const MIN_DELAY = 300;

async function throttledFetch(url: string): Promise<Response> {
  const now = Date.now();
  const wait = Math.max(0, MIN_DELAY - (now - lastRequestTime));
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  lastRequestTime = Date.now();
  return fetch(url);
}

async function fetchOpenF1<T>(endpoint: string, params?: Record<string, string | number>): Promise<T[]> {
  // Try local data first
  try {
    const localRes = await fetch(`${LOCAL_BASE}/${endpoint}`);
    if (localRes.ok) {
      const data = await localRes.json();
      if (Array.isArray(data) && data.length > 0) {
        if (params) {
          return data.filter((item: any) =>
            Object.entries(params).every(([k, v]) => String(item[k]) === String(v))
          ) as T[];
        }
        return data as T[];
      }
      // Handle object-keyed data (e.g., mclaren_drivers_by_session.json keyed by session_key)
      if (data && typeof data === 'object' && !Array.isArray(data) && params?.session_key) {
        const sessionData = data[String(params.session_key)];
        if (Array.isArray(sessionData) && sessionData.length > 0) {
          const otherParams = Object.fromEntries(
            Object.entries(params).filter(([k]) => k !== 'session_key')
          );
          if (Object.keys(otherParams).length > 0) {
            return sessionData.filter((item: any) =>
              Object.entries(otherParams).every(([k, v]) => String(item[k]) === String(v))
            ) as T[];
          }
          return sessionData as T[];
        }
      }
    }
  } catch { /* fall through to API */ }

  // Fallback to live API with throttling
  const url = new URL(`${BASE}/${endpoint}`);
  if (params) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, String(v)));
  }
  const res = await throttledFetch(url.toString());
  if (!res.ok) throw new Error(`OpenF1 ${endpoint}: ${res.status}`);
  return res.json();
}

export async function getSessions(year?: number): Promise<OpenF1Session[]> {
  return fetchOpenF1<OpenF1Session>('sessions', { year: year ?? new Date().getFullYear() });
}

export async function getAllSessions(): Promise<OpenF1Session[]> {
  return fetchOpenF1<OpenF1Session>('sessions');
}

export async function getLatestSession(): Promise<OpenF1Session | null> {
  const sessions = await getSessions();
  return sessions.length > 0 ? sessions[sessions.length - 1] : null;
}

export async function getPositions(sessionKey: number): Promise<OpenF1Position[]> {
  return fetchOpenF1<OpenF1Position>('position', { session_key: sessionKey });
}

export async function getLatestPositions(sessionKey: number): Promise<OpenF1Position[]> {
  const positions = await getPositions(sessionKey);
  const latest = new Map<number, OpenF1Position>();
  for (const pos of positions) {
    const existing = latest.get(pos.driver_number);
    if (!existing || new Date(pos.date) > new Date(existing.date)) {
      latest.set(pos.driver_number, pos);
    }
  }
  return Array.from(latest.values()).sort((a, b) => a.position - b.position);
}

export async function getLaps(sessionKey: number, driverNumber?: number): Promise<OpenF1Lap[]> {
  const params: Record<string, string | number> = { session_key: sessionKey };
  if (driverNumber) params.driver_number = driverNumber;
  return fetchOpenF1<OpenF1Lap>('laps', params);
}

export async function getWeather(sessionKey: number): Promise<OpenF1Weather[]> {
  return fetchOpenF1<OpenF1Weather>('weather', { session_key: sessionKey });
}

export async function getIntervals(sessionKey: number): Promise<OpenF1Interval[]> {
  return fetchOpenF1<OpenF1Interval>('intervals', { session_key: sessionKey });
}

export async function getPitStops(sessionKey: number): Promise<OpenF1Pit[]> {
  return fetchOpenF1<OpenF1Pit>('pit', { session_key: sessionKey });
}

export async function getStints(sessionKey: number): Promise<OpenF1Stint[]> {
  return fetchOpenF1<OpenF1Stint>('stints', { session_key: sessionKey });
}

export async function getDrivers(sessionKey: number): Promise<OpenF1Driver[]> {
  return fetchOpenF1<OpenF1Driver>('drivers', { session_key: sessionKey });
}

export async function getRaceControl(sessionKey: number): Promise<OpenF1RaceControl[]> {
  return fetchOpenF1<OpenF1RaceControl>('race_control', { session_key: sessionKey });
}
