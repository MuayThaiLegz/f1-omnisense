import type { JolpicaDriverStanding, JolpicaConstructorStanding, JolpicaRaceResult } from '../types';

const BASE = 'https://api.jolpi.ca/ergast/f1';
const LOCAL_BASE = '/api/jolpica';

async function fetchLocal<T>(route: string): Promise<T | null> {
  try {
    const res = await fetch(`${LOCAL_BASE}/${route}`);
    if (res.ok) return res.json();
  } catch { /* ignore */ }
  return null;
}

async function fetchJolpica<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}/${path}`);
  if (!res.ok) throw new Error(`Jolpica ${path}: ${res.status}`);
  return res.json();
}

export async function getDriverStandings(season: string = 'current'): Promise<JolpicaDriverStanding[]> {
  // Try local first — local data is already unwrapped array
  const local = await fetchLocal<JolpicaDriverStanding[]>('driver_standings');
  if (local && local.length > 0) {
    // Filter to latest season if requesting current
    if (season === 'current') {
      const maxSeason = Math.max(...local.map((d: any) => Number(d.season || 0)));
      return local.filter((d: any) => Number(d.season || 0) === maxSeason);
    }
    return local.filter((d: any) => d.season === season);
  }
  const data = await fetchJolpica<any>(`${season}/driverStandings.json`);
  return data?.MRData?.StandingsTable?.StandingsLists?.[0]?.DriverStandings ?? [];
}

export async function getConstructorStandings(season: string = 'current'): Promise<JolpicaConstructorStanding[]> {
  const local = await fetchLocal<JolpicaConstructorStanding[]>('constructor_standings');
  if (local && local.length > 0) {
    if (season === 'current') {
      const maxSeason = Math.max(...local.map((d: any) => Number(d.season || 0)));
      return local.filter((d: any) => Number(d.season || 0) === maxSeason);
    }
    return local.filter((d: any) => d.season === season);
  }
  const data = await fetchJolpica<any>(`${season}/constructorStandings.json`);
  return data?.MRData?.StandingsTable?.StandingsLists?.[0]?.ConstructorStandings ?? [];
}

export async function getRaceResults(season: string = 'current'): Promise<JolpicaRaceResult[]> {
  const local = await fetchLocal<JolpicaRaceResult[]>('race_results');
  if (local && local.length > 0) {
    if (season === 'current') {
      const maxSeason = Math.max(...local.map((d: any) => Number(d.season || 0)));
      return local.filter((d: any) => Number(d.season || 0) === maxSeason);
    }
    return local.filter((d: any) => d.season === season);
  }
  const data = await fetchJolpica<any>(`${season}/results.json?limit=500`);
  return data?.MRData?.RaceTable?.Races ?? [];
}

export async function getQualifyingResults(season: string = 'current'): Promise<any[]> {
  const local = await fetchLocal<any[]>('qualifying');
  if (local && local.length > 0) {
    if (season === 'current') {
      const maxSeason = Math.max(...local.map((d: any) => Number(d.season || 0)));
      return local.filter((d: any) => Number(d.season || 0) === maxSeason);
    }
    return local;
  }
  const data = await fetchJolpica<any>(`${season}/qualifying.json?limit=500`);
  return data?.MRData?.RaceTable?.Races ?? [];
}

export async function getCircuits(season: string = 'current'): Promise<any[]> {
  const local = await fetchLocal<any[]>('circuits');
  if (local && local.length > 0) return local;
  const data = await fetchJolpica<any>(`${season}/circuits.json`);
  return data?.MRData?.CircuitTable?.Circuits ?? [];
}

export async function getSeasons(): Promise<any[]> {
  const local = await fetchLocal<any[]>('seasons');
  if (local && local.length > 0) return local;
  const data = await fetchJolpica<any>('seasons.json?limit=100');
  return data?.MRData?.SeasonTable?.Seasons ?? [];
}
