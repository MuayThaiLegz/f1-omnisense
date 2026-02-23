import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb } from '../_db.js';

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  try {
    const db = await getDb();
    const races = await db.collection('race_results')
      .find({}, { projection: { _id: 0 } })
      .sort({ season: 1, round: 1 })
      .toArray();

    const driverData: Record<string, { points: number; wins: number; Driver: any; Constructors: any[]; season: string }> = {};
    for (const race of races) {
      const season = race.season ?? '';
      for (const result of race.Results ?? []) {
        const driver = result.Driver ?? {};
        const did = driver.driverId ?? '';
        const key = `${season}_${did}`;
        if (!driverData[key]) driverData[key] = { points: 0, wins: 0, Driver: null, Constructors: [], season: '' };
        const d = driverData[key];
        d.points += parseFloat(result.points ?? '0');
        if (result.position === '1') d.wins++;
        d.Driver = driver;
        d.season = season;
        const constructor = result.Constructor;
        if (constructor) d.Constructors = [constructor];
      }
    }

    const standings: any[] = [];
    for (const d of Object.values(driverData)) {
      if (d.Driver) {
        standings.push({
          position: '0', positionText: '0',
          points: String(d.points), wins: String(d.wins),
          Driver: d.Driver, Constructors: d.Constructors, season: d.season,
        });
      }
    }
    standings.sort((a, b) => -(parseInt(a.season || '0') - parseInt(b.season || '0')) || -(parseFloat(a.points) - parseFloat(b.points)));

    let currentSeason: string | null = null;
    let pos = 0;
    for (const s of standings) {
      if (s.season !== currentSeason) { currentSeason = s.season; pos = 1; }
      s.position = String(pos);
      s.positionText = String(pos);
      pos++;
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(standings);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
