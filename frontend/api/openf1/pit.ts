import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb, DRIVER_NUMBERS, buildSessionMap, resolveSk } from '../_db.js';

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  try {
    const db = await getDb();
    const sources: string[] = await db.collection('telemetry').distinct('_source_file');
    const { yearRaceToKey } = buildSessionMap(sources);

    const results = await db.collection('telemetry').aggregate([
      { $match: { Compound: { $ne: null } } },
      { $group: {
        _id: { Driver: '$Driver', Year: '$Year', Race: '$Race', Compound: '$Compound' },
        min_lap: { $min: '$LapNumber' },
      }},
      { $sort: { '_id.Race': 1, '_id.Driver': 1, min_lap: 1 } },
    ]).toArray();

    const driverRaceStints: Record<string, { driver: string; year: string; race: string; minLap: number }[]> = {};
    for (const r of results) {
      const { Driver: driver, Year: year, Race: race } = r._id;
      const key = `${year}_${driver}_${race}`;
      (driverRaceStints[key] ??= []).push({
        driver: driver ?? '', year: year ?? '', race: race ?? '',
        minLap: r.min_lap ?? 0,
      });
    }

    const pits: any[] = [];
    for (const stints of Object.values(driverRaceStints)) {
      stints.sort((a, b) => a.minLap - b.minLap);
      for (let i = 1; i < stints.length; i++) {
        const s = stints[i];
        const sk = resolveSk(yearRaceToKey, s.year, s.race);
        pits.push({
          session_key: sk, meeting_key: sk,
          driver_number: DRIVER_NUMBERS[s.driver] ?? 0,
          date: '', lap_number: s.minLap, pit_duration: 23.5,
        });
      }
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(pits);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
