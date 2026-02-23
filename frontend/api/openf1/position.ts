import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb, DRIVER_NUMBERS, buildSessionMap, resolveSk } from '../_db.js';

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  try {
    const db = await getDb();
    const sources: string[] = await db.collection('telemetry').distinct('_source_file');
    const { yearRaceToKey } = buildSessionMap(sources);

    const results = await db.collection('telemetry').aggregate([
      { $group: {
        _id: { Driver: '$Driver', Year: '$Year', Race: '$Race', LapNumber: '$LapNumber' },
        date: { $last: '$Date' },
      }},
      { $sort: { '_id.Year': 1, '_id.Race': 1, '_id.LapNumber': 1 } },
    ], { allowDiskUse: true }).toArray();

    const lapGroups: Record<string, { driver: string; date: string; year: string; race: string }[]> = {};
    for (const r of results) {
      const { Driver: driver, Year: year, Race: race, LapNumber: lapNum } = r._id;
      const key = `${year}_${race}_${lapNum ?? 0}`;
      (lapGroups[key] ??= []).push({ driver: driver ?? '', date: String(r.date ?? ''), year: year ?? '', race: race ?? '' });
    }

    const positions: any[] = [];
    for (const drivers of Object.values(lapGroups)) {
      for (let i = 0; i < drivers.length; i++) {
        const d = drivers[i];
        const sk = resolveSk(yearRaceToKey, d.year, d.race);
        positions.push({
          session_key: sk, meeting_key: sk,
          driver_number: DRIVER_NUMBERS[d.driver] ?? 0,
          date: d.date, position: i + 1,
        });
      }
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(positions);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
