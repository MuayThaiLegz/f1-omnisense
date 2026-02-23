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
        start_lap: { $min: '$LapNumber' },
        end_lap: { $max: '$LapNumber' },
        tyre_life: { $min: '$TyreLife' },
      }},
      { $sort: { '_id.Year': 1, '_id.Race': 1, '_id.Driver': 1, start_lap: 1 } },
    ], { allowDiskUse: true }).toArray();

    const stints: any[] = [];
    const stintCounter: Record<string, number> = {};

    for (const r of results) {
      const { Driver: driver, Year: year, Race: race, Compound: compound } = r._id;
      const key = `${year}_${driver}_${race}`;
      stintCounter[key] = (stintCounter[key] ?? 0) + 1;
      const sk = resolveSk(yearRaceToKey, year ?? '', race ?? '');
      stints.push({
        session_key: sk, meeting_key: sk,
        driver_number: DRIVER_NUMBERS[driver ?? ''] ?? 0,
        stint_number: stintCounter[key],
        lap_start: r.start_lap ? parseInt(r.start_lap) : 0,
        lap_end: r.end_lap ? parseInt(r.end_lap) : 0,
        compound: compound ?? 'UNKNOWN',
        tyre_age_at_start: r.tyre_life ? parseInt(r.tyre_life) : 0,
      });
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(stints);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
