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
        lap_time: { $first: '$LapTime' },
        top_speed: { $max: '$Speed' },
        date: { $first: '$Date' },
      }},
      { $sort: { '_id.Year': 1, '_id.Race': 1, '_id.LapNumber': 1 } },
    ], { allowDiskUse: true }).toArray();

    const laps: any[] = [];
    for (const r of results) {
      const { Driver: driver, Year: year, Race: race, LapNumber: lapNum } = r._id;
      if (lapNum == null) continue;
      const sk = resolveSk(yearRaceToKey, year ?? '', race ?? '');
      let lapDuration: number | null = null;
      const lt = r.lap_time;
      if (lt && String(lt).includes('days')) {
        try {
          const timePart = String(lt).split(' ').pop()!;
          const [h, m, s] = timePart.split(':');
          lapDuration = parseFloat(h) * 3600 + parseFloat(m) * 60 + parseFloat(s);
        } catch { /* skip */ }
      }
      laps.push({
        session_key: sk, meeting_key: sk,
        driver_number: DRIVER_NUMBERS[driver] ?? 0,
        lap_number: parseInt(lapNum),
        lap_duration: lapDuration,
        duration_sector_1: null, duration_sector_2: null, duration_sector_3: null,
        is_pit_out_lap: false,
        date_start: String(r.date ?? ''),
        st_speed: r.top_speed ? parseFloat(r.top_speed) : null,
      });
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(laps);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
