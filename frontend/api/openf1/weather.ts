import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb, buildSessionMap } from '../_db.js';

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  try {
    const db = await getDb();
    const sources: string[] = await db.collection('telemetry').distinct('_source_file');
    const { yearRaceToKey } = buildSessionMap(sources);
    const weather: any[] = [];

    for (const [yrRace, sk] of Object.entries(yearRaceToKey)) {
      const year = yrRace.split('|')[0];
      weather.push({
        session_key: sk, meeting_key: sk,
        date: `${year}-06-01T14:00:00`, air_temperature: 25.0,
        track_temperature: 40.0, humidity: 55, pressure: 1013.0,
        rainfall: false, wind_direction: 180, wind_speed: 3.5,
      });
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(weather);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
