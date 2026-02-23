import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb, buildSessionMap } from '../_db.js';

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  try {
    const db = await getDb();
    const sources: string[] = await db.collection('telemetry').distinct('_source_file');
    const { yearRaceToKey } = buildSessionMap(sources);
    const events: any[] = [];

    for (const [yrRace, sk] of Object.entries(yearRaceToKey)) {
      const year = yrRace.split('|')[0];
      events.push({
        date: `${year}-06-01T14:00:00`, session_key: sk, meeting_key: sk,
        driver_number: null, lap_number: 1, category: 'Flag',
        flag: 'GREEN', scope: 'Track', message: 'GREEN LIGHT - PIT EXIT OPEN',
      });
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(events);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
