import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb, DRIVER_META, buildSessionMap } from '../_db.js';

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  try {
    const db = await getDb();
    const sources: string[] = await db.collection('telemetry').distinct('_source_file');
    const { yearRaceToKey } = buildSessionMap(sources);
    const drivers: any[] = [];

    for (const sk of Object.values(yearRaceToKey)) {
      for (const [code, meta] of Object.entries(DRIVER_META)) {
        drivers.push({
          session_key: sk, meeting_key: sk,
          driver_number: meta.number, broadcast_name: meta.broadcast_name,
          full_name: meta.full_name, name_acronym: code,
          team_name: meta.team_name, team_colour: meta.team_colour,
          first_name: meta.first_name, last_name: meta.last_name,
          country_code: meta.country_code, headshot_url: meta.headshot_url,
        });
      }
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(drivers);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
