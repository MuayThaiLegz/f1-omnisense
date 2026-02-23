import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb } from './_db.js';

const COLLECTION_MAP: Record<string, string> = {
  sessions: 'openf1_sessions',
  position: 'openf1_position',
  laps: 'openf1_laps',
  drivers: 'openf1_drivers',
  weather: 'openf1_weather',
  intervals: 'openf1_intervals',
  pit: 'openf1_pit',
  stints: 'openf1_stints',
  race_control: 'openf1_race_control',
};

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const endpoint = (req.query.endpoint as string) ?? '';
    const collName = COLLECTION_MAP[endpoint];
    if (!collName) return res.status(404).json({ error: `Unknown endpoint: ${endpoint}` });

    const db = await getDb();
    const coll = db.collection(collName);

    // Build MongoDB filter from remaining query params
    const filter: Record<string, any> = {};
    for (const [k, v] of Object.entries(req.query)) {
      if (k === 'endpoint') continue;
      if (typeof v !== 'string') continue;
      // Try numeric conversion for session_key, driver_number, year, etc.
      const num = Number(v);
      filter[k] = !isNaN(num) && v.trim() !== '' ? num : v;
    }

    const docs = await coll.find(filter, { projection: { _id: 0 } }).toArray();

    res.setHeader('Cache-Control', 's-maxage=60, stale-while-revalidate=120');
    res.setHeader('Content-Type', 'application/json');
    return res.json(docs);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
