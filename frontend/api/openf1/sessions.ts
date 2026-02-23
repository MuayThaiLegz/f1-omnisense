import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb, GP_TO_CIRCUIT, GP_TO_COUNTRY } from '../_db.js';

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  try {
    const db = await getDb();
    const sources: string[] = await db.collection('telemetry').distinct('_source_file');
    const sessions: any[] = [];
    let sessionKey = 9000;
    let raceIdx = 0;
    let prevYear: string | null = null;

    for (const src of sources.sort()) {
      const parts = src.replace('.csv', '').split('_');
      if (parts.length < 3) continue;
      const year = parts[0];
      if (year !== prevYear) { raceIdx = 0; prevYear = year; }
      const raceName = parts.slice(1).join(' ').replace(' Race', '');
      const circuitShort = GP_TO_CIRCUIT[raceName] ?? raceName.split(' ')[0];
      const country = GP_TO_COUNTRY[raceName] ?? circuitShort;
      const month = Math.min(3 + raceIdx, 12);

      sessions.push({
        session_key: sessionKey, session_name: 'Race', session_type: 'Race',
        date_start: `${year}-${String(month).padStart(2, '0')}-15T14:00:00`,
        date_end: `${year}-${String(month).padStart(2, '0')}-15T16:00:00`,
        year: parseInt(year), circuit_key: sessionKey,
        circuit_short_name: circuitShort, country_name: country,
        country_key: sessionKey, location: circuitShort,
        meeting_key: sessionKey, meeting_name: `${year} ${raceName}`,
        _source_file: src,
      });
      sessionKey++;
      raceIdx++;
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(sessions);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
