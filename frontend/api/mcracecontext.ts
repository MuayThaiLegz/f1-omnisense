import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb } from './_db.js';

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const url = req.url ?? '';
    // /api/mcracecontext/2024/tire_stints.csv
    const m = url.match(/\/api\/mcracecontext\/(\d{4})\/tire_stints\.csv/);
    if (!m) return res.status(404).send('');

    const year = m[1];
    const db = await getDb();

    const results = await db.collection('telemetry').aggregate([
      { $match: { Year: year } },
      { $group: {
        _id: { Driver: '$Driver', Race: '$Race', Compound: '$Compound' },
        start_lap: { $min: '$LapNumber' },
        end_lap: { $max: '$LapNumber' },
      }},
      { $sort: { '_id.Race': 1, '_id.Driver': 1, start_lap: 1 } },
    ], { allowDiskUse: true }).toArray();

    const lines = ['session_type,compound,driver_acronym,meeting_name,start_lap,end_lap'];
    for (const r of results) {
      const race = r._id.Race ?? '';
      const raceName = race.includes('Grand Prix') ? race : race + ' Grand Prix';
      lines.push([
        'Race',
        r._id.Compound ?? 'UNKNOWN',
        r._id.Driver ?? '',
        raceName,
        String(r.start_lap ? parseInt(r.start_lap) : 0),
        String(r.end_lap ? parseInt(r.end_lap) : 0),
      ].join(','));
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    return res.send(lines.join('\n'));
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
