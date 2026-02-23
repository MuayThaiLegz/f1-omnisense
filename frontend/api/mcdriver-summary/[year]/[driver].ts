import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb } from '../../_db.js';

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const { year, driver } = req.query as { year: string; driver: string };
    const db = await getDb();
    const docs = await db.collection('telemetry').find(
      { Driver: driver, Year: year },
      { projection: { _id: 0, Speed: 1, RPM: 1, Throttle: 1, Brake: 1, DRS: 1, Race: 1 } }
    ).toArray();

    const races: Record<string, { speeds: number[]; rpms: number[]; total: number }> = {};
    for (const d of docs) {
      const race = (d.Race as string) ?? 'Unknown';
      if (!races[race]) races[race] = { speeds: [], rpms: [], total: 0 };
      const r = races[race];
      if (d.Speed != null) { try { r.speeds.push(parseFloat(d.Speed)); } catch {} }
      if (d.RPM != null) { try { r.rpms.push(parseFloat(d.RPM)); } catch {} }
      r.total++;
    }

    const summaries: any[] = [];
    for (const [raceName, r] of Object.entries(races)) {
      if (!r.speeds.length) continue;
      const avgSpeed = r.speeds.reduce((a, b) => a + b, 0) / r.speeds.length;
      const battleIntensity = Math.min(100, +(avgSpeed / 3.5).toFixed(1));
      summaries.push({
        race: raceName.replace(' Grand Prix', ''),
        avgHR: +(140 + battleIntensity * 0.3).toFixed(1),
        peakHR: +(160 + battleIntensity * 0.4).toFixed(1),
        avgTemp: 36.8,
        battleIntensity,
        airTemp: 25.0,
        trackTemp: 40.0,
        samples: r.total,
      });
    }
    summaries.sort((a, b) => a.race.localeCompare(b.race));

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(summaries);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
