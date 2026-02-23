import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb } from '../../_db.js';

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const { year, driver } = req.query as { year: string; driver: string };
    const db = await getDb();
    const docs = await db.collection('telemetry').find(
      { Driver: driver, Year: year },
      { projection: { _id: 0, Speed: 1, RPM: 1, Throttle: 1, Brake: 1, DRS: 1, Compound: 1, Race: 1 } }
    ).toArray();

    const races: Record<string, { speeds: number[]; rpms: number[]; throttles: number[]; brakeCount: number; drsCount: number; compounds: Set<string>; total: number }> = {};
    for (const d of docs) {
      const race = (d.Race as string) ?? 'Unknown';
      if (!races[race]) races[race] = { speeds: [], rpms: [], throttles: [], brakeCount: 0, drsCount: 0, compounds: new Set(), total: 0 };
      const r = races[race];
      if (d.Speed != null) { try { r.speeds.push(parseFloat(d.Speed)); } catch {} }
      if (d.RPM != null) { try { r.rpms.push(parseFloat(d.RPM)); } catch {} }
      if (d.Throttle != null) { try { r.throttles.push(parseFloat(d.Throttle)); } catch {} }
      if (d.Brake === true || d.Brake === 'True' || d.Brake === 1) r.brakeCount++;
      try { if (d.DRS != null && parseInt(String(d.DRS)) >= 10) r.drsCount++; } catch {}
      if (d.Compound) r.compounds.add(String(d.Compound));
      r.total++;
    }

    const summaries: any[] = [];
    for (const [raceName, r] of Object.entries(races)) {
      if (!r.speeds.length) continue;
      const n = r.total || 1;
      summaries.push({
        race: raceName.replace(' Grand Prix', ''),
        avgSpeed: +(r.speeds.reduce((a, b) => a + b, 0) / r.speeds.length).toFixed(2),
        topSpeed: +Math.max(...r.speeds).toFixed(1),
        avgRPM: r.rpms.length ? Math.round(r.rpms.reduce((a, b) => a + b, 0) / r.rpms.length) : 0,
        maxRPM: r.rpms.length ? Math.round(Math.max(...r.rpms)) : 0,
        avgThrottle: r.throttles.length ? +(r.throttles.reduce((a, b) => a + b, 0) / r.throttles.length).toFixed(2) : 0,
        brakePct: +(r.brakeCount / n * 100).toFixed(2),
        drsPct: +(r.drsCount / n * 100).toFixed(2),
        compounds: [...r.compounds].sort(),
        samples: n,
      });
    }
    summaries.sort((a, b) => a.race.localeCompare(b.race));

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(summaries);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
