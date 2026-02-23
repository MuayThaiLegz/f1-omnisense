import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb } from './_db.js';

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const fullPath = (req.query.path as string) ?? '';
    // path = 2024/2024_Bahrain_Grand_Prix_Race_biometrics.csv
    const m = fullPath.match(/^(\d{4})\/(.+)\.csv$/);
    if (!m) return res.status(404).send('');

    const year = m[1];
    const filename = m[2]; // e.g. 2024_Bahrain_Grand_Prix_Race_biometrics

    const db = await getDb();

    // Try biometrics collection first
    let data = await db.collection('biometrics').find(
      { _source_file: filename, Year: year },
      { projection: { _id: 0 } }
    ).toArray();

    // Fallback: query telemetry and synthesize biometrics
    if (!data.length) {
      // Extract race name: 2024_Bahrain_Grand_Prix_Race_biometrics -> Bahrain
      const raceName = filename
        .replace(/^\d{4}_/, '')
        .replace(/_biometrics$/, '')
        .replace(/_Grand_Prix_Race$/, '')
        .replace(/_Race$/, '')
        .replace(/_/g, ' ');

      const telemetry = await db.collection('telemetry').find(
        { Race: { $regex: `^${raceName}`, $options: 'i' }, Year: year },
        { projection: { _id: 0 } }
      ).toArray();

      // Synthesize biometrics from telemetry data
      data = telemetry.map(d => {
        const speed = parseFloat(d.Speed) || 0;
        const throttle = parseFloat(d.Throttle) || 0;
        const battleIntensity = Math.min(1, speed / 350);
        const hr = 140 + battleIntensity * 30 + (throttle / 100) * 10;
        return {
          ...d,
          HeartRate_bpm: hr.toFixed(1),
          CockpitTemp_C: (36.5 + battleIntensity * 2).toFixed(1),
          AirTemp_C: '25.0',
          TrackTemp_C: '40.0',
          Humidity_pct: '45',
          BattleIntensity: battleIntensity.toFixed(3),
        };
      });
    }

    if (!data.length) return res.status(404).send('');

    const headers = ['Date','RPM','Speed','nGear','Throttle','Brake','DRS','Source','Time','SessionTime','Distance','Driver','Year','Race','LapNumber','LapTime','Compound','TyreLife','HeartRate_bpm','CockpitTemp_C','AirTemp_C','TrackTemp_C','Humidity_pct','BattleIntensity'];
    const lines = [headers.join(',')];
    for (const d of data) {
      lines.push(headers.map(h => String(d[h] ?? '')).join(','));
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    return res.send(lines.join('\n'));
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
