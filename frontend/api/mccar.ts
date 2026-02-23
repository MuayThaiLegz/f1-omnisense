import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb } from './_db.js';

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const fullPath = (req.query.path as string) ?? '';
    // path = 2024/2024_Bahrain_Grand_Prix_Race.csv
    const m = fullPath.match(/^(\d{4})\/(.+)\.csv$/);
    if (!m) return res.status(404).send('');

    const year = m[1];
    const filename = m[2]; // e.g. 2024_Bahrain_Grand_Prix_Race

    const db = await getDb();
    const docs = await db.collection('telemetry').find(
      { _source_file: filename, Year: year },
      { projection: { _id: 0 } }
    ).toArray();

    // If no match by _source_file, try matching by Race name
    let data = docs;
    if (!data.length) {
      // Extract race name: 2024_Bahrain_Grand_Prix_Race -> Bahrain
      // or 2024_Saudi_Arabian_Grand_Prix_Race -> Saudi Arabian
      const raceName = filename
        .replace(/^\d{4}_/, '')
        .replace(/_Grand_Prix_Race$/, '')
        .replace(/_Race$/, '')
        .replace(/_/g, ' ');
      data = await db.collection('telemetry').find(
        { Race: { $regex: `^${raceName}`, $options: 'i' }, Year: year },
        { projection: { _id: 0 } }
      ).toArray();
    }

    if (!data.length) return res.status(404).send('');

    const headers = ['Date','RPM','Speed','nGear','Throttle','Brake','DRS','Source','Time','SessionTime','Distance','Driver','Year','Race','LapNumber','LapTime','Compound','TyreLife'];
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
