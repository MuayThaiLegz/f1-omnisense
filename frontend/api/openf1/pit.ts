import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb } from '../_db.js';

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const db = await getDb();
    const filter: Record<string, any> = {};
    for (const [k, v] of Object.entries(req.query)) {
      if (typeof v !== 'string') continue;
      const num = Number(v);
      filter[k] = !isNaN(num) && v.trim() !== '' ? num : v;
    }
    const docs = await db.collection('openf1_pit').find(filter, { projection: { _id: 0 } }).toArray();
    res.setHeader('Cache-Control', 's-maxage=60, stale-while-revalidate=120');
    return res.json(docs);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
