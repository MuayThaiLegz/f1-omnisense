import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb } from '../_db.js';

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  try {
    const db = await getDb();
    const snapshot = await db.collection('anomaly_scores_snapshot').findOne({}, { projection: { _id: 0 } });
    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate=600');
    return res.json(snapshot ?? {});
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
