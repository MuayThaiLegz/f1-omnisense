import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb } from './_db.js';

const COLLECTION = 'fleet_vehicles';

export default async function handler(req: VercelRequest, res: VercelResponse) {
  const db = await getDb();
  const col = db.collection(COLLECTION);

  try {
    if (req.method === 'GET') {
      const vehicles = await col.find({}, { projection: { _id: 0 } })
        .sort({ createdAt: -1 })
        .toArray();
      return res.json(vehicles);
    }

    if (req.method === 'POST') {
      const {
        model, driverName, driverNumber, driverCode,
        teamName, chassisId, engineSpec, season, notes,
      } = req.body ?? {};

      // Validate required fields
      if (!model || !driverName || !driverNumber || !driverCode) {
        return res.status(400).json({
          error: 'Missing required fields: model, driverName, driverNumber, driverCode',
        });
      }

      const doc = {
        model: String(model),
        driverName: String(driverName),
        driverNumber: Number(driverNumber),
        driverCode: String(driverCode).toUpperCase().slice(0, 3),
        teamName: String(teamName || 'McLaren'),
        chassisId: String(chassisId || ''),
        engineSpec: String(engineSpec || ''),
        season: Number(season) || new Date().getFullYear(),
        notes: String(notes || ''),
        createdAt: new Date(),
      };

      await col.insertOne(doc);
      const { _id, ...safe } = doc as any;
      return res.status(201).json(safe);
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
