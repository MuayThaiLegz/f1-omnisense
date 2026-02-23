import type { VercelRequest, VercelResponse } from '@vercel/node';
import { MongoClient, GridFSBucket } from 'mongodb';

let cached: { client: MongoClient } | null = null;

async function getClient() {
  if (cached) return cached.client;
  const uri = process.env.MONGODB_URI!;
  const client = new MongoClient(uri);
  await client.connect();
  cached = { client };
  return client;
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    const requestedFile = (req.query.file as string) ?? '';
    if (!requestedFile.endsWith('.glb')) return res.status(404).send('Not found');

    // Map short names to GridFS filenames
    const GRIDFS_MAP: Record<string, string> = {
      'mcl38.glb': 'unpacked-mclaren_mcl38_lod_a.glb',
      'mcl39.glb': 'f1_2025_mclaren_mcl39.glb',
      'mcl60.glb': 'unpacked-mclaren_mcl60_lod_a.glb',
    };
    const filename = GRIDFS_MAP[requestedFile] ?? requestedFile;

    const client = await getClient();
    const db = client.db(process.env.MONGODB_DB || 'McLaren_f1');
    const bucket = new GridFSBucket(db);

    const files = await bucket.find({ filename }).toArray();
    if (!files.length) return res.status(404).send('Model not found');

    const file = files[0];
    const stream = bucket.openDownloadStreamByName(filename);

    res.setHeader('Content-Type', 'model/gltf-binary');
    res.setHeader('Cache-Control', 'public, max-age=604800');
    res.setHeader('Content-Length', String(file.length));
    res.setHeader('Content-Disposition', `inline; filename="${filename}"`);

    stream.pipe(res);
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
