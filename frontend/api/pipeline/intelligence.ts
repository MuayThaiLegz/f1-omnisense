import type { VercelRequest, VercelResponse } from '@vercel/node';
import { getDb } from '../_db.js';

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  try {
    const db = await getDb();
    const docs = await db.collection('f1_knowledge').find({}, { projection: { _id: 0, embedding: 0 } }).toArray();

    const rules: any[] = [];
    const equipment: any[] = [];
    const dimensionalData: any[] = [];
    const materialSpecs: any[] = [];

    for (const doc of docs) {
      const meta = doc.metadata ?? {};
      const dt = meta.data_type ?? '';
      const content = (doc.page_content as string) ?? '';

      if (dt === 'regulation') {
        const descLines = content.split('\n');
        rules.push({
          id: meta.rule_id ?? '', category: meta.category ?? '',
          description: descLines[1] ?? content,
          value: null, unit: null, condition: null, reference: null,
          severity: meta.severity ?? 'info',
          source_standard: meta.source ?? '', _source: meta.source ?? '', _page: meta.page ?? 0,
        });
      } else if (dt === 'equipment') {
        const lines = content.split('\n');
        let eqType = '', eqDesc = '', eqLocation: string | null = null;
        for (const line of lines) {
          if (line.startsWith('Type: ')) eqType = line.slice(6);
          else if (line.startsWith('Location: ')) eqLocation = line.slice(10);
          else if (!line.startsWith('[')) eqDesc = line;
        }
        equipment.push({
          tag: meta.tag ?? '', type: eqType || meta.category || '',
          description: eqDesc, kks: '', specs: {},
          location_description: eqLocation,
          _source: meta.source ?? '', _page: meta.page ?? 0,
        });
      } else if (dt === 'dimension') {
        const lines = content.split('\n');
        const dimDesc = lines[1] ?? '';
        let value: any = null, unit = '';
        for (const line of lines) {
          if (line.startsWith('Value: ')) {
            const valStr = line.slice(7).trim();
            const parts = valStr.split(/\s+/);
            const num = parseFloat(parts[0]);
            if (!isNaN(num)) { value = num; unit = parts.slice(1).join(' '); }
            else value = valStr;
          }
        }
        dimensionalData.push({
          component: meta.component ?? '', dimension: dimDesc,
          value, unit, _source: meta.source ?? '', _page: meta.page ?? 0,
        });
      } else if (dt === 'material') {
        const lines = content.split('\n');
        let application = '';
        for (const line of lines) {
          if (line.startsWith('Application: ')) application = line.slice(13);
        }
        materialSpecs.push({
          material: meta.material ?? '', application, properties: {},
          _source: meta.source ?? '', _page: meta.page ?? 0,
        });
      }
    }

    res.setHeader('Cache-Control', 's-maxage=600, stale-while-revalidate=1200');
    return res.json({
      documents: [], rules, equipment, dimensional_data: dimensionalData, material_specs: materialSpecs,
      stats: {
        total_pages: 0, total_rules: rules.length, total_equipment: equipment.length,
        total_dimensions: dimensionalData.length, total_materials: materialSpecs.length,
        total_tokens_in: 0, total_tokens_out: 0, total_cost_usd: 0, total_latency_s: 0,
      },
    });
  } catch (e: any) {
    return res.status(500).json({ error: e.message });
  }
}
