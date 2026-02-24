/**
 * API client for the 3D Model Generation service (port 8101).
 */

const BASE = '/api/3d-gen';

export interface GenerateParams {
  image: File;
  model_name?: string;
  provider: 'hunyuan' | 'meshy' | 'tripo' | 'trellis' | 'texture_paint';
  textured?: boolean;
  steps?: number;
  guidance_scale?: number;
  seed?: number;
  octree_resolution?: number;
  randomize_seed?: boolean;
  texture_prompt?: string;
  enable_pbr?: boolean;
  target_polycount?: number;
  material_preset?: string;
  multi_view?: boolean;
  n_views?: number;
  texture_resolution?: number;
}

export interface Job {
  job_id: string;
  model_name: string;
  provider: string;
  status: 'queued' | 'generating' | 'painting_texture' | 'applying_material' | 'completed' | 'failed';
  progress: number;
  glb_url: string | null;
  error: string | null;
  created_at: string;
  completed_at: string | null;
}

export interface GeneratedModel {
  model_name: string;
  directory: string;
  has_hunyuan: boolean;
  has_meshy: boolean;
  has_pbr: boolean;
  has_texture_paint: boolean;
  [key: string]: unknown;
}

export interface MaterialPreset {
  value: string;
  label: string;
  baseColor: number[];
  metallic: number;
  roughness: number;
}

export interface MeshQuality {
  model_name: string;
  generated: {
    file: string;
    vertices?: number;
    faces?: number;
    bbox?: number[];
    file_size_bytes: number;
  }[];
}

export async function submitGeneration(params: GenerateParams): Promise<{ job_id: string; status: string; provider: string; model_name: string }> {
  const form = new FormData();
  form.append('image', params.image);
  if (params.model_name) form.append('model_name', params.model_name);
  form.append('provider', params.provider);
  if (params.textured != null) form.append('textured', String(params.textured));
  if (params.steps != null) form.append('steps', String(params.steps));
  if (params.guidance_scale != null) form.append('guidance_scale', String(params.guidance_scale));
  if (params.seed != null) form.append('seed', String(params.seed));
  if (params.octree_resolution != null) form.append('octree_resolution', String(params.octree_resolution));
  if (params.randomize_seed != null) form.append('randomize_seed', String(params.randomize_seed));
  if (params.texture_prompt) form.append('texture_prompt', params.texture_prompt);
  if (params.enable_pbr != null) form.append('enable_pbr', String(params.enable_pbr));
  if (params.target_polycount != null) form.append('target_polycount', String(params.target_polycount));
  if (params.material_preset) form.append('material_preset', params.material_preset);
  if (params.multi_view != null) form.append('multi_view', String(params.multi_view));
  if (params.n_views != null) form.append('n_views', String(params.n_views));
  if (params.texture_resolution != null) form.append('texture_resolution', String(params.texture_resolution));

  const res = await fetch(`${BASE}/generate`, { method: 'POST', body: form });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function regenerateModel(modelName: string, opts?: {
  texture_prompt?: string;
  enable_pbr?: boolean;
  target_polycount?: number;
}): Promise<{ job_id: string; status: string; provider: string }> {
  const res = await fetch(`${BASE}/regenerate/${modelName}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(opts ?? {}),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function getJobStatus(jobId: string): Promise<Job> {
  const res = await fetch(`${BASE}/jobs/${jobId}`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function listJobs(): Promise<{ jobs: Job[] }> {
  const res = await fetch(`${BASE}/jobs`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function listModels(): Promise<{ models: GeneratedModel[] }> {
  const res = await fetch(`${BASE}/models`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export function getGlbUrl(modelName: string, provider: string, textured = false): string {
  return `${BASE}/models/${modelName}/glb?provider=${provider}&textured=${textured}`;
}

export async function applyTexture(modelName: string, preset?: string): Promise<Record<string, unknown>> {
  const url = preset ? `${BASE}/apply-texture/${modelName}?preset=${preset}` : `${BASE}/apply-texture/${modelName}`;
  const res = await fetch(url, { method: 'POST' });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function getMeshQuality(modelName: string): Promise<MeshQuality> {
  const res = await fetch(`${BASE}/quality/${modelName}`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function getTexturePaintStatus(): Promise<{ available: boolean; gpu: string; vram_gb: number; reason?: string }> {
  const res = await fetch(`${BASE}/texture-paint/status`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function getMaterialPresets(): Promise<{ presets: MaterialPreset[] }> {
  const res = await fetch(`${BASE}/material-presets`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}
