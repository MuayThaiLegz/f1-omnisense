import { useState, useEffect, useCallback, useRef } from 'react';
import {
  Upload, Box, Loader2, CheckCircle2, XCircle, RefreshCw,
  Paintbrush, Download, ChevronDown, Eye, Layers,
  Cpu, Sparkles, Palette,
} from 'lucide-react';
import * as api from '../api/model3d';
import type { MaterialPreset, GeneratedModel, Job, MeshQuality } from '../api/model3d';

// ─── Provider config ────────────────────────────────────────────────
const PROVIDERS = [
  { id: 'hunyuan' as const, label: 'Core', sublabel: 'Hunyuan3D', icon: Cpu, color: '#3b82f6', desc: 'Shape generation via Hunyuan3D + PBR materials' },
  // { id: 'meshy' as const, label: 'Pro', sublabel: 'Meshy.ai', icon: Sparkles, color: '#FF8000', desc: 'Production-quality textured 3D via Meshy API' },
  { id: 'texture_paint' as const, label: 'Paint', sublabel: 'TEXTure', icon: Palette, color: '#a855f7', desc: 'Local GPU texture painting on existing mesh' },
] as const;

type Provider = typeof PROVIDERS[number]['id'];

// ─── Component ──────────────────────────────────────────────────────
export function ModelGen3D() {
  // Upload state
  const [imageFile, setImageFile] = useState<File | null>(null);
  const [imagePreview, setImagePreview] = useState<string | null>(null);
  const [modelName, setModelName] = useState('');
  const [provider, setProvider] = useState<Provider>('hunyuan');
  const [isDragOver, setIsDragOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Provider options
  const [textured, setTextured] = useState(false);
  const [materialPreset, setMaterialPreset] = useState('carbon_fiber');
  const [texturePrompt, setTexturePrompt] = useState('');
  const [enablePbr, setEnablePbr] = useState(true);

  // Job tracking
  const [activeJob, setActiveJob] = useState<Job | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Data
  const [presets, setPresets] = useState<MaterialPreset[]>([]);
  const [models, setModels] = useState<GeneratedModel[]>([]);
  const [viewingModel, setViewingModel] = useState<string | null>(null);
  const [viewingProvider, setViewingProvider] = useState<string>('hunyuan');
  const [quality, setQuality] = useState<MeshQuality | null>(null);
  const [texturePaintAvail, setTexturePaintAvail] = useState<boolean | null>(null);

  // Load presets + models on mount
  useEffect(() => {
    api.getMaterialPresets().then(r => setPresets(r.presets)).catch(() => {});
    api.listModels().then(r => setModels(r.models)).catch(() => {});
    api.getTexturePaintStatus().then(r => setTexturePaintAvail(r.available)).catch(() => setTexturePaintAvail(false));
  }, []);

  // Cleanup poll on unmount
  useEffect(() => {
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, []);

  // ─── Image handling ─────────────────────────────────────────────
  const handleFile = useCallback((file: File) => {
    setImageFile(file);
    setImagePreview(URL.createObjectURL(file));
    if (!modelName) {
      setModelName(file.name.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_-]/g, '_'));
    }
  }, [modelName]);

  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file && file.type.startsWith('image/')) handleFile(file);
  }, [handleFile]);

  // ─── Submit generation ──────────────────────────────────────────
  const handleSubmit = async () => {
    if (!imageFile) return;
    setSubmitting(true);
    try {
      const result = await api.submitGeneration({
        image: imageFile,
        model_name: modelName || undefined,
        provider,
        textured: provider === 'hunyuan' ? textured : undefined,
        texture_prompt: texturePrompt || undefined,
        enable_pbr: enablePbr || undefined,
        material_preset: materialPreset || undefined,
      });

      // Start polling
      const job: Job = {
        job_id: result.job_id,
        model_name: result.model_name,
        provider: result.provider,
        status: 'queued',
        progress: 0,
        glb_url: null,
        error: null,
        created_at: new Date().toISOString(),
        completed_at: null,
      };
      setActiveJob(job);

      pollRef.current = setInterval(async () => {
        try {
          const updated = await api.getJobStatus(result.job_id);
          setActiveJob(updated);
          if (updated.status === 'completed' || updated.status === 'failed') {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = null;
            if (updated.status === 'completed') {
              api.listModels().then(r => setModels(r.models)).catch(() => {});
            }
          }
        } catch {
          // ignore poll errors
        }
      }, 3000);
    } catch (err) {
      setActiveJob({
        job_id: 'error',
        model_name: modelName,
        provider,
        status: 'failed',
        progress: 0,
        glb_url: null,
        error: err instanceof Error ? err.message : String(err),
        created_at: new Date().toISOString(),
        completed_at: null,
      });
    } finally {
      setSubmitting(false);
    }
  };

  // ─── Actions ────────────────────────────────────────────────────
  const handleApplyPbr = async (name: string, preset: string) => {
    try {
      await api.applyTexture(name, preset);
      api.listModels().then(r => setModels(r.models)).catch(() => {});
    } catch {
      // ignore
    }
  };

  const handleRegenMeshy = async (name: string) => {
    try {
      const result = await api.regenerateModel(name);
      setActiveJob({
        job_id: result.job_id,
        model_name: name,
        provider: 'meshy',
        status: 'queued',
        progress: 0,
        glb_url: null,
        error: null,
        created_at: new Date().toISOString(),
        completed_at: null,
      });
      pollRef.current = setInterval(async () => {
        try {
          const updated = await api.getJobStatus(result.job_id);
          setActiveJob(updated);
          if (updated.status === 'completed' || updated.status === 'failed') {
            if (pollRef.current) clearInterval(pollRef.current);
            pollRef.current = null;
            if (updated.status === 'completed') {
              api.listModels().then(r => setModels(r.models)).catch(() => {});
            }
          }
        } catch { /* ignore */ }
      }, 3000);
    } catch { /* ignore */ }
  };

  const handleViewQuality = async (name: string) => {
    try {
      const q = await api.getMeshQuality(name);
      setQuality(q);
    } catch { /* ignore */ }
  };

  // ─── Status helpers ──────────────────────────────────────────────
  const statusLabel = (s: string) => {
    switch (s) {
      case 'queued': return 'Queued…';
      case 'generating': return 'Generating 3D…';
      case 'painting_texture': return 'Painting texture…';
      case 'applying_material': return 'Applying PBR…';
      case 'completed': return 'Complete';
      case 'failed': return 'Failed';
      default: return s;
    }
  };

  const providerTag = (prov: string) => {
    const p = PROVIDERS.find(p => p.id === prov);
    return p ? { label: p.label, color: p.color } : { label: prov, color: '#666' };
  };

  // ─── Render ─────────────────────────────────────────────────────
  return (
    <div className="space-y-5">
      {/* Upload + Options Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Image Upload */}
        <div className="bg-[#1A1F2E] rounded-xl border border-[rgba(255,128,0,0.12)] p-4">
          <h3 className="text-sm font-medium text-foreground mb-3 flex items-center gap-2">
            <Upload className="w-3.5 h-3.5 text-[#FF8000]" />
            Reference Image
          </h3>

          <div
            onDragOver={(e) => { e.preventDefault(); setIsDragOver(true); }}
            onDragLeave={() => setIsDragOver(false)}
            onDrop={onDrop}
            onClick={() => fileInputRef.current?.click()}
            className={`relative border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-all ${
              isDragOver
                ? 'border-[#FF8000] bg-[#FF8000]/5'
                : imagePreview
                  ? 'border-[rgba(255,128,0,0.2)] bg-[#0D1117]'
                  : 'border-[rgba(255,128,0,0.12)] hover:border-[rgba(255,128,0,0.3)] bg-[#0D1117]'
            }`}
          >
            {imagePreview ? (
              <img src={imagePreview} alt="Preview" className="max-h-48 mx-auto rounded-lg object-contain" />
            ) : (
              <div className="space-y-2">
                <Box className="w-8 h-8 mx-auto text-muted-foreground" />
                <p className="text-sm text-muted-foreground">Drop an F1 car image here or click to browse</p>
                <p className="text-[12px] text-muted-foreground/60">PNG, JPG up to 10MB</p>
              </div>
            )}
            <input
              ref={fileInputRef}
              type="file"
              accept="image/*"
              className="hidden"
              onChange={(e) => { if (e.target.files?.[0]) handleFile(e.target.files[0]); }}
            />
          </div>

          {/* Model name */}
          <div className="mt-3">
            <label className="text-[12px] text-muted-foreground mb-1 block">Model Name</label>
            <input
              type="text"
              value={modelName}
              onChange={(e) => setModelName(e.target.value)}
              placeholder="e.g. mcl38_front"
              className="w-full bg-[#0D1117] border border-[rgba(255,128,0,0.12)] rounded-lg px-3 py-1.5 text-sm text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:border-[#FF8000]/40"
            />
          </div>
        </div>

        {/* Provider + Options */}
        <div className="bg-[#1A1F2E] rounded-xl border border-[rgba(255,128,0,0.12)] p-4">
          <h3 className="text-sm font-medium text-foreground mb-3 flex items-center gap-2">
            <Layers className="w-3.5 h-3.5 text-[#FF8000]" />
            Generation Options
          </h3>

          {/* Provider buttons */}
          <div className="grid grid-cols-3 gap-2 mb-4">
            {PROVIDERS.map((p) => {
              const Icon = p.icon;
              const disabled = p.id === 'texture_paint' && texturePaintAvail === false;
              return (
                <button
                  key={p.id}
                  onClick={() => !disabled && setProvider(p.id)}
                  disabled={disabled}
                  className={`relative rounded-lg p-3 text-center transition-all border ${
                    provider === p.id
                      ? 'border-current bg-current/10'
                      : disabled
                        ? 'border-[rgba(255,128,0,0.12)] bg-[#0D1117] opacity-40 cursor-not-allowed'
                        : 'border-[rgba(255,128,0,0.12)] bg-[#0D1117] hover:border-[rgba(255,128,0,0.2)]'
                  }`}
                  style={provider === p.id ? { color: p.color, borderColor: p.color } : undefined}
                >
                  <Icon className="w-4 h-4 mx-auto mb-1" style={provider === p.id ? { color: p.color } : undefined} />
                  <div className="text-[12px] font-medium" style={provider === p.id ? { color: p.color } : undefined}>{p.label}</div>
                  <div className="text-[11px] text-muted-foreground">{p.sublabel}</div>
                </button>
              );
            })}
          </div>

          <p className="text-[12px] text-muted-foreground mb-3">
            {PROVIDERS.find(p => p.id === provider)?.desc}
          </p>

          {/* Hunyuan options */}
          {provider === 'hunyuan' && (
            <div className="space-y-2">
              <label className="flex items-center gap-2 text-[12px] text-muted-foreground cursor-pointer">
                <input type="checkbox" checked={textured} onChange={(e) => setTextured(e.target.checked)}
                  className="rounded border-[rgba(255,128,0,0.2)] bg-[#0D1117] text-[#FF8000] focus:ring-[#FF8000]/30 w-3 h-3" />
                Generate with texture (slower, full color)
              </label>
              {!textured && (
                <div>
                  <label className="text-[12px] text-muted-foreground mb-1 block">PBR Material Preset</label>
                  <div className="relative">
                    <select
                      value={materialPreset}
                      onChange={(e) => setMaterialPreset(e.target.value)}
                      className="w-full bg-[#0D1117] border border-[rgba(255,128,0,0.12)] rounded-lg px-3 py-1.5 text-sm text-foreground appearance-none focus:outline-none focus:border-[#FF8000]/40"
                    >
                      {presets.map(p => <option key={p.value} value={p.value}>{p.label}</option>)}
                    </select>
                    <ChevronDown className="w-3 h-3 absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none" />
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Meshy options — disabled, provider commented out */}

          {/* TEXTure options */}
          {provider === 'texture_paint' && (
            <div className="space-y-2">
              <div>
                <label className="text-[12px] text-muted-foreground mb-1 block">Paint Prompt</label>
                <input
                  type="text"
                  value={texturePrompt}
                  onChange={(e) => setTexturePrompt(e.target.value)}
                  placeholder="e.g. Formula 1 car with McLaren papaya livery"
                  className="w-full bg-[#0D1117] border border-[rgba(255,128,0,0.12)] rounded-lg px-3 py-1.5 text-sm text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:border-[#FF8000]/40"
                />
              </div>
              <div>
                <label className="text-[12px] text-muted-foreground mb-1 block">Material Preset</label>
                <div className="relative">
                  <select
                    value={materialPreset}
                    onChange={(e) => setMaterialPreset(e.target.value)}
                    className="w-full bg-[#0D1117] border border-[rgba(255,128,0,0.12)] rounded-lg px-3 py-1.5 text-sm text-foreground appearance-none focus:outline-none focus:border-[#FF8000]/40"
                  >
                    {presets.map(p => <option key={p.value} value={p.value}>{p.label}</option>)}
                  </select>
                  <ChevronDown className="w-3 h-3 absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none" />
                </div>
              </div>
            </div>
          )}

          {/* Submit button */}
          <button
            onClick={handleSubmit}
            disabled={!imageFile || submitting || (activeJob?.status === 'generating' || activeJob?.status === 'queued')}
            className="mt-4 w-full flex items-center justify-center gap-2 rounded-lg py-2.5 text-sm font-medium transition-all disabled:opacity-40 disabled:cursor-not-allowed"
            style={{
              background: `linear-gradient(135deg, ${PROVIDERS.find(p => p.id === provider)?.color}22, ${PROVIDERS.find(p => p.id === provider)?.color}11)`,
              border: `1px solid ${PROVIDERS.find(p => p.id === provider)?.color}44`,
              color: PROVIDERS.find(p => p.id === provider)?.color,
            }}
          >
            {submitting ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Sparkles className="w-3.5 h-3.5" />}
            Generate 3D Model
          </button>
        </div>
      </div>

      {/* Active Job Progress */}
      {activeJob && (
        <div className="bg-[#1A1F2E] rounded-xl border border-[rgba(255,128,0,0.12)] p-4">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-sm font-medium text-foreground flex items-center gap-2">
              {activeJob.status === 'completed' ? (
                <CheckCircle2 className="w-3.5 h-3.5 text-green-400" />
              ) : activeJob.status === 'failed' ? (
                <XCircle className="w-3.5 h-3.5 text-red-400" />
              ) : (
                <Loader2 className="w-3.5 h-3.5 text-[#FF8000] animate-spin" />
              )}
              {activeJob.model_name}
            </h3>
            <span className="text-[12px] px-2 py-0.5 rounded-full"
              style={{ color: providerTag(activeJob.provider).color, background: `${providerTag(activeJob.provider).color}15` }}>
              {providerTag(activeJob.provider).label}
            </span>
          </div>

          {/* Progress bar */}
          <div className="h-1.5 bg-[#0D1117] rounded-full overflow-hidden mb-2">
            <div
              className="h-full rounded-full transition-all duration-500"
              style={{
                width: `${activeJob.progress}%`,
                background: activeJob.status === 'failed' ? '#ef4444' : providerTag(activeJob.provider).color,
              }}
            />
          </div>
          <div className="flex items-center justify-between text-[12px]">
            <span className="text-muted-foreground">{statusLabel(activeJob.status)}</span>
            <span className="text-muted-foreground">{activeJob.progress}%</span>
          </div>
          {activeJob.error && (
            <p className="text-[12px] text-red-400 mt-2">{activeJob.error}</p>
          )}

          {/* View completed model */}
          {activeJob.status === 'completed' && activeJob.glb_url && (
            <div className="mt-3 flex gap-2">
              <button
                onClick={() => { setViewingModel(activeJob.model_name); setViewingProvider(activeJob.provider); }}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[12px] bg-[#FF8000]/10 text-[#FF8000] border border-[#FF8000]/20 hover:bg-[#FF8000]/20 transition-all"
              >
                <Eye className="w-3 h-3" /> View 3D
              </button>
              <a
                href={activeJob.glb_url}
                download
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[12px] bg-[#222838] text-muted-foreground border border-[rgba(255,128,0,0.12)] hover:text-foreground transition-all"
              >
                <Download className="w-3 h-3" /> Download GLB
              </a>
            </div>
          )}
        </div>
      )}

      {/* GLB Viewer */}
      {viewingModel && (
        <div className="bg-[#1A1F2E] rounded-xl border border-[rgba(255,128,0,0.12)] overflow-hidden">
          <div className="flex items-center justify-between px-4 py-2 border-b border-[rgba(255,128,0,0.12)]">
            <h3 className="text-sm font-medium text-foreground flex items-center gap-2">
              <Eye className="w-3.5 h-3.5 text-[#FF8000]" />
              {viewingModel}
            </h3>
            <button
              onClick={() => setViewingModel(null)}
              className="text-[12px] text-muted-foreground hover:text-foreground transition-colors"
            >
              Close
            </button>
          </div>
          <iframe
            src={`/glb_viewer.html?url=${encodeURIComponent(api.getGlbUrl(viewingModel, viewingProvider))}`}
            className="w-full h-[500px] border-0 bg-black"
            title="3D Model Viewer"
          />
        </div>
      )}

      {/* Generated Models Gallery */}
      {models.length > 0 && (
        <div className="bg-[#1A1F2E] rounded-xl border border-[rgba(255,128,0,0.12)] p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-medium text-foreground flex items-center gap-2">
              <Box className="w-3.5 h-3.5 text-[#FF8000]" />
              Generated Models ({models.length})
            </h3>
            <button
              onClick={() => api.listModels().then(r => setModels(r.models)).catch(() => {})}
              className="text-[12px] text-muted-foreground hover:text-foreground transition-colors flex items-center gap-1"
            >
              <RefreshCw className="w-3 h-3" /> Refresh
            </button>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
            {models.map((m) => (
              <div key={m.model_name} className="bg-[#0D1117] rounded-lg border border-[rgba(255,128,0,0.12)] p-3">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-medium text-foreground truncate">{m.model_name}</span>
                  <div className="flex gap-1">
                    {m.has_hunyuan && (
                      <span className="text-[11px] px-1.5 py-0.5 rounded-full bg-[#3b82f6]/10 text-[#3b82f6]">Core</span>
                    )}
                    {m.has_meshy && (
                      <span className="text-[11px] px-1.5 py-0.5 rounded-full bg-[#FF8000]/10 text-[#FF8000]">Pro</span>
                    )}
                    {m.has_pbr && (
                      <span className="text-[11px] px-1.5 py-0.5 rounded-full bg-green-500/10 text-green-400">PBR</span>
                    )}
                    {m.has_texture_paint && (
                      <span className="text-[11px] px-1.5 py-0.5 rounded-full bg-[#a855f7]/10 text-[#a855f7]">Paint</span>
                    )}
                  </div>
                </div>

                {/* Actions */}
                <div className="flex flex-wrap gap-1.5 mt-2">
                  {m.has_hunyuan && (
                    <button
                      onClick={() => { setViewingModel(m.model_name); setViewingProvider('hunyuan'); }}
                      className="flex items-center gap-1 px-2 py-1 rounded text-[11px] bg-[#3b82f6]/10 text-[#3b82f6] hover:bg-[#3b82f6]/20 transition-all"
                    >
                      <Eye className="w-2.5 h-2.5" /> View
                    </button>
                  )}
                  {m.has_meshy && (
                    <button
                      onClick={() => { setViewingModel(m.model_name); setViewingProvider('meshy'); }}
                      className="flex items-center gap-1 px-2 py-1 rounded text-[11px] bg-[#FF8000]/10 text-[#FF8000] hover:bg-[#FF8000]/20 transition-all"
                    >
                      <Eye className="w-2.5 h-2.5" /> Pro
                    </button>
                  )}
                  {m.has_texture_paint && (
                    <button
                      onClick={() => { setViewingModel(m.model_name); setViewingProvider('texture_paint'); }}
                      className="flex items-center gap-1 px-2 py-1 rounded text-[11px] bg-[#a855f7]/10 text-[#a855f7] hover:bg-[#a855f7]/20 transition-all"
                    >
                      <Eye className="w-2.5 h-2.5" /> Paint
                    </button>
                  )}
                  {m.has_hunyuan && !m.has_pbr && (
                    <button
                      onClick={() => handleApplyPbr(m.model_name, materialPreset)}
                      className="flex items-center gap-1 px-2 py-1 rounded text-[11px] bg-green-500/10 text-green-400 hover:bg-green-500/20 transition-all"
                    >
                      <Paintbrush className="w-2.5 h-2.5" /> +PBR
                    </button>
                  )}
                  {!m.has_meshy && (
                    <button
                      onClick={() => handleRegenMeshy(m.model_name)}
                      className="flex items-center gap-1 px-2 py-1 rounded text-[11px] bg-[#FF8000]/10 text-[#FF8000] hover:bg-[#FF8000]/20 transition-all"
                    >
                      <RefreshCw className="w-2.5 h-2.5" /> +Pro
                    </button>
                  )}
                  <button
                    onClick={() => handleViewQuality(m.model_name)}
                    className="flex items-center gap-1 px-2 py-1 rounded text-[11px] bg-[#222838] text-muted-foreground hover:text-foreground transition-all"
                  >
                    <Layers className="w-2.5 h-2.5" /> Quality
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Mesh Quality Panel */}
      {quality && (
        <div className="bg-[#1A1F2E] rounded-xl border border-[rgba(255,128,0,0.12)] p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-medium text-foreground">
              Mesh Quality — {quality.model_name}
            </h3>
            <button onClick={() => setQuality(null)} className="text-[12px] text-muted-foreground hover:text-foreground">Close</button>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {quality.generated.map((g) => (
              <div key={g.file} className="bg-[#0D1117] rounded-lg p-3 border border-[rgba(255,128,0,0.12)]">
                <div className="text-[12px] font-mono text-foreground mb-2">{g.file}</div>
                <div className="space-y-1 text-[12px]">
                  {g.vertices != null && (
                    <div className="flex justify-between"><span className="text-muted-foreground">Vertices</span><span className="text-foreground">{g.vertices.toLocaleString()}</span></div>
                  )}
                  {g.faces != null && (
                    <div className="flex justify-between"><span className="text-muted-foreground">Faces</span><span className="text-foreground">{g.faces.toLocaleString()}</span></div>
                  )}
                  {g.bbox && (
                    <div className="flex justify-between"><span className="text-muted-foreground">Bbox</span><span className="text-foreground font-mono">{g.bbox.join(' × ')}</span></div>
                  )}
                  <div className="flex justify-between"><span className="text-muted-foreground">Size</span><span className="text-foreground">{(g.file_size_bytes / 1024).toFixed(0)} KB</span></div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
