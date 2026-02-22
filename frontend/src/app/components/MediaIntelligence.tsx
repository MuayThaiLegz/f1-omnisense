import { useState, useEffect } from 'react';
import {
  Brain, Eye, Video, Scan, Loader2, Search, Tag, ImageIcon,
} from 'lucide-react';
import { pipeline } from '../api/local';

interface GDinoFrame {
  frame_index: number;
  detections: { category: string; score: number; bbox: number[] }[];
  output_image: string;
}

interface VideoModelResult {
  total_frames: number;
  fps: number;
  inference_time_s: number;
  top_predictions: { label: string; score: number }[];
}

interface MiniCPMFrame {
  frame_index: number;
  narration: string;
  tokens: number;
  time_s: number;
  tok_per_s: number;
}

interface VisualSearchResult {
  path: string;
  score: number;
  auto_tags: { label: string; score: number }[];
  source_video: string;
  frame_index: number;
}

interface VisualTagImage {
  path: string;
  auto_tags: { label: string; score: number }[];
  source_video: string;
  frame_index: number;
}

const modelStatus = [
  { name: 'Gemma 3 4B', type: 'Vision-Language', status: 'ready', framework: 'Ollama', purpose: 'Scene narration, tire commentary, pit lane description' },
  { name: 'CLIP', type: 'Image-Text Similarity', status: 'ready', framework: 'PyTorch', purpose: 'Gemma narration ↔ frame embedding similarity scoring' },
  { name: 'GroundingDINO', type: 'Object Detection', status: 'ready', framework: 'PyTorch', purpose: 'Text-grounded object detection in F1 media' },
  { name: 'SAM2', type: 'Segmentation', status: 'ready', framework: 'PyTorch', purpose: 'Segment Anything for component isolation' },
  { name: 'VideoMAE', type: 'Video Understanding', status: 'ready', framework: 'Transformers', purpose: 'Self-supervised video representation learning' },
  { name: 'TimeSformer', type: 'Temporal Analysis', status: 'ready', framework: 'Transformers', purpose: 'Divided space-time attention for video' },
];

export function MediaIntelligence() {
  const [gdinoData, setGdinoData] = useState<Record<string, GDinoFrame[]> | null>(null);
  const [fusedData, setFusedData] = useState<Record<string, GDinoFrame[]> | null>(null);
  const [minicpmData, setMinicpmData] = useState<Record<string, MiniCPMFrame[]> | null>(null);
  const [videomaeData, setVideomaeData] = useState<Record<string, VideoModelResult> | null>(null);
  const [timesformerData, setTimesformerData] = useState<Record<string, VideoModelResult> | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedImage, setSelectedImage] = useState<string | null>(null);
  const [expandedVideo, setExpandedVideo] = useState<string | null>(null);
  const [clipQuery, setClipQuery] = useState('');
  const [clipResults, setClipResults] = useState<VisualSearchResult[] | null>(null);
  const [clipSearching, setClipSearching] = useState(false);
  const [allTags, setAllTags] = useState<VisualTagImage[] | null>(null);
  const [selectedTag, setSelectedTag] = useState<string | null>(null);
  const [topTags, setTopTags] = useState<{ label: string; max_score: number }[]>([]);

  useEffect(() => {
    Promise.allSettled([
      pipeline.gdino().then(setGdinoData),
      pipeline.fused().then(setFusedData),
      pipeline.minicpm().then(setMinicpmData),
      pipeline.videomae().then(setVideomaeData),
      pipeline.timesformer().then(setTimesformerData),
    ]).finally(() => setLoading(false));

    // Load CLIP auto-tags
    fetch('/api/visual-tags')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data) {
          setAllTags(data.images);
          setTopTags(data.tags || []);
        }
      })
      .catch(() => {});
  }, []);

  const doClipSearch = async (query: string) => {
    if (!query.trim()) { setClipResults(null); return; }
    setClipSearching(true);
    try {
      const res = await fetch(`/api/visual-search?q=${encodeURIComponent(query)}&k=12`);
      if (res.ok) {
        const data = await res.json();
        setClipResults(data.results);
      }
    } catch { /* server not running */ }
    finally { setClipSearching(false); }
  };

  const filteredByTag = selectedTag && allTags
    ? allTags.filter(img => img.auto_tags.some(t => t.label === selectedTag))
    : null;

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-6 h-6 text-[#FF8000] animate-spin" />
        <span className="ml-3 text-muted-foreground text-sm">Loading media pipeline results...</span>
      </div>
    );
  }

  const totalDetections = gdinoData
    ? Object.values(gdinoData).flat().reduce((s, f) => s + f.detections.length, 0)
    : 0;
  const totalFramesAnalyzed = gdinoData ? Object.values(gdinoData).flat().length : 0;
  const totalNarrations = minicpmData ? Object.values(minicpmData).flat().length : 0;
  const categories = gdinoData
    ? [...new Set(Object.values(gdinoData).flat().flatMap(f => f.detections.map(d => d.category)))]
    : [];

  const pipelineStats = [
    { label: 'Videos Processed', value: gdinoData ? String(Object.keys(gdinoData).length) : '—' },
    { label: 'Frames Analyzed', value: String(totalFramesAnalyzed) },
    { label: 'Detections', value: String(totalDetections) },
    { label: 'VLM Narrations', value: String(totalNarrations) },
    { label: 'VideoMAE Runs', value: videomaeData ? String(Object.keys(videomaeData).length) : '—' },
    { label: 'Models Available', value: String(modelStatus.length) },
  ];

  return (
    <div className="space-y-4">
      {/* Pipeline Status Bar */}
      <div className="bg-[#12121e] border border-[rgba(255,128,0,0.12)] rounded-xl p-4">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-[#FF8000]/10 flex items-center justify-center">
              <Brain className="w-4 h-4 text-[#FF8000]" />
            </div>
            <div>
              <h3 className="text-sm text-foreground">Media Analysis Pipeline</h3>
              <div className="text-[10px] text-muted-foreground">GroundingDINO + SAM2 + VideoMAE + TimeSformer + Gemma 3 + CLIP</div>
            </div>
          </div>
          <span className="flex items-center gap-1.5 text-[10px] text-green-400 bg-green-500/10 px-2 py-1 rounded-full">
            <span className="w-1.5 h-1.5 rounded-full bg-green-400 animate-pulse" />
            Results Loaded
          </span>
        </div>
        <div className="grid grid-cols-6 gap-3">
          {pipelineStats.map((stat) => (
            <div key={stat.label} className="bg-[#0a0a12] rounded-lg p-2">
              <div className="text-[9px] text-muted-foreground tracking-wider mb-1">{stat.label}</div>
              <div className="text-sm font-mono text-foreground">{stat.value}</div>
            </div>
          ))}
        </div>
      </div>

      {/* CLIP Visual Search */}
      <div className="bg-[#12121e] border border-[rgba(255,128,0,0.12)] rounded-xl p-4">
        <div className="flex items-center gap-3 mb-3">
          <div className="w-8 h-8 rounded-lg bg-purple-500/10 flex items-center justify-center">
            <ImageIcon className="w-4 h-4 text-purple-400" />
          </div>
          <div className="flex-1">
            <h3 className="text-sm text-foreground">CLIP Visual Search</h3>
            <div className="text-[10px] text-muted-foreground">Search images by text description — CLIP ViT-B/32 (512-dim)</div>
          </div>
        </div>

        {/* Search Bar */}
        <div className="flex items-center gap-2 mb-3">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <input
              type="text"
              placeholder="Search: &quot;pit stop crew&quot;, &quot;rain conditions&quot;, &quot;cockpit view&quot;..."
              value={clipQuery}
              onChange={e => setClipQuery(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') doClipSearch(clipQuery); }}
              className="w-full bg-[#0a0a12] border border-[rgba(255,128,0,0.12)] rounded-lg pl-10 pr-4 py-2 text-xs text-foreground placeholder:text-muted-foreground focus:outline-none focus:border-purple-400/40"
            />
          </div>
          <button
            onClick={() => doClipSearch(clipQuery)}
            disabled={clipSearching || !clipQuery.trim()}
            className="px-4 py-2 bg-purple-500/20 text-purple-400 text-xs rounded-lg hover:bg-purple-500/30 disabled:opacity-30 transition-colors"
          >
            {clipSearching ? <Loader2 className="w-3 h-3 animate-spin" /> : 'Search'}
          </button>
        </div>

        {/* Auto-Tag Filter Chips */}
        {topTags.length > 0 && (
          <div className="flex items-center gap-1.5 flex-wrap mb-3">
            <Tag className="w-3 h-3 text-muted-foreground shrink-0" />
            <button
              onClick={() => { setSelectedTag(null); setClipResults(null); }}
              className={`text-[9px] px-2 py-0.5 rounded-full transition-all ${
                !selectedTag ? 'bg-purple-500/20 text-purple-400' : 'text-muted-foreground hover:bg-[#1a1a2e]'
              }`}
            >
              All
            </button>
            {topTags.slice(0, 12).map(tag => {
              const shortLabel = tag.label.replace(/formula one |on a formula one car|of a formula one car/g, '').replace(/ car$/, '').trim();
              return (
                <button
                  key={tag.label}
                  onClick={() => { setSelectedTag(selectedTag === tag.label ? null : tag.label); setClipResults(null); }}
                  className={`text-[9px] px-2 py-0.5 rounded-full transition-all ${
                    selectedTag === tag.label ? 'bg-purple-500/20 text-purple-400' : 'text-muted-foreground hover:bg-[#1a1a2e]'
                  }`}
                >
                  {shortLabel}
                </button>
              );
            })}
          </div>
        )}

        {/* Search Results */}
        {clipResults && (
          <div>
            <div className="text-[10px] text-muted-foreground mb-2">{clipResults.length} results for &quot;{clipQuery}&quot;</div>
            <div className="grid grid-cols-6 gap-2">
              {clipResults.map((r, i) => (
                <div key={r.path} className="relative rounded-lg overflow-hidden border border-[rgba(255,128,0,0.08)] hover:border-purple-400/30 transition-all group">
                  <img
                    src={`/media/${r.path}`}
                    alt={r.path}
                    className="w-full h-20 object-cover"
                  />
                  <div className="absolute inset-0 bg-gradient-to-t from-black/80 via-transparent to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
                  <div className="absolute bottom-0 left-0 right-0 px-1.5 py-1 flex items-center justify-between">
                    <span className="text-[8px] text-purple-400 font-mono font-bold">{(r.score * 100).toFixed(1)}%</span>
                    <span className="text-[7px] text-muted-foreground">#{i + 1}</span>
                  </div>
                  <div className="absolute top-0 left-0 right-0 px-1.5 py-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                    <span className="text-[7px] text-white/80">{r.source_video} f{r.frame_index}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Tag-Filtered Results */}
        {!clipResults && filteredByTag && (
          <div>
            <div className="text-[10px] text-muted-foreground mb-2">{filteredByTag.length} images tagged &quot;{selectedTag}&quot;</div>
            <div className="grid grid-cols-6 gap-2">
              {filteredByTag.map(img => {
                const tagScore = img.auto_tags.find(t => t.label === selectedTag)?.score ?? 0;
                return (
                  <div key={img.path} className="relative rounded-lg overflow-hidden border border-[rgba(255,128,0,0.08)] hover:border-purple-400/30 transition-all group">
                    <img
                      src={`/media/${img.path}`}
                      alt={img.path}
                      className="w-full h-20 object-cover"
                    />
                    <div className="absolute bottom-0 left-0 right-0 px-1.5 py-0.5 bg-black/70">
                      <span className="text-[8px] text-purple-400 font-mono">{(tagScore * 100).toFixed(1)}%</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>

      <div className="grid grid-cols-12 gap-4">
        {/* Left — Results Feed */}
        <div className="col-span-7 space-y-4">
          {/* Detection Gallery */}
          {(fusedData || gdinoData) && (
            <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
              <h3 className="text-xs text-muted-foreground tracking-widest mb-3 flex items-center gap-2">
                <Scan className="w-3 h-3" />
                DETECTION GALLERY — {totalDetections} objects across {totalFramesAnalyzed} frames
              </h3>
              <div className="text-[10px] text-muted-foreground mb-3">
                Categories: {categories.map(c => (
                  <span key={c} className="inline-block px-1.5 py-0.5 rounded bg-[#FF8000]/10 text-[#FF8000] mr-1 mb-1">{c}</span>
                ))}
              </div>
              <div className="grid grid-cols-4 gap-2">
                {Object.entries(fusedData ?? gdinoData!).flatMap(([video, frames]) =>
                  frames.map((frame) => (
                    <button
                      key={`${video}-${frame.frame_index}`}
                      type="button"
                      onClick={() => setSelectedImage(selectedImage === frame.output_image ? null : frame.output_image)}
                      className={`relative rounded-lg overflow-hidden border transition-all ${
                        selectedImage === frame.output_image ? 'border-[#FF8000]' : 'border-[rgba(255,128,0,0.08)] hover:border-[#FF8000]/30'
                      }`}
                    >
                      <img
                        src={`/media/fused_results/${frame.output_image}`}
                        alt={frame.output_image}
                        className="w-full h-20 object-cover"
                        onError={(e) => { (e.target as HTMLImageElement).src = `/media/gdino_results/${frame.output_image}`; }}
                      />
                      <div className="absolute bottom-0 left-0 right-0 bg-black/70 px-1.5 py-0.5 flex items-center justify-between">
                        <span className="text-[8px] text-foreground font-mono">{frame.detections.length} det</span>
                        <span className="text-[7px] text-muted-foreground">f{frame.frame_index}</span>
                      </div>
                    </button>
                  ))
                )}
              </div>
              {selectedImage && (
                <div className="mt-4 space-y-2">
                  <div className="rounded-lg overflow-hidden border border-[#FF8000]/20">
                    <img
                      src={`/media/fused_results/${selectedImage}`}
                      alt="Detection detail"
                      className="w-full"
                      onError={(e) => { (e.target as HTMLImageElement).src = `/media/gdino_results/${selectedImage}`; }}
                    />
                  </div>
                  {/* Show detections for selected frame */}
                  {(() => {
                    const frame = Object.values(fusedData ?? gdinoData!).flat().find(f => f.output_image === selectedImage);
                    if (!frame) return null;
                    return (
                      <div className="bg-[#0a0a12] rounded-lg p-3 space-y-1">
                        <div className="text-[9px] text-muted-foreground tracking-wider mb-1">DETECTIONS — Frame {frame.frame_index}</div>
                        {frame.detections.map((det, i) => (
                          <div key={i} className="flex items-center gap-3 text-[11px]">
                            <span className="text-[#FF8000] font-mono w-8">{(det.score * 100).toFixed(0)}%</span>
                            <span className="text-foreground">{det.category}</span>
                            <span className="text-muted-foreground font-mono text-[9px]">
                              [{det.bbox.join(', ')}]
                            </span>
                          </div>
                        ))}
                      </div>
                    );
                  })()}
                </div>
              )}
            </div>
          )}

          {/* Video Classification Results */}
          {(videomaeData || timesformerData) && (
            <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
              <h3 className="text-xs text-muted-foreground tracking-widest mb-3 flex items-center gap-2">
                <Video className="w-3 h-3" />
                VIDEO CLASSIFICATION
              </h3>
              <div className="space-y-3">
                {Object.keys(videomaeData ?? timesformerData ?? {}).map(video => {
                  const vmae = videomaeData?.[video];
                  const tsf = timesformerData?.[video];
                  return (
                    <div key={video} className="bg-[#0a0a12] rounded-lg p-3">
                      <div className="flex items-center justify-between mb-2">
                        <span className="text-xs text-[#FF8000] font-mono">{video}</span>
                        <span className="text-[9px] text-muted-foreground">{vmae?.total_frames ?? tsf?.total_frames} frames @ {vmae?.fps ?? tsf?.fps}fps</span>
                      </div>
                      <div className="grid grid-cols-2 gap-3">
                        {vmae && (
                          <div>
                            <div className="text-[9px] text-purple-400 tracking-wider mb-1">VideoMAE</div>
                            {vmae.top_predictions.slice(0, 3).map((p, i) => (
                              <div key={i} className="flex items-center gap-2 text-[10px]">
                                <div className="w-16 h-1 bg-[#1a1a2e] rounded-full overflow-hidden">
                                  <div className="h-full bg-purple-400 rounded-full" style={{ width: `${p.score * 100}%` }} />
                                </div>
                                <span className="font-mono text-foreground">{(p.score * 100).toFixed(1)}%</span>
                                <span className="text-muted-foreground truncate">{p.label}</span>
                              </div>
                            ))}
                          </div>
                        )}
                        {tsf && (
                          <div>
                            <div className="text-[9px] text-cyan-400 tracking-wider mb-1">TimeSformer</div>
                            {tsf.top_predictions.slice(0, 3).map((p, i) => (
                              <div key={i} className="flex items-center gap-2 text-[10px]">
                                <div className="w-16 h-1 bg-[#1a1a2e] rounded-full overflow-hidden">
                                  <div className="h-full bg-cyan-400 rounded-full" style={{ width: `${p.score * 100}%` }} />
                                </div>
                                <span className="font-mono text-foreground">{(p.score * 100).toFixed(1)}%</span>
                                <span className="text-muted-foreground truncate">{p.label}</span>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>

        {/* Right Column */}
        <div className="col-span-5 space-y-3">
          {/* VLM Narrations */}
          {minicpmData && (
            <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
              <h3 className="text-xs text-muted-foreground tracking-widest mb-3 flex items-center gap-2">
                <Eye className="w-3 h-3" />
                VLM NARRATIONS — Gemma 3 4B + CLIP
              </h3>
              <div className="space-y-3">
                {Object.entries(minicpmData).map(([video, frames]) => (
                  <div key={video}>
                    <button
                      type="button"
                      onClick={() => setExpandedVideo(expandedVideo === video ? null : video)}
                      className="w-full text-left"
                    >
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-[11px] text-[#FF8000] font-mono">{video}</span>
                        <span className="text-[9px] text-muted-foreground">{frames.length} frames</span>
                      </div>
                    </button>
                    {expandedVideo === video ? (
                      <div className="space-y-2 mt-2">
                        {frames.map((frame) => {
                          const narration = frame.narration.trim().replace(/^<think>[\s\S]*?<\/think>\s*/i, '');
                          const firstParagraph = narration.split('\n').filter(l => l.trim())[0] ?? '';
                          return (
                            <div key={frame.frame_index} className="bg-[#0a0a12] rounded-lg p-2">
                              <div className="flex items-center gap-2 mb-1">
                                <span className="text-[9px] text-[#FF8000] font-mono">Frame {frame.frame_index}</span>
                                <span className="text-[9px] text-muted-foreground">{frame.tokens} tok | {frame.tok_per_s.toFixed(1)} tok/s</span>
                              </div>
                              <p className="text-[10px] text-muted-foreground leading-relaxed">{firstParagraph.slice(0, 300)}</p>
                            </div>
                          );
                        })}
                      </div>
                    ) : (
                      <div className="text-[10px] text-muted-foreground leading-relaxed line-clamp-2">
                        {frames[0]?.narration.trim().replace(/^<think>[\s\S]*?<\/think>\s*/i, '').split('\n').filter(l => l.trim())[0]?.slice(0, 150)}...
                      </div>
                    )}
                    <div className="text-[9px] text-muted-foreground mt-1">
                      avg {(frames.reduce((s, f) => s + f.tok_per_s, 0) / frames.length).toFixed(1)} tok/s
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Model Status */}
          <h3 className="text-xs text-muted-foreground tracking-widest">MODEL STATUS</h3>
          {modelStatus.map((model) => (
            <div key={model.name} className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-3">
              <div className="flex items-center justify-between mb-2">
                <div>
                  <div className="text-xs text-foreground">{model.name}</div>
                  <div className="text-[10px] text-[#FF8000]">{model.type}</div>
                </div>
                <span className={`text-[9px] px-1.5 py-0.5 rounded-full uppercase ${
                  model.status === 'active' ? 'text-green-400 bg-green-500/10' : 'text-cyan-400 bg-cyan-500/10'
                }`}>
                  {model.status}
                </span>
              </div>
              <div className="text-[10px] text-muted-foreground mb-1">{model.purpose}</div>
              <div className="text-[9px] text-muted-foreground">
                Framework: <span className="font-mono text-foreground">{model.framework}</span>
              </div>
            </div>
          ))}

          {/* Architecture */}
          <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
            <h4 className="text-[10px] text-muted-foreground tracking-widest mb-3">MEDIA PIPELINE</h4>
            <div className="space-y-2 text-[10px]">
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-[#FF8000]" />
                <span className="text-muted-foreground">Detection:</span>
                <span className="text-foreground">GroundingDINO (text-grounded)</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-cyan-400" />
                <span className="text-muted-foreground">Segmentation:</span>
                <span className="text-foreground">SAM2 (fused with detection)</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-purple-400" />
                <span className="text-muted-foreground">Video:</span>
                <span className="text-foreground">VideoMAE + TimeSformer</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-green-400" />
                <span className="text-muted-foreground">VLM:</span>
                <span className="text-foreground">Gemma 3 4B (Ollama) + CLIP scoring</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-amber-400" />
                <span className="text-muted-foreground">Source:</span>
                <span className="text-foreground">f1data/McMedia/</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
