import { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import {
  Brain, FileText, Cog, Ruler, FlaskConical, ChevronDown, ChevronRight,
  Loader2, Cpu, DollarSign, Layers, Upload, CheckCircle2, XCircle, CloudUpload,
} from 'lucide-react';
import type { IntelligenceData, DocumentMeta } from '../types';

interface UploadItem {
  name: string;
  status: 'uploading' | 'done' | 'error';
  chunks?: number;
  textLength?: number;
  error?: string;
}

export function AIInsights() {
  const [data, setData] = useState<IntelligenceData | null>(null);
  const [loading, setLoading] = useState(true);
  const [expandedDocs, setExpandedDocs] = useState<Set<string>>(new Set());
  const [uploads, setUploads] = useState<UploadItem[]>([]);
  const [dragging, setDragging] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const uploadFiles = useCallback(async (files: FileList | File[]) => {
    const fileArray = Array.from(files);
    // Add all files as uploading
    setUploads(prev => [
      ...fileArray.map(f => ({ name: f.name, status: 'uploading' as const })),
      ...prev,
    ]);

    for (const file of fileArray) {
      const formData = new FormData();
      formData.append('file', file);
      try {
        const res = await fetch('/api/upload', { method: 'POST', body: formData });
        const result = await res.json();
        setUploads(prev => prev.map(u =>
          u.name === file.name && u.status === 'uploading'
            ? result.status === 'ok'
              ? { name: file.name, status: 'done', chunks: result.chunks, textLength: result.text_length }
              : { name: file.name, status: 'error', error: result.error }
            : u
        ));
      } catch (err) {
        setUploads(prev => prev.map(u =>
          u.name === file.name && u.status === 'uploading'
            ? { name: file.name, status: 'error', error: 'Network error' }
            : u
        ));
      }
    }
  }, []);

  useEffect(() => {
    fetch('/api/pipeline/intelligence')
      .then((r) => r.json())
      .then((d) => { setData(d); setExpandedDocs(new Set(d.documents.map((doc: DocumentMeta) => doc.name))); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  const categoryBreakdown = useMemo(() => {
    if (!data) return [];
    const counts: Record<string, number> = {};
    data.rules.forEach((r) => {
      const cat = r.category || 'Uncategorized';
      counts[cat] = (counts[cat] || 0) + 1;
    });
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 12);
  }, [data]);

  const equipmentTypes = useMemo(() => {
    if (!data) return [];
    const counts: Record<string, number> = {};
    data.equipment.forEach((e) => {
      const t = e.type || 'Unknown';
      counts[t] = (counts[t] || 0) + 1;
    });
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
  }, [data]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-6 h-6 text-[#FF8000] animate-spin" />
        <span className="ml-3 text-muted-foreground text-sm">Loading pipeline intelligence...</span>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="flex items-center justify-center h-64">
        <Brain className="w-6 h-6 text-amber-400" />
        <span className="ml-3 text-muted-foreground text-sm">
          No intelligence data. Run: python pipeline/export_for_ui.py
        </span>
      </div>
    );
  }

  const s = data.stats;

  return (
    <div className="space-y-4">
      {/* Pipeline Stats Bar */}
      <div className="bg-[#12121e] border border-[rgba(255,128,0,0.12)] rounded-xl p-4">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-[#FF8000]/10 flex items-center justify-center">
              <Brain className="w-4 h-4 text-[#FF8000]" />
            </div>
            <div>
              <h3 className="text-sm text-foreground">PDF Extraction Intelligence</h3>
              <div className="text-[10px] text-muted-foreground">
                Groq Llama 4 Maverick | {data.documents.length} document{data.documents.length !== 1 ? 's' : ''} | 3-pass extraction
              </div>
            </div>
          </div>
          <span className="flex items-center gap-1.5 text-[10px] text-green-400 bg-green-500/10 px-2 py-1 rounded-full">
            <span className="w-1.5 h-1.5 rounded-full bg-green-400" />
            Extracted
          </span>
        </div>
        <div className="grid grid-cols-6 gap-3">
          {[
            { label: 'Pages Processed', value: s.total_pages.toLocaleString(), icon: <FileText className="w-3 h-3 text-cyan-400" /> },
            { label: 'Rules Extracted', value: s.total_rules.toLocaleString(), icon: <Layers className="w-3 h-3 text-[#FF8000]" /> },
            { label: 'Equipment Items', value: s.total_equipment.toLocaleString(), icon: <Cog className="w-3 h-3 text-green-400" /> },
            { label: 'Dimensions', value: s.total_dimensions.toLocaleString(), icon: <Ruler className="w-3 h-3 text-amber-400" /> },
            { label: 'API Cost', value: `$${s.total_cost_usd.toFixed(2)}`, icon: <DollarSign className="w-3 h-3 text-purple-400" /> },
            { label: 'Tokens Used', value: `${((s.total_tokens_in + s.total_tokens_out) / 1_000_000).toFixed(1)}M`, icon: <Cpu className="w-3 h-3 text-red-400" /> },
          ].map((stat) => (
            <div key={stat.label} className="bg-[#0a0a12] rounded-lg p-2">
              <div className="flex items-center gap-1.5 mb-1">
                {stat.icon}
                <span className="text-[9px] text-muted-foreground tracking-wider">{stat.label}</span>
              </div>
              <div className="text-sm font-mono text-foreground">{stat.value}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Upload Section */}
      <div>
        <h3 className="text-xs text-muted-foreground tracking-widest mb-2">UPLOAD DOCUMENTS</h3>
        <div
          className={`bg-[#12121e] border-2 border-dashed rounded-xl p-6 transition-colors ${
            dragging
              ? 'border-[#FF8000] bg-[#FF8000]/5'
              : 'border-[rgba(255,128,0,0.2)] hover:border-[rgba(255,128,0,0.4)]'
          }`}
          onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
          onDragLeave={() => setDragging(false)}
          onDrop={(e) => {
            e.preventDefault();
            setDragging(false);
            if (e.dataTransfer.files.length > 0) uploadFiles(e.dataTransfer.files);
          }}
        >
          <div className="flex flex-col items-center gap-3">
            <CloudUpload className={`w-8 h-8 ${dragging ? 'text-[#FF8000]' : 'text-[rgba(255,128,0,0.4)]'}`} />
            <div className="text-center">
              <p className="text-sm text-foreground">
                Drop files here or{' '}
                <button
                  onClick={() => fileInputRef.current?.click()}
                  className="text-[#FF8000] hover:underline"
                >
                  browse
                </button>
              </p>
              <p className="text-[10px] text-muted-foreground mt-1">
                PDF, DOCX, TXT, CSV, JSON, MD — ingested into RAG knowledge base
              </p>
            </div>
          </div>
          <input
            ref={fileInputRef}
            type="file"
            multiple
            accept=".pdf,.docx,.txt,.csv,.json,.md"
            aria-label="Upload documents"
            className="hidden"
            onChange={(e) => {
              if (e.target.files && e.target.files.length > 0) {
                uploadFiles(e.target.files);
                e.target.value = '';
              }
            }}
          />
        </div>

        {/* Upload Results */}
        {uploads.length > 0 && (
          <div className="mt-3 space-y-1.5">
            {uploads.map((u, i) => (
              <div
                key={`${u.name}-${i}`}
                className="flex items-center gap-3 bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-lg px-4 py-2.5"
              >
                {u.status === 'uploading' && <Loader2 className="w-3.5 h-3.5 text-[#FF8000] animate-spin shrink-0" />}
                {u.status === 'done' && <CheckCircle2 className="w-3.5 h-3.5 text-green-400 shrink-0" />}
                {u.status === 'error' && <XCircle className="w-3.5 h-3.5 text-red-400 shrink-0" />}
                <div className="flex-1 min-w-0">
                  <div className="text-[11px] text-foreground truncate">{u.name}</div>
                  {u.status === 'done' && (
                    <div className="text-[10px] text-muted-foreground">
                      {u.chunks} chunks embedded ({((u.textLength || 0) / 1000).toFixed(1)}K chars)
                    </div>
                  )}
                  {u.status === 'error' && (
                    <div className="text-[10px] text-red-400">{u.error}</div>
                  )}
                  {u.status === 'uploading' && (
                    <div className="text-[10px] text-muted-foreground">Processing & embedding...</div>
                  )}
                </div>
                {u.status === 'done' && (
                  <Upload className="w-3 h-3 text-green-400 shrink-0" />
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="grid grid-cols-12 gap-4">
        {/* Left: Documents + Categories */}
        <div className="col-span-7 space-y-4">
          {/* Document Explorer */}
          <div className="space-y-2">
            <h3 className="text-xs text-muted-foreground tracking-widest">DOCUMENTS</h3>
            {data.documents.map((doc) => (
              <DocumentCard
                key={doc.name}
                doc={doc}
                expanded={expandedDocs.has(doc.name)}
                onToggle={() => setExpandedDocs(prev => {
                  const next = new Set(prev);
                  if (next.has(doc.name)) next.delete(doc.name); else next.add(doc.name);
                  return next;
                })}
              />
            ))}
          </div>

          {/* Regulation Categories */}
          <div>
            <h3 className="text-xs text-muted-foreground tracking-widest mb-2">REGULATION CATEGORIES</h3>
            <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
              <div className="space-y-2">
                {categoryBreakdown.map(([cat, count]) => {
                  const pct = Math.round((count / s.total_rules) * 100);
                  return (
                    <div key={cat} className="flex items-center gap-3">
                      <span className="text-[11px] text-foreground w-48 truncate">{cat}</span>
                      <div className="flex-1 h-1.5 bg-[#0a0a12] rounded-full overflow-hidden">
                        <div
                          className="h-full bg-[#FF8000] rounded-full"
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                      <span className="text-[10px] font-mono text-muted-foreground w-10 text-right">{count}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>

        {/* Right: Equipment + Specs */}
        <div className="col-span-5 space-y-4">
          {/* Equipment Types */}
          <div>
            <h3 className="text-xs text-muted-foreground tracking-widest mb-2">EQUIPMENT TYPES</h3>
            <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4 space-y-1.5">
              {equipmentTypes.map(([type, count]) => (
                <div key={type} className="flex items-center justify-between text-[11px]">
                  <div className="flex items-center gap-2">
                    <Cog className="w-3 h-3 text-green-400" />
                    <span className="text-foreground">{type}</span>
                  </div>
                  <span className="font-mono text-muted-foreground">{count}</span>
                </div>
              ))}
              {equipmentTypes.length === 0 && (
                <div className="text-[11px] text-muted-foreground text-center py-2">No equipment data</div>
              )}
            </div>
          </div>

          {/* Dimensional Data Sample */}
          <div>
            <h3 className="text-xs text-muted-foreground tracking-widest mb-2">DIMENSIONAL SPECS (sample)</h3>
            <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4 space-y-2">
              {data.dimensional_data.slice(0, 8).map((dim, i) => (
                <div key={i} className="flex items-center justify-between text-[11px]">
                  <span className="text-foreground truncate max-w-[200px]">{dim.component}</span>
                  <span className="font-mono text-[#FF8000]">
                    {dim.value != null ? `${dim.value} ${dim.unit || ''}` : '—'}
                  </span>
                </div>
              ))}
              {data.dimensional_data.length === 0 && (
                <div className="text-[11px] text-muted-foreground text-center py-2">No dimensional data</div>
              )}
              {data.dimensional_data.length > 8 && (
                <div className="text-[10px] text-muted-foreground text-center pt-1">
                  +{data.dimensional_data.length - 8} more
                </div>
              )}
            </div>
          </div>

          {/* Material Specs */}
          <div>
            <h3 className="text-xs text-muted-foreground tracking-widest mb-2">MATERIAL SPECS</h3>
            <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4 space-y-2">
              {data.material_specs.slice(0, 6).map((mat, i) => (
                <div key={i} className="text-[11px]">
                  <div className="flex items-center gap-2">
                    <FlaskConical className="w-3 h-3 text-purple-400 shrink-0" />
                    <span className="text-foreground">{mat.material || 'Unknown'}</span>
                  </div>
                  {mat.application && (
                    <div className="text-[10px] text-muted-foreground ml-5 truncate">{mat.application}</div>
                  )}
                </div>
              ))}
              {data.material_specs.length === 0 && (
                <div className="text-[11px] text-muted-foreground text-center py-2">No material data</div>
              )}
            </div>
          </div>

          {/* Pipeline Architecture */}
          <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
            <h4 className="text-[10px] text-muted-foreground tracking-widest mb-3">EXTRACTION PIPELINE</h4>
            <div className="space-y-2 text-[10px]">
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-cyan-400" />
                <span className="text-muted-foreground">Model:</span>
                <span className="text-foreground">Groq Llama 4 Maverick</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-[#FF8000]" />
                <span className="text-muted-foreground">Pass 1:</span>
                <span className="text-foreground">Document Overview</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-green-400" />
                <span className="text-muted-foreground">Pass 2:</span>
                <span className="text-foreground">Equipment & Tags</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-amber-400" />
                <span className="text-muted-foreground">Pass 3:</span>
                <span className="text-foreground">Specifications & Rules</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-purple-400" />
                <span className="text-muted-foreground">Storage:</span>
                <span className="text-foreground">MongoDB Atlas Vector DB</span>
              </div>
            </div>
          </div>
        </div>
      </div>

    </div>
  );
}

function DocumentCard({ doc, expanded, onToggle }: {
  doc: DocumentMeta;
  expanded: boolean;
  onToggle: () => void;
}) {
  const totalItems = doc.passes.reduce((s, p) => s + p.items_found, 0);
  const totalCost = doc.passes.reduce((s, p) => s + p.cost_usd, 0);

  return (
    <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full flex items-center gap-3 p-4 text-left hover:bg-[#1a1a2e] transition-colors"
      >
        {expanded
          ? <ChevronDown className="w-4 h-4 text-[#FF8000] shrink-0" />
          : <ChevronRight className="w-4 h-4 text-muted-foreground shrink-0" />
        }
        <div className="flex-1 min-w-0">
          <div className="text-xs text-foreground truncate">{doc.title}</div>
          <div className="text-[10px] text-muted-foreground">
            {doc.document_type} {doc.revision && `| ${doc.revision}`} {doc.date && `| ${doc.date}`}
          </div>
        </div>
        <div className="flex items-center gap-4 text-[10px] shrink-0">
          <span className="font-mono text-muted-foreground">{doc.total_pages} pages</span>
          <span className="font-mono text-[#FF8000]">{totalItems} items</span>
          <span className="font-mono text-muted-foreground">${totalCost.toFixed(2)}</span>
        </div>
      </button>

      {expanded && (
        <div className="px-4 pb-4 space-y-3 border-t border-[rgba(255,128,0,0.06)]">
          {/* Passes */}
          <div className="pt-3">
            <div className="text-[9px] text-muted-foreground tracking-widest mb-2">EXTRACTION PASSES</div>
            <div className="space-y-1.5">
              {doc.passes.map((p) => (
                <div key={p.number} className="flex items-center gap-3 text-[11px]">
                  <span className="text-[#FF8000] font-mono w-5">P{p.number}</span>
                  <span className="text-foreground w-28">{p.name}</span>
                  <span className="text-muted-foreground font-mono">{p.items_found} items</span>
                  <span className="text-muted-foreground font-mono">{p.latency_s.toFixed(0)}s</span>
                  <span className="text-muted-foreground font-mono">${p.cost_usd.toFixed(3)}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Sections */}
          {doc.sections && doc.sections.length > 0 && (
            <div>
              <div className="text-[9px] text-muted-foreground tracking-widest mb-2">TABLE OF CONTENTS</div>
              <div className="grid grid-cols-2 gap-1">
                {doc.sections.slice(0, 20).map((s, i) => (
                  <div key={i} className="text-[10px] flex items-center gap-1.5">
                    <span className="text-[#FF8000] font-mono">{s.number}</span>
                    <span className="text-foreground truncate">{s.title}</span>
                  </div>
                ))}
              </div>
              {doc.sections.length > 20 && (
                <div className="text-[10px] text-muted-foreground mt-1">+{doc.sections.length - 20} more</div>
              )}
            </div>
          )}

          {/* Standards */}
          {doc.standards && doc.standards.length > 0 && (
            <div>
              <div className="text-[9px] text-muted-foreground tracking-widest mb-1">STANDARDS REFERENCED</div>
              <div className="flex flex-wrap gap-1">
                {doc.standards.slice(0, 15).map((std) => (
                  <span key={std} className="text-[9px] px-1.5 py-0.5 rounded bg-cyan-500/10 text-cyan-400">
                    {std}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
