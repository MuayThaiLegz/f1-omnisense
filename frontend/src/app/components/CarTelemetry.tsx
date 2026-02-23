import React, { useState, useEffect, useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, AreaChart, Area, BarChart, Bar, Cell,
  ReferenceLine,
} from 'recharts';
import {
  Gauge, Zap, Timer, Loader2, AlertCircle, Wind, GitCompareArrows, Disc,
  ArrowUp, ArrowDown,
} from 'lucide-react';
import { fetchCSV, parseCSV } from '../api/local';

const compoundColors: Record<string, string> = {
  SOFT: '#ef4444', MEDIUM: '#f59e0b', HARD: '#e8e8f0', INTERMEDIATE: '#22c55e', WET: '#3b82f6',
};

const NOR_COLOR = '#FF8000';
const PIA_COLOR = '#22d3ee';

const RACES_2023 = [
  'Bahrain', 'Saudi Arabian', 'Australian', 'Miami', 'Monaco', 'Spanish',
  'Canadian', 'British', 'Hungarian', 'Dutch', 'Italian', 'Singapore',
  'Japanese', 'Mexico City', 'Las Vegas', 'Abu Dhabi',
];

const RACES_2024 = [
  'Bahrain', 'Saudi Arabian', 'Australian', 'Japanese', 'Monaco', 'Canadian',
  'Spanish', 'British', 'Hungarian', 'Belgian', 'Dutch', 'Italian',
  'Azerbaijan', 'Singapore', 'Mexico City', 'Las Vegas', 'Abu Dhabi',
  'Emilia Romagna',
];

const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload?.length) {
    return (
      <div className="bg-[#0D1117] border border-[rgba(255,128,0,0.2)] rounded-lg p-2 text-[12px]">
        <div className="text-muted-foreground mb-1">{label}</div>
        {payload.map((entry: any, index: number) => (
          <div key={index} className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full" style={{ backgroundColor: entry.color }} />
            <span className="text-muted-foreground">{entry.name}:</span>
            <span className="text-foreground font-mono">{typeof entry.value === 'number' ? entry.value.toFixed(1) : entry.value}</span>
          </div>
        ))}
      </div>
    );
  }
  return null;
};

function KPI({ icon, label, value, detail, color = 'text-foreground' }: { icon: React.ReactNode; label: string; value: string; detail: string; color?: string }) {
  return (
    <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-3">
      <div className="flex items-center gap-2 mb-1.5">
        {icon}
        <span className="text-[12px] text-muted-foreground tracking-wider">{label}</span>
      </div>
      <div className={`text-lg font-mono ${color}`}>{value}</div>
      <div className="text-[12px] text-muted-foreground mt-0.5">{detail}</div>
    </div>
  );
}

function DeltaKPI({ label, val1, val2, label1, label2, unit, icon, color1 = NOR_COLOR, color2 = PIA_COLOR }: {
  label: string; val1: string; val2: string; label1: string; label2: string;
  unit: string; icon: React.ReactNode; color1?: string; color2?: string;
}) {
  const n1 = parseFloat(val1);
  const n2 = parseFloat(val2);
  const diff = n1 - n2;
  const absDiff = Math.abs(diff).toFixed(1);
  const firstWins = diff > 0;
  return (
    <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-3">
      <div className="flex items-center gap-2 mb-2">
        {icon}
        <span className="text-[12px] text-muted-foreground tracking-wider">{label}</span>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <div className="text-[11px] text-muted-foreground mb-0.5">{label1}</div>
          <div className="text-base font-mono" style={{ color: color1 }}>{val1}{unit}</div>
        </div>
        <div>
          <div className="text-[11px] text-muted-foreground mb-0.5">{label2}</div>
          <div className="text-base font-mono" style={{ color: color2 }}>{val2}{unit}</div>
        </div>
      </div>
      {diff !== 0 && !isNaN(diff) && (
        <div className="mt-2 flex items-center gap-1 text-[12px]">
          {firstWins
            ? <ArrowUp className="w-3 h-3" style={{ color: color1 }} />
            : <ArrowDown className="w-3 h-3" style={{ color: color2 }} />
          }
          <span className="text-muted-foreground">
            {firstWins ? label1 : label2} +{absDiff}{unit}
          </span>
        </div>
      )}
    </div>
  );
}

interface CarSummary {
  race: string;
  avgSpeed: number;
  topSpeed: number;
  avgRPM: number;
  maxRPM: number;
  avgThrottle: number;
  brakePct: number;
  drsPct: number;
  compounds: string[];
  samples: number;
}

type Tab = 'detail' | 'season' | 'h2h' | 'racecompare';

/* ── helpers ── */
function computeCarKpis(data: Record<string, string>[]) {
  if (!data.length) return null;
  const speeds = data.map(r => Number(r.Speed) || 0);
  const rpms = data.map(r => Number(r.RPM) || 0);
  const drsCount = data.filter(r => Number(r.DRS) >= 10).length;
  const compounds = [...new Set(data.map(r => r.Compound).filter(Boolean))];
  return {
    topSpeed: Math.max(...speeds).toFixed(0),
    avgSpeed: (speeds.reduce((a, b) => a + b, 0) / speeds.length).toFixed(1),
    avgRPM: (rpms.reduce((a, b) => a + b, 0) / rpms.length).toFixed(0),
    drsActivations: drsCount,
    drsPct: ((drsCount / data.length) * 100).toFixed(1),
    avgThrottle: (data.map(r => Number(r.Throttle) || 0).reduce((a, b) => a + b, 0) / data.length).toFixed(1),
    brakePct: ((data.filter(r => r.Brake === 'True' || r.Brake === '1').length / data.length) * 100).toFixed(1),
    compounds,
  };
}

function buildLapTimes(data: Record<string, string>[]) {
  const lapMap = new Map<number, { times: number[]; compound: string }>();
  for (const r of data) {
    const lap = Number(r.LapNumber);
    const lt = r.LapTime;
    if (!lap || !lt) continue;
    let seconds = 0;
    const match = lt.match(/(\d+):(\d+):(\d+\.?\d*)/);
    if (match) seconds = Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3]);
    if (seconds > 0 && seconds < 300) {
      if (!lapMap.has(lap)) lapMap.set(lap, { times: [], compound: r.Compound || '' });
      lapMap.get(lap)!.times.push(seconds);
    }
  }
  return Array.from(lapMap.entries())
    .map(([lap, { times, compound }]) => ({ lap, time: times[0], compound }))
    .sort((a, b) => a.lap - b.lap);
}

function downsample(data: Record<string, string>[], maxPoints = 500) {
  const step = Math.max(1, Math.floor(data.length / maxPoints));
  return data.filter((_, i) => i % step === 0);
}

export function CarTelemetry() {
  const [tab, setTab] = useState<Tab>('detail');
  const [year, setYear] = useState(2024);
  const [race, setRace] = useState('Bahrain');
  const [driver, setDriver] = useState('NOR');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rawData, setRawData] = useState<Record<string, string>[]>([]);
  const [stintsData, setStintsData] = useState<Record<string, string>[]>([]);
  const [seasonSummary, setSeasonSummary] = useState<CarSummary[]>([]);
  const [summaryLoading, setSummaryLoading] = useState(false);

  // H2H state
  const [h2hMode, setH2hMode] = useState<'race' | 'season'>('race');
  const [h2hLoading, setH2hLoading] = useState(false);
  const [h2hRaceData, setH2hRaceData] = useState<Record<string, string>[]>([]);
  const [h2hSeasonNor, setH2hSeasonNor] = useState<CarSummary[]>([]);
  const [h2hSeasonPia, setH2hSeasonPia] = useState<CarSummary[]>([]);

  // Compare state
  const [compareMode, setCompareMode] = useState<'race' | 'year'>('race');
  const [race2, setRace2] = useState('Saudi Arabian');
  const [compareLoading, setCompareLoading] = useState(false);
  const [compareData1, setCompareData1] = useState<Record<string, string>[]>([]);
  const [compareData2, setCompareData2] = useState<Record<string, string>[]>([]);
  const [compareSeason23, setCompareSeason23] = useState<CarSummary[]>([]);
  const [compareSeason24, setCompareSeason24] = useState<CarSummary[]>([]);

  const races = year === 2024 ? RACES_2024 : RACES_2023;

  // Load per-race telemetry (detail tab)
  useEffect(() => {
    if (tab !== 'detail') return;
    setLoading(true);
    setError(null);
    const raceName = `${year}_${race.replace(/ /g, '_')}_Grand_Prix_Race`;
    Promise.allSettled([
      fetchCSV(`mccar/${year}/${raceName}.csv`).then(csv => setRawData(parseCSV(csv))),
      fetchCSV(`mcracecontext/${year}/tire_stints.csv`).then(csv => setStintsData(parseCSV(csv))),
    ]).then(results => {
      if (results[0].status === 'rejected') { setError('Race data not available'); setRawData([]); }
    }).finally(() => setLoading(false));
  }, [year, race, tab]);

  // Load season summary (season tab)
  useEffect(() => {
    if (tab !== 'season') return;
    setSummaryLoading(true);
    fetch(`/api/mccar-summary/${year}/${driver}`)
      .then(res => res.json())
      .then(data => setSeasonSummary(data))
      .catch(() => setSeasonSummary([]))
      .finally(() => setSummaryLoading(false));
  }, [year, driver, tab]);

  // H2H race — load CSV (contains both drivers)
  useEffect(() => {
    if (tab !== 'h2h' || h2hMode !== 'race') return;
    setH2hLoading(true);
    const raceName = `${year}_${race.replace(/ /g, '_')}_Grand_Prix_Race`;
    fetchCSV(`mccar/${year}/${raceName}.csv`)
      .then(csv => setH2hRaceData(parseCSV(csv)))
      .catch(() => setH2hRaceData([]))
      .finally(() => setH2hLoading(false));
  }, [year, race, tab, h2hMode]);

  // H2H season — both drivers
  useEffect(() => {
    if (tab !== 'h2h' || h2hMode !== 'season') return;
    setH2hLoading(true);
    Promise.all([
      fetch(`/api/mccar-summary/${year}/NOR`).then(r => r.json()).catch(() => []),
      fetch(`/api/mccar-summary/${year}/PIA`).then(r => r.json()).catch(() => []),
    ]).then(([nor, pia]) => { setH2hSeasonNor(nor); setH2hSeasonPia(pia); })
      .finally(() => setH2hLoading(false));
  }, [year, tab, h2hMode]);

  // Compare: race vs race
  useEffect(() => {
    if (tab !== 'racecompare' || compareMode !== 'race') return;
    setCompareLoading(true);
    const name1 = `${year}_${race.replace(/ /g, '_')}_Grand_Prix_Race`;
    const name2 = `${year}_${race2.replace(/ /g, '_')}_Grand_Prix_Race`;
    Promise.all([
      fetchCSV(`mccar/${year}/${name1}.csv`).then(csv => parseCSV(csv)).catch(() => []),
      fetchCSV(`mccar/${year}/${name2}.csv`).then(csv => parseCSV(csv)).catch(() => []),
    ]).then(([d1, d2]) => { setCompareData1(d1); setCompareData2(d2); })
      .finally(() => setCompareLoading(false));
  }, [year, race, race2, driver, tab, compareMode]);

  // Compare: year vs year
  useEffect(() => {
    if (tab !== 'racecompare' || compareMode !== 'year') return;
    setCompareLoading(true);
    Promise.all([
      fetch(`/api/mccar-summary/2023/${driver}`).then(r => r.json()).catch(() => []),
      fetch(`/api/mccar-summary/2024/${driver}`).then(r => r.json()).catch(() => []),
    ]).then(([s23, s24]) => { setCompareSeason23(s23); setCompareSeason24(s24); })
      .finally(() => setCompareLoading(false));
  }, [driver, tab, compareMode]);

  const driverData = useMemo(() => rawData.filter(r => r.Driver === driver), [rawData, driver]);

  const kpis = useMemo(() => {
    if (!driverData.length) return null;
    const speeds = driverData.map(r => Number(r.Speed) || 0);
    const rpms = driverData.map(r => Number(r.RPM) || 0);
    const drsCount = driverData.filter(r => Number(r.DRS) >= 10).length;
    const compounds = [...new Set(driverData.map(r => r.Compound).filter(Boolean))];
    return {
      topSpeed: Math.max(...speeds).toFixed(0),
      avgRPM: (rpms.reduce((a, b) => a + b, 0) / rpms.length).toFixed(0),
      drsActivations: drsCount,
      compounds,
    };
  }, [driverData]);

  const speedTrace = useMemo(() => {
    if (!driverData.length) return [];
    return downsample(driverData).map(r => ({
      dist: (Number(r.Distance) / 1000).toFixed(2),
      speed: Number(r.Speed) || 0,
      rpm: Number(r.RPM) || 0,
      gear: Number(r.nGear) || 0,
      throttle: Number(r.Throttle) || 0,
      brake: r.Brake === 'True' || r.Brake === '1' ? 100 : 0,
    }));
  }, [driverData]);

  const lapTimes = useMemo(() => buildLapTimes(driverData), [driverData]);

  const drsPerLap = useMemo(() => {
    if (!driverData.length) return [];
    const lapDrs = new Map<number, number>();
    for (const r of driverData) {
      const lap = Number(r.LapNumber);
      if (!lap) continue;
      if (Number(r.DRS) >= 10) lapDrs.set(lap, (lapDrs.get(lap) || 0) + 1);
    }
    return Array.from(lapDrs.entries()).map(([lap, count]) => ({ lap, drs: count })).sort((a, b) => a.lap - b.lap);
  }, [driverData]);

  const raceStints = useMemo(() => {
    if (!stintsData.length) return [];
    return stintsData
      .filter(r => {
        const matchDriver = r.driver_acronym === driver;
        const matchRace = r.meeting_name?.includes(race);
        const matchYear = String(r.year) === String(year);
        const isRace = r.session_name === 'Race' || r.session_type === 'Race';
        return matchDriver && matchRace && matchYear && isRace;
      })
      .map(r => ({
        stint: Number(r.stint_number),
        compound: r.compound || '',
        lapStart: Number(r.lap_start),
        lapEnd: Number(r.lap_end),
        stintLaps: Number(r.stint_laps) || (Number(r.lap_end) - Number(r.lap_start)),
        tyreAge: Number(r.tyre_age_at_start) || 0,
      }))
      .sort((a, b) => a.stint - b.stint);
  }, [stintsData, race, year, driver]);

  const highlightRace = race;

  return (
    <div className="space-y-4">
      {/* Tab Switcher + Controls */}
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex items-center gap-1 bg-[#1A1F2E] rounded-lg p-0.5 border border-[rgba(255,128,0,0.12)]">
          {([
            { id: 'detail' as Tab, label: 'Race Detail' },
            { id: 'season' as Tab, label: 'Season' },
            { id: 'h2h' as Tab, label: 'Head to Head' },
            { id: 'racecompare' as Tab, label: 'Compare' },
          ]).map(t => (
            <button type="button" key={t.id} onClick={() => setTab(t.id)}
              className={`text-sm px-4 py-1.5 rounded-md transition-all ${
                tab === t.id ? 'bg-[#FF8000]/10 text-[#FF8000]' : 'text-muted-foreground hover:text-foreground'
              }`}
            >{t.label}</button>
          ))}
        </div>
        {/* Year (hidden in year-vs-year compare) */}
        {tab !== 'racecompare' || compareMode !== 'year' ? (
          <div className="flex items-center gap-1 bg-[#1A1F2E] rounded-lg p-0.5 border border-[rgba(255,128,0,0.12)]">
            {[2023, 2024].map(y => (
              <button type="button" key={y} onClick={() => { setYear(y); setRace(y === 2024 ? RACES_2024[0] : RACES_2023[0]); setRace2(y === 2024 ? RACES_2024[1] : RACES_2023[1]); }}
                className={`text-sm px-3 py-1.5 rounded-md transition-all ${year === y ? 'bg-[#FF8000]/10 text-[#FF8000]' : 'text-muted-foreground hover:text-foreground'}`}
              >{y}</button>
            ))}
          </div>
        ) : null}
        {/* Race A */}
        {(tab === 'detail' || (tab === 'h2h' && h2hMode === 'race') || (tab === 'racecompare' && compareMode === 'race')) && (
          <select value={race} onChange={e => setRace(e.target.value)} aria-label="Select race"
            className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-lg text-sm text-foreground px-3 py-1.5 outline-none"
          >
            {races.map(r => <option key={r} value={r}>{r} GP</option>)}
          </select>
        )}
        {/* Race B */}
        {tab === 'racecompare' && compareMode === 'race' && (
          <>
            <span className="text-[12px] text-muted-foreground">vs</span>
            <select value={race2} onChange={e => setRace2(e.target.value)} aria-label="Select second race"
              className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-lg text-sm text-foreground px-3 py-1.5 outline-none"
            >
              {races.map(r => <option key={r} value={r}>{r} GP</option>)}
            </select>
          </>
        )}
        {/* Driver (not in h2h) */}
        {tab !== 'h2h' && (
          <div className="flex items-center gap-1 bg-[#1A1F2E] rounded-lg p-0.5 border border-[rgba(255,128,0,0.12)]">
            {['NOR', 'PIA'].map(d => (
              <button type="button" key={d} onClick={() => setDriver(d)}
                className={`text-sm px-3 py-1.5 rounded-md transition-all ${driver === d ? 'bg-[#FF8000]/10 text-[#FF8000]' : 'text-muted-foreground hover:text-foreground'}`}
              >{d}</button>
            ))}
          </div>
        )}
        {/* H2H sub-mode */}
        {tab === 'h2h' && (
          <div className="flex items-center gap-1 bg-[#1A1F2E] rounded-lg p-0.5 border border-[rgba(255,128,0,0.12)]">
            {([{ id: 'race' as const, label: 'Single Race' }, { id: 'season' as const, label: 'Full Season' }]).map(m => (
              <button type="button" key={m.id} onClick={() => setH2hMode(m.id)}
                className={`text-sm px-3 py-1.5 rounded-md transition-all ${h2hMode === m.id ? 'bg-[#FF8000]/10 text-[#FF8000]' : 'text-muted-foreground hover:text-foreground'}`}
              >{m.label}</button>
            ))}
          </div>
        )}
        {/* Compare sub-mode */}
        {tab === 'racecompare' && (
          <div className="flex items-center gap-1 bg-[#1A1F2E] rounded-lg p-0.5 border border-[rgba(255,128,0,0.12)]">
            {([{ id: 'race' as const, label: 'Race vs Race' }, { id: 'year' as const, label: '2023 vs 2024' }]).map(m => (
              <button type="button" key={m.id} onClick={() => setCompareMode(m.id)}
                className={`text-sm px-3 py-1.5 rounded-md transition-all ${compareMode === m.id ? 'bg-[#FF8000]/10 text-[#FF8000]' : 'text-muted-foreground hover:text-foreground'}`}
              >{m.label}</button>
            ))}
          </div>
        )}
        {tab === 'detail' && <span className="text-[12px] text-muted-foreground">{driverData.length.toLocaleString()} data points</span>}
        {tab === 'season' && seasonSummary.length > 0 && <span className="text-[12px] text-muted-foreground">{seasonSummary.length} races</span>}
      </div>

      {error && tab === 'detail' && (
        <div className="flex items-center gap-2 text-amber-400 text-sm bg-amber-500/10 rounded-lg p-3">
          <AlertCircle className="w-4 h-4" />{error}
        </div>
      )}

      {tab === 'detail' && (
        <RaceDetailView kpis={kpis} speedTrace={speedTrace} lapTimes={lapTimes}
          drsPerLap={drsPerLap} raceStints={raceStints} race={race} year={year} loading={loading} />
      )}
      {tab === 'season' && (
        <SeasonCompareView seasonSummary={seasonSummary} loading={summaryLoading}
          year={year} driver={driver} highlightRace={highlightRace} />
      )}
      {tab === 'h2h' && (
        <H2HView mode={h2hMode} loading={h2hLoading} year={year} race={race}
          raceData={h2hRaceData} seasonNor={h2hSeasonNor} seasonPia={h2hSeasonPia} />
      )}
      {tab === 'racecompare' && (
        <CompareView mode={compareMode} loading={compareLoading} year={year} driver={driver}
          race1={race} race2={race2} data1={compareData1} data2={compareData2}
          season23={compareSeason23} season24={compareSeason24} />
      )}
    </div>
  );
}

/* ─── Race Detail View ─── */

function RaceDetailView({ kpis, speedTrace, lapTimes, drsPerLap, raceStints, race, year, loading }: {
  kpis: { topSpeed: string; avgRPM: string; drsActivations: number; compounds: string[] } | null;
  speedTrace: { dist: string; speed: number; rpm: number; gear: number; throttle: number; brake: number }[];
  lapTimes: { lap: number; time: number; compound: string }[];
  drsPerLap: { lap: number; drs: number }[];
  raceStints: { stint: number; compound: string; lapStart: number; lapEnd: number; stintLaps: number; tyreAge: number }[];
  race: string;
  year: number;
  loading: boolean;
}) {
  if (loading) {
    return (
      <div className="flex items-center justify-center h-[400px]">
        <Loader2 className="w-6 h-6 text-[#FF8000] animate-spin" />
        <span className="ml-2 text-sm text-muted-foreground">Loading telemetry...</span>
      </div>
    );
  }

  return (
    <>
      {kpis && (
        <div className="grid grid-cols-4 gap-3">
          <KPI icon={<Gauge className="w-4 h-4 text-[#FF8000]" />} label="TOP SPEED" value={`${kpis.topSpeed} km/h`} detail={`${race} GP ${year}`} color="text-[#FF8000]" />
          <KPI icon={<Zap className="w-4 h-4 text-cyan-400" />} label="AVG RPM" value={kpis.avgRPM} detail="Engine average" color="text-cyan-400" />
          <KPI icon={<Wind className="w-4 h-4 text-green-400" />} label="DRS ACTIVATIONS" value={String(kpis.drsActivations)} detail="Total samples with DRS open" color="text-green-400" />
          <KPI icon={<Timer className="w-4 h-4 text-amber-400" />} label="TIRE COMPOUNDS" value={kpis.compounds.join(' / ')} detail={`${raceStints.length} stints`} color="text-amber-400" />
        </div>
      )}

      {speedTrace.length > 0 && (
        <div className="grid grid-cols-2 gap-3">
          <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
            <h3 className="text-sm text-foreground mb-1">Speed Trace</h3>
            <p className="text-[12px] text-muted-foreground mb-3">Speed (km/h) over distance</p>
            <div className="h-[220px]">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={speedTrace}>
                  <defs>
                    <linearGradient id="speedGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#FF8000" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#FF8000" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                  <XAxis dataKey="dist" tick={{ fill: '#8888a0', fontSize: 9 }} tickCount={8} />
                  <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={[0, 'auto']} />
                  <Tooltip content={<CustomTooltip />} />
                  <Area type="monotone" dataKey="speed" stroke="#FF8000" fill="url(#speedGrad)" strokeWidth={1.5} dot={false} name="Speed (km/h)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>
          <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
            <h3 className="text-sm text-foreground mb-1">RPM & Gear</h3>
            <p className="text-[12px] text-muted-foreground mb-3">Engine RPM and gear selection</p>
            <div className="h-[220px]">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={speedTrace}>
                  <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                  <XAxis dataKey="dist" tick={{ fill: '#8888a0', fontSize: 9 }} tickCount={8} />
                  <YAxis yAxisId="rpm" tick={{ fill: '#8888a0', fontSize: 9 }} />
                  <YAxis yAxisId="gear" orientation="right" domain={[0, 8]} tick={{ fill: '#8888a0', fontSize: 9 }} />
                  <Tooltip content={<CustomTooltip />} />
                  <Line yAxisId="rpm" type="monotone" dataKey="rpm" stroke="#22d3ee" strokeWidth={1} dot={false} name="RPM" />
                  <Line yAxisId="gear" type="stepAfter" dataKey="gear" stroke="#a78bfa" strokeWidth={1.5} dot={false} name="Gear" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      )}

      {speedTrace.length > 0 && (
        <div className="grid grid-cols-2 gap-3">
          <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
            <h3 className="text-sm text-foreground mb-1">Throttle & Brake</h3>
            <p className="text-[12px] text-muted-foreground mb-3">Throttle application (%) and brake zones</p>
            <div className="h-[220px]">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={speedTrace}>
                  <defs>
                    <linearGradient id="throttleGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#22c55e" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                  <XAxis dataKey="dist" tick={{ fill: '#8888a0', fontSize: 9 }} tickCount={8} />
                  <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={[0, 100]} />
                  <Tooltip content={<CustomTooltip />} />
                  <Area type="monotone" dataKey="throttle" stroke="#22c55e" fill="url(#throttleGrad)" strokeWidth={1} dot={false} name="Throttle %" />
                  <Area type="monotone" dataKey="brake" stroke="#ef4444" fill="#ef444420" strokeWidth={1} dot={false} name="Brake" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>
          {lapTimes.length > 0 && (
            <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
              <h3 className="text-sm text-foreground mb-1">Lap Time Progression</h3>
              <p className="text-[12px] text-muted-foreground mb-3">Lap time by compound</p>
              <div className="h-[220px]">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={lapTimes}>
                    <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                    <XAxis dataKey="lap" tick={{ fill: '#8888a0', fontSize: 9 }} />
                    <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={['auto', 'auto']} />
                    <Tooltip content={<CustomTooltip />} />
                    <Bar dataKey="time" name="Lap Time (s)" radius={[2, 2, 0, 0]}>
                      {lapTimes.map((entry, i) => (
                        <Cell key={i} fill={compoundColors[entry.compound] || '#8888a0'} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}
        </div>
      )}

      <div className="grid grid-cols-2 gap-3">
        {drsPerLap.length > 0 && (
          <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
            <h3 className="text-sm text-foreground mb-1">DRS Usage by Lap</h3>
            <p className="text-[12px] text-muted-foreground mb-3">DRS activation samples per lap</p>
            <div className="h-[200px]">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={drsPerLap}>
                  <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                  <XAxis dataKey="lap" tick={{ fill: '#8888a0', fontSize: 9 }} />
                  <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="drs" fill="#22c55e" name="DRS Activations" radius={[2, 2, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}
        {raceStints.length > 0 && (
          <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
            <h3 className="text-sm text-foreground mb-1">Tire Strategy</h3>
            <p className="text-[12px] text-muted-foreground mb-3">Stint breakdown — compound, laps & age</p>
            <div className="space-y-2">
              {raceStints.map((s, i) => (
                <div key={i} className="flex items-center gap-3">
                  <span className="text-[12px] text-muted-foreground w-12">Stint {s.stint}</span>
                  <div className="flex-1 h-6 bg-[#222838] rounded-md overflow-hidden relative">
                    <div className="h-full rounded-md flex items-center px-2"
                      style={{ backgroundColor: `${compoundColors[s.compound] || '#8888a0'}30`, borderLeft: `3px solid ${compoundColors[s.compound] || '#8888a0'}`, width: `${Math.min(100, (s.stintLaps / 30) * 100)}%` }}
                    >
                      <span className="text-[12px] font-mono text-foreground">{s.compound}</span>
                    </div>
                  </div>
                  <span className="text-[12px] text-muted-foreground w-20 text-right">L{s.lapStart}–{s.lapEnd}</span>
                  <span className="text-[12px] font-mono text-foreground w-16 text-right">{s.stintLaps} laps</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </>
  );
}

/* ─── Season Compare View ─── */

function SeasonCompareView({ seasonSummary, loading, year, driver, highlightRace }: {
  seasonSummary: CarSummary[]; loading: boolean; year: number; driver: string; highlightRace: string;
}) {
  if (loading) {
    return <div className="flex items-center justify-center h-[400px]"><Loader2 className="w-6 h-6 text-[#FF8000] animate-spin" /><span className="ml-2 text-sm text-muted-foreground">Computing season telemetry summary...</span></div>;
  }
  if (!seasonSummary.length) {
    return <div className="flex items-center justify-center h-[400px] text-muted-foreground text-sm">No telemetry data available for {year}</div>;
  }

  const seasonAvgSpeed = seasonSummary.reduce((s, r) => s + r.avgSpeed, 0) / seasonSummary.length;
  const seasonTopSpeed = Math.max(...seasonSummary.map(r => r.topSpeed));
  const seasonAvgRPM = seasonSummary.reduce((s, r) => s + r.avgRPM, 0) / seasonSummary.length;
  const seasonAvgThrottle = seasonSummary.reduce((s, r) => s + r.avgThrottle, 0) / seasonSummary.length;
  const fastestRace = seasonSummary.reduce((best, r) => r.topSpeed > best.topSpeed ? r : best, seasonSummary[0]);
  const heaviestBraking = seasonSummary.reduce((best, r) => r.brakePct > best.brakePct ? r : best, seasonSummary[0]);

  return (
    <>
      <div className="grid grid-cols-6 gap-3">
        <KPI icon={<Gauge className="w-4 h-4 text-[#FF8000]" />} label="SEASON TOP SPEED" value={`${seasonTopSpeed.toFixed(0)} km/h`} detail={`${fastestRace.race} GP`} color="text-[#FF8000]" />
        <KPI icon={<Gauge className="w-4 h-4 text-cyan-400" />} label="SEASON AVG SPEED" value={`${seasonAvgSpeed.toFixed(1)} km/h`} detail={`Across ${seasonSummary.length} races`} color="text-cyan-400" />
        <KPI icon={<Zap className="w-4 h-4 text-purple-400" />} label="SEASON AVG RPM" value={seasonAvgRPM.toFixed(0)} detail="Engine average" color="text-purple-400" />
        <KPI icon={<Wind className="w-4 h-4 text-green-400" />} label="AVG THROTTLE" value={`${seasonAvgThrottle.toFixed(1)}%`} detail="Season average" color="text-green-400" />
        <KPI icon={<Disc className="w-4 h-4 text-red-400" />} label="HEAVIEST BRAKING" value={`${heaviestBraking.brakePct.toFixed(1)}%`} detail={`${heaviestBraking.race} GP`} color="text-red-400" />
        <KPI icon={<GitCompareArrows className="w-4 h-4 text-amber-400" />} label="FASTEST CIRCUIT" value={fastestRace.race} detail={`${fastestRace.topSpeed.toFixed(0)} km/h top`} color="text-amber-400" />
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Top Speed by Race</h3>
          <p className="text-[12px] text-muted-foreground mb-3">{driver} — {year} season</p>
          <div className="h-[260px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={seasonSummary}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={['auto', 'auto']} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="topSpeed" name="Top Speed (km/h)" radius={[2, 2, 0, 0]}>
                  {seasonSummary.map((entry, i) => <Cell key={i} fill={entry.race === highlightRace ? '#FF8000' : '#FF800080'} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Average Speed by Race</h3>
          <p className="text-[12px] text-muted-foreground mb-3">Mean speed across all telemetry samples</p>
          <div className="h-[260px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={seasonSummary}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={['auto', 'auto']} />
                <Tooltip content={<CustomTooltip />} />
                <ReferenceLine y={seasonAvgSpeed} stroke="#22d3ee" strokeDasharray="4 2" strokeOpacity={0.6} />
                <Bar dataKey="avgSpeed" name="Avg Speed (km/h)" radius={[2, 2, 0, 0]}>
                  {seasonSummary.map((entry, i) => <Cell key={i} fill={entry.race === highlightRace ? '#22d3ee' : '#22d3ee80'} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Throttle vs Brake by Race</h3>
          <p className="text-[12px] text-muted-foreground mb-3">Average throttle % and braking %</p>
          <div className="h-[260px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={seasonSummary}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="avgThrottle" name="Throttle %" fill="#22c55e" radius={[2, 2, 0, 0]} />
                <Bar dataKey="brakePct" name="Brake %" fill="#ef4444" radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">DRS Usage by Race</h3>
          <p className="text-[12px] text-muted-foreground mb-3">% of telemetry samples with DRS open</p>
          <div className="h-[260px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={seasonSummary}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={[0, 'auto']} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="drsPct" name="DRS %" radius={[2, 2, 0, 0]}>
                  {seasonSummary.map((entry, i) => <Cell key={i} fill={entry.race === highlightRace ? '#22c55e' : '#22c55e80'} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Season Summary Table */}
      <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
        <h3 className="text-sm text-foreground mb-3 flex items-center gap-2">
          <GitCompareArrows className="w-3 h-3 text-[#FF8000]" />{driver} — {year} Season Telemetry Summary
        </h3>
        <div className="overflow-x-auto">
          <div className="grid grid-cols-[160px_80px_80px_80px_80px_80px_80px_80px_120px] gap-px bg-[rgba(255,128,0,0.12)] rounded-lg overflow-hidden min-w-[840px]">
            {['Race', 'Avg Spd', 'Top Spd', 'Avg RPM', 'Max RPM', 'Throttle', 'Brake %', 'DRS %', 'Compounds'].map(h => (
              <div key={h} className="bg-[#222838] px-3 py-2 text-[11px] text-muted-foreground tracking-wider">{h}</div>
            ))}
            {seasonSummary.map((r, i) => (
              <React.Fragment key={i}>
                <div className={`px-3 py-1.5 text-sm ${r.race === highlightRace ? 'bg-[#FF8000]/10 text-[#FF8000]' : 'bg-[#1A1F2E] text-foreground'}`}>{r.race} GP</div>
                <div className={`px-3 py-1.5 text-sm font-mono ${r.race === highlightRace ? 'bg-[#FF8000]/10' : 'bg-[#1A1F2E]'} text-foreground`}>{r.avgSpeed.toFixed(1)}</div>
                <div className={`px-3 py-1.5 text-sm font-mono ${r.race === highlightRace ? 'bg-[#FF8000]/10' : 'bg-[#1A1F2E]'} ${r.topSpeed >= seasonTopSpeed - 5 ? 'text-[#FF8000]' : 'text-foreground'}`}>{r.topSpeed.toFixed(0)}</div>
                <div className={`px-3 py-1.5 text-sm font-mono ${r.race === highlightRace ? 'bg-[#FF8000]/10' : 'bg-[#1A1F2E]'} text-foreground`}>{r.avgRPM}</div>
                <div className={`px-3 py-1.5 text-sm font-mono ${r.race === highlightRace ? 'bg-[#FF8000]/10' : 'bg-[#1A1F2E]'} text-foreground`}>{r.maxRPM}</div>
                <div className={`px-3 py-1.5 text-sm font-mono ${r.race === highlightRace ? 'bg-[#FF8000]/10' : 'bg-[#1A1F2E]'} text-green-400`}>{r.avgThrottle.toFixed(1)}%</div>
                <div className={`px-3 py-1.5 text-sm font-mono ${r.race === highlightRace ? 'bg-[#FF8000]/10' : 'bg-[#1A1F2E]'} ${r.brakePct > 15 ? 'text-red-400' : 'text-foreground'}`}>{r.brakePct.toFixed(1)}</div>
                <div className={`px-3 py-1.5 text-sm font-mono ${r.race === highlightRace ? 'bg-[#FF8000]/10' : 'bg-[#1A1F2E]'} text-foreground`}>{r.drsPct.toFixed(1)}</div>
                <div className={`px-3 py-1.5 text-[12px] ${r.race === highlightRace ? 'bg-[#FF8000]/10' : 'bg-[#1A1F2E]'}`}>
                  {r.compounds.map((c, ci) => <span key={ci} className="font-mono mr-1" style={{ color: compoundColors[c] || '#8888a0' }}>{c}</span>)}
                </div>
              </React.Fragment>
            ))}
          </div>
        </div>
      </div>
    </>
  );
}

/* ─── Head-to-Head View ─── */

function H2HView({ mode, loading, year, race, raceData, seasonNor, seasonPia }: {
  mode: 'race' | 'season'; loading: boolean; year: number; race: string;
  raceData: Record<string, string>[]; seasonNor: CarSummary[]; seasonPia: CarSummary[];
}) {
  if (loading) {
    return <div className="flex items-center justify-center h-[400px]"><Loader2 className="w-6 h-6 text-[#FF8000] animate-spin" /><span className="ml-2 text-sm text-muted-foreground">Loading head-to-head data...</span></div>;
  }
  if (mode === 'race') return <H2HRaceView raceData={raceData} race={race} year={year} />;
  return <H2HSeasonView seasonNor={seasonNor} seasonPia={seasonPia} year={year} />;
}

function H2HRaceView({ raceData, race, year }: { raceData: Record<string, string>[]; race: string; year: number }) {
  const norData = useMemo(() => raceData.filter(r => r.Driver === 'NOR'), [raceData]);
  const piaData = useMemo(() => raceData.filter(r => r.Driver === 'PIA'), [raceData]);
  const norKpis = useMemo(() => computeCarKpis(norData), [norData]);
  const piaKpis = useMemo(() => computeCarKpis(piaData), [piaData]);

  // Speed trace overlay
  const mergedSpeed = useMemo(() => {
    const norSampled = downsample(norData);
    const piaSampled = downsample(piaData);
    const map = new Map<string, { dist: string; norSpeed?: number; piaSpeed?: number }>();
    for (const r of norSampled) {
      const dist = (Number(r.Distance) / 1000).toFixed(1);
      map.set(dist, { ...map.get(dist), dist, norSpeed: Number(r.Speed) || 0 });
    }
    for (const r of piaSampled) {
      const dist = (Number(r.Distance) / 1000).toFixed(1);
      map.set(dist, { ...map.get(dist), dist, piaSpeed: Number(r.Speed) || 0 });
    }
    return Array.from(map.values()).sort((a, b) => parseFloat(a.dist) - parseFloat(b.dist));
  }, [norData, piaData]);

  // Lap times overlay
  const norLaps = useMemo(() => buildLapTimes(norData), [norData]);
  const piaLaps = useMemo(() => buildLapTimes(piaData), [piaData]);
  const mergedLapTimes = useMemo(() => {
    const map = new Map<number, { lap: number; norTime?: number; piaTime?: number }>();
    for (const d of norLaps) map.set(d.lap, { ...map.get(d.lap), lap: d.lap, norTime: d.time });
    for (const d of piaLaps) map.set(d.lap, { ...map.get(d.lap), lap: d.lap, piaTime: d.time });
    return Array.from(map.values()).sort((a, b) => a.lap - b.lap);
  }, [norLaps, piaLaps]);

  if (!norData.length && !piaData.length) {
    return <div className="flex items-center justify-center h-[300px] text-muted-foreground text-sm">No data available for {race} GP {year}</div>;
  }

  return (
    <>
      <div className="grid grid-cols-4 gap-3">
        <DeltaKPI label="TOP SPEED" val1={norKpis?.topSpeed || '—'} val2={piaKpis?.topSpeed || '—'} label1="NOR" label2="PIA" unit=" km/h"
          icon={<Gauge className="w-4 h-4 text-[#FF8000]" />} />
        <DeltaKPI label="AVG RPM" val1={norKpis?.avgRPM || '—'} val2={piaKpis?.avgRPM || '—'} label1="NOR" label2="PIA" unit=""
          icon={<Zap className="w-4 h-4 text-cyan-400" />} />
        <DeltaKPI label="AVG THROTTLE" val1={norKpis?.avgThrottle || '—'} val2={piaKpis?.avgThrottle || '—'} label1="NOR" label2="PIA" unit="%"
          icon={<Wind className="w-4 h-4 text-green-400" />} />
        <DeltaKPI label="DRS USAGE" val1={norKpis?.drsPct || '—'} val2={piaKpis?.drsPct || '—'} label1="NOR" label2="PIA" unit="%"
          icon={<Wind className="w-4 h-4 text-green-400" />} />
      </div>

      {/* Speed Trace Overlay */}
      {mergedSpeed.length > 0 && (
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Speed Trace Overlay — {race} GP {year}</h3>
          <p className="text-[12px] text-muted-foreground mb-3">
            <span style={{ color: NOR_COLOR }}>■</span> NOR vs <span style={{ color: PIA_COLOR }}>■</span> PIA — km/h over distance
          </p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={mergedSpeed}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="dist" tick={{ fill: '#8888a0', fontSize: 9 }} tickCount={10} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={[0, 'auto']} />
                <Tooltip content={<CustomTooltip />} />
                <Line type="monotone" dataKey="norSpeed" stroke={NOR_COLOR} strokeWidth={1.5} dot={false} name="NOR Speed" />
                <Line type="monotone" dataKey="piaSpeed" stroke={PIA_COLOR} strokeWidth={1.5} dot={false} name="PIA Speed" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Lap Times Overlay */}
      {mergedLapTimes.length > 0 && (
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Lap Times — NOR vs PIA</h3>
          <p className="text-[12px] text-muted-foreground mb-3">Lap time (seconds) per lap</p>
          <div className="h-[260px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={mergedLapTimes}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="lap" tick={{ fill: '#8888a0', fontSize: 9 }} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={['auto', 'auto']} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="norTime" name="NOR (s)" fill={NOR_COLOR} radius={[2, 2, 0, 0]} />
                <Bar dataKey="piaTime" name="PIA (s)" fill={PIA_COLOR} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </>
  );
}

function H2HSeasonView({ seasonNor, seasonPia, year }: { seasonNor: CarSummary[]; seasonPia: CarSummary[]; year: number }) {
  const merged = useMemo(() => {
    const allRaces = [...new Set([...seasonNor.map(r => r.race), ...seasonPia.map(r => r.race)])];
    return allRaces.map(race => {
      const nor = seasonNor.find(r => r.race === race);
      const pia = seasonPia.find(r => r.race === race);
      return {
        race,
        norTop: nor?.topSpeed ?? 0, piaTop: pia?.topSpeed ?? 0,
        norAvg: nor?.avgSpeed ?? 0, piaAvg: pia?.avgSpeed ?? 0,
        norThrottle: nor?.avgThrottle ?? 0, piaThrottle: pia?.avgThrottle ?? 0,
        norBrake: nor?.brakePct ?? 0, piaBrake: pia?.brakePct ?? 0,
      };
    });
  }, [seasonNor, seasonPia]);

  const avg = (arr: CarSummary[], key: keyof CarSummary) => arr.length ? (arr.reduce((s, r) => s + (r[key] as number), 0) / arr.length).toFixed(1) : '—';
  const topOf = (arr: CarSummary[]) => arr.length ? Math.max(...arr.map(r => r.topSpeed)).toFixed(0) : '—';

  if (!merged.length) return <div className="flex items-center justify-center h-[300px] text-muted-foreground text-sm">No season data for {year}</div>;

  return (
    <>
      <div className="grid grid-cols-4 gap-3">
        <DeltaKPI label="TOP SPEED" val1={topOf(seasonNor)} val2={topOf(seasonPia)} label1="NOR" label2="PIA" unit=" km/h" icon={<Gauge className="w-4 h-4 text-[#FF8000]" />} />
        <DeltaKPI label="AVG SPEED" val1={avg(seasonNor, 'avgSpeed')} val2={avg(seasonPia, 'avgSpeed')} label1="NOR" label2="PIA" unit=" km/h" icon={<Gauge className="w-4 h-4 text-cyan-400" />} />
        <DeltaKPI label="AVG THROTTLE" val1={avg(seasonNor, 'avgThrottle')} val2={avg(seasonPia, 'avgThrottle')} label1="NOR" label2="PIA" unit="%" icon={<Wind className="w-4 h-4 text-green-400" />} />
        <DeltaKPI label="AVG BRAKE" val1={avg(seasonNor, 'brakePct')} val2={avg(seasonPia, 'brakePct')} label1="NOR" label2="PIA" unit="%" icon={<Disc className="w-4 h-4 text-red-400" />} />
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Top Speed by Race — NOR vs PIA</h3>
          <p className="text-[12px] text-muted-foreground mb-3">{year} season</p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={merged}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={['auto', 'auto']} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="norTop" name="NOR" fill={NOR_COLOR} radius={[2, 2, 0, 0]} />
                <Bar dataKey="piaTop" name="PIA" fill={PIA_COLOR} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Avg Speed by Race — NOR vs PIA</h3>
          <p className="text-[12px] text-muted-foreground mb-3">{year} season</p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={merged}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={['auto', 'auto']} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="norAvg" name="NOR" fill={NOR_COLOR} radius={[2, 2, 0, 0]} />
                <Bar dataKey="piaAvg" name="PIA" fill={PIA_COLOR} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Throttle % — NOR vs PIA</h3>
          <p className="text-[12px] text-muted-foreground mb-3">{year} season</p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={merged}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="norThrottle" name="NOR" fill={NOR_COLOR} radius={[2, 2, 0, 0]} />
                <Bar dataKey="piaThrottle" name="PIA" fill={PIA_COLOR} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Braking % — NOR vs PIA</h3>
          <p className="text-[12px] text-muted-foreground mb-3">{year} season</p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={merged}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="norBrake" name="NOR" fill={NOR_COLOR} radius={[2, 2, 0, 0]} />
                <Bar dataKey="piaBrake" name="PIA" fill={PIA_COLOR} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </>
  );
}

/* ─── Compare View ─── */

function CompareView({ mode, loading, year, driver, race1, race2, data1, data2, season23, season24 }: {
  mode: 'race' | 'year'; loading: boolean; year: number; driver: string;
  race1: string; race2: string; data1: Record<string, string>[]; data2: Record<string, string>[];
  season23: CarSummary[]; season24: CarSummary[];
}) {
  if (loading) {
    return <div className="flex items-center justify-center h-[400px]"><Loader2 className="w-6 h-6 text-[#FF8000] animate-spin" /><span className="ml-2 text-sm text-muted-foreground">Loading comparison...</span></div>;
  }
  if (mode === 'race') return <RaceVsRaceView driver={driver} year={year} race1={race1} race2={race2} data1={data1} data2={data2} />;
  return <YearVsYearView driver={driver} season23={season23} season24={season24} />;
}

function RaceVsRaceView({ driver, year, race1, race2, data1, data2 }: {
  driver: string; year: number; race1: string; race2: string;
  data1: Record<string, string>[]; data2: Record<string, string>[];
}) {
  const d1 = useMemo(() => data1.filter(r => r.Driver === driver), [data1, driver]);
  const d2 = useMemo(() => data2.filter(r => r.Driver === driver), [data2, driver]);
  const kpis1 = useMemo(() => computeCarKpis(d1), [d1]);
  const kpis2 = useMemo(() => computeCarKpis(d2), [d2]);

  const laps1 = useMemo(() => buildLapTimes(d1), [d1]);
  const laps2 = useMemo(() => buildLapTimes(d2), [d2]);
  const mergedLaps = useMemo(() => {
    const map = new Map<number, any>();
    for (const d of laps1) map.set(d.lap, { lap: d.lap, race1Time: d.time });
    for (const d of laps2) map.set(d.lap, { ...map.get(d.lap), lap: d.lap, race2Time: d.time });
    return Array.from(map.values()).sort((a: any, b: any) => a.lap - b.lap);
  }, [laps1, laps2]);

  const R1_COLOR = '#FF8000';
  const R2_COLOR = '#a78bfa';

  if (!d1.length && !d2.length) return <div className="flex items-center justify-center h-[300px] text-muted-foreground text-sm">No data available</div>;

  return (
    <>
      <div className="grid grid-cols-4 gap-3">
        <DeltaKPI label="TOP SPEED" val1={kpis1?.topSpeed || '—'} val2={kpis2?.topSpeed || '—'} label1={race1} label2={race2} unit=" km/h" icon={<Gauge className="w-4 h-4 text-[#FF8000]" />} color1={R1_COLOR} color2={R2_COLOR} />
        <DeltaKPI label="AVG SPEED" val1={kpis1?.avgSpeed || '—'} val2={kpis2?.avgSpeed || '—'} label1={race1} label2={race2} unit=" km/h" icon={<Gauge className="w-4 h-4 text-cyan-400" />} color1={R1_COLOR} color2={R2_COLOR} />
        <DeltaKPI label="AVG THROTTLE" val1={kpis1?.avgThrottle || '—'} val2={kpis2?.avgThrottle || '—'} label1={race1} label2={race2} unit="%" icon={<Wind className="w-4 h-4 text-green-400" />} color1={R1_COLOR} color2={R2_COLOR} />
        <DeltaKPI label="BRAKE %" val1={kpis1?.brakePct || '—'} val2={kpis2?.brakePct || '—'} label1={race1} label2={race2} unit="%" icon={<Disc className="w-4 h-4 text-red-400" />} color1={R1_COLOR} color2={R2_COLOR} />
      </div>

      <div className="flex items-center gap-4 text-[12px] text-muted-foreground px-1">
        <span><span style={{ color: R1_COLOR }}>■</span> {race1} GP</span>
        <span><span style={{ color: R2_COLOR }}>■</span> {race2} GP</span>
      </div>

      {mergedLaps.length > 0 && (
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Lap Times — {race1} vs {race2}</h3>
          <p className="text-[12px] text-muted-foreground mb-3">{driver} — {year}</p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={mergedLaps}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="lap" tick={{ fill: '#8888a0', fontSize: 9 }} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={['auto', 'auto']} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="race1Time" name={`${race1} (s)`} fill={R1_COLOR} radius={[2, 2, 0, 0]} />
                <Bar dataKey="race2Time" name={`${race2} (s)`} fill={R2_COLOR} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}
    </>
  );
}

function YearVsYearView({ driver, season23, season24 }: { driver: string; season23: CarSummary[]; season24: CarSummary[] }) {
  const avg = (arr: CarSummary[], key: keyof CarSummary) => arr.length ? (arr.reduce((s, r) => s + (r[key] as number), 0) / arr.length).toFixed(1) : '—';
  const topOf = (arr: CarSummary[]) => arr.length ? Math.max(...arr.map(r => r.topSpeed)).toFixed(0) : '—';

  const commonRaces = useMemo(() => {
    const r24 = new Set(season24.map(r => r.race));
    return season23.filter(r => r24.has(r.race)).map(r => r.race);
  }, [season23, season24]);

  const merged = useMemo(() => commonRaces.map(race => {
    const r23 = season23.find(r => r.race === race)!;
    const r24 = season24.find(r => r.race === race)!;
    return { race, top23: r23.topSpeed, top24: r24.topSpeed, avg23: r23.avgSpeed, avg24: r24.avgSpeed, throttle23: r23.avgThrottle, throttle24: r24.avgThrottle, brake23: r23.brakePct, brake24: r24.brakePct };
  }), [commonRaces, season23, season24]);

  const Y23 = '#a78bfa';
  const Y24 = '#FF8000';

  if (!season23.length && !season24.length) return <div className="flex items-center justify-center h-[300px] text-muted-foreground text-sm">No season data for {driver}</div>;

  return (
    <>
      <div className="grid grid-cols-4 gap-3">
        <DeltaKPI label="TOP SPEED" val1={topOf(season23)} val2={topOf(season24)} label1="2023" label2="2024" unit=" km/h" icon={<Gauge className="w-4 h-4 text-[#FF8000]" />} color1={Y23} color2={Y24} />
        <DeltaKPI label="AVG SPEED" val1={avg(season23, 'avgSpeed')} val2={avg(season24, 'avgSpeed')} label1="2023" label2="2024" unit=" km/h" icon={<Gauge className="w-4 h-4 text-cyan-400" />} color1={Y23} color2={Y24} />
        <DeltaKPI label="AVG THROTTLE" val1={avg(season23, 'avgThrottle')} val2={avg(season24, 'avgThrottle')} label1="2023" label2="2024" unit="%" icon={<Wind className="w-4 h-4 text-green-400" />} color1={Y23} color2={Y24} />
        <DeltaKPI label="AVG BRAKE" val1={avg(season23, 'brakePct')} val2={avg(season24, 'brakePct')} label1="2023" label2="2024" unit="%" icon={<Disc className="w-4 h-4 text-red-400" />} color1={Y23} color2={Y24} />
      </div>

      <div className="flex items-center gap-4 text-[12px] text-muted-foreground px-1">
        <span><span style={{ color: Y23 }}>■</span> 2023</span>
        <span><span style={{ color: Y24 }}>■</span> 2024</span>
        <span className="ml-auto">{commonRaces.length} common races</span>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Top Speed — 2023 vs 2024</h3>
          <p className="text-[12px] text-muted-foreground mb-3">{driver} — common circuits</p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={merged}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={['auto', 'auto']} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="top23" name="2023" fill={Y23} radius={[2, 2, 0, 0]} />
                <Bar dataKey="top24" name="2024" fill={Y24} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Avg Speed — 2023 vs 2024</h3>
          <p className="text-[12px] text-muted-foreground mb-3">{driver} — common circuits</p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={merged}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} domain={['auto', 'auto']} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="avg23" name="2023" fill={Y23} radius={[2, 2, 0, 0]} />
                <Bar dataKey="avg24" name="2024" fill={Y24} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Throttle % — 2023 vs 2024</h3>
          <p className="text-[12px] text-muted-foreground mb-3">{driver} — common circuits</p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={merged}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="throttle23" name="2023" fill={Y23} radius={[2, 2, 0, 0]} />
                <Bar dataKey="throttle24" name="2024" fill={Y24} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="bg-[#1A1F2E] border border-[rgba(255,128,0,0.12)] rounded-xl shadow-[0_1px_3px_rgba(0,0,0,0.3)] p-4">
          <h3 className="text-sm text-foreground mb-1">Braking % — 2023 vs 2024</h3>
          <p className="text-[12px] text-muted-foreground mb-3">{driver} — common circuits</p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={merged}>
                <CartesianGrid stroke="rgba(255,128,0,0.12)" />
                <XAxis dataKey="race" tick={{ fill: '#8888a0', fontSize: 8 }} angle={-45} textAnchor="end" height={60} interval={0} />
                <YAxis tick={{ fill: '#8888a0', fontSize: 9 }} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="brake23" name="2023" fill={Y23} radius={[2, 2, 0, 0]} />
                <Bar dataKey="brake24" name="2024" fill={Y24} radius={[2, 2, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </>
  );
}
