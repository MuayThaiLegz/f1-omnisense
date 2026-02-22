import { useState, useEffect, useMemo } from 'react';
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell, AreaChart, Area,
} from 'recharts';
import {
  Trophy, TrendingUp, Loader2, AlertCircle, Flag, Users,
} from 'lucide-react';
import { usePolling } from '../hooks/usePolling';
import * as jolpica from '../api/jolpica';
import { fetchCSV, parseCSV } from '../api/local';

const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload?.length) {
    return (
      <div className="bg-[#0a0a12] border border-[rgba(255,128,0,0.2)] rounded-lg p-2 text-[10px]">
        <div className="text-muted-foreground mb-1">{label}</div>
        {payload.map((entry: any, index: number) => (
          <div key={index} className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full" style={{ backgroundColor: entry.color }} />
            <span className="text-muted-foreground">{entry.name}:</span>
            <span className="text-foreground font-mono">{entry.value}</span>
          </div>
        ))}
      </div>
    );
  }
  return null;
};

const teamColors: Record<string, string> = {
  'Red Bull': '#3671C6', 'McLaren': '#FF8000', 'Ferrari': '#E8002D',
  'Mercedes': '#27F4D2', 'Aston Martin': '#229971', 'Alpine': '#FF87BC',
  'Williams': '#64C4FF', 'RB': '#6692FF', 'Kick Sauber': '#52E252',
  'Haas F1 Team': '#B6BABD',
};

const compoundColors: Record<string, string> = {
  SOFT: '#ef4444', MEDIUM: '#f59e0b', HARD: '#e8e8f0', INTERMEDIATE: '#22c55e', WET: '#3b82f6',
};

type Tab = 'season' | 'mclaren';

export function McLarenAnalytics() {
  const [tab, setTab] = useState<Tab>('season');

  return (
    <div className="space-y-4">
      {/* Tab Switcher */}
      <div className="flex items-center gap-1 bg-[#12121e] rounded-lg p-0.5 border border-[rgba(255,128,0,0.12)] w-fit">
        {([
          { id: 'season' as Tab, label: 'Season Overview' },
          { id: 'mclaren' as Tab, label: 'McLaren Data' },
        ]).map(t => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`text-xs px-4 py-1.5 rounded-md transition-all ${
              tab === t.id ? 'bg-[#FF8000]/10 text-[#FF8000]' : 'text-muted-foreground hover:text-foreground'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'season' ? <SeasonOverview /> : <McLarenDeepDive />}
    </div>
  );
}

/* ─── Season Overview (Jolpica API) ─── */

function SeasonOverview() {
  const { data: driverStandings, loading: dsLoading } = usePolling({
    fetcher: () => jolpica.getDriverStandings(),
    interval: 120000,
  });

  const { data: constructorStandings, loading: csLoading } = usePolling({
    fetcher: () => jolpica.getConstructorStandings(),
    interval: 120000,
  });

  const { data: raceResults, loading: rrLoading } = usePolling({
    fetcher: () => jolpica.getRaceResults(),
    interval: 120000,
  });

  const driverChartData = useMemo(() => {
    if (!driverStandings) return [];
    return driverStandings.slice(0, 20).map(d => ({
      name: d.Driver.code || `${d.Driver.givenName[0]}. ${d.Driver.familyName}`,
      points: Number(d.points),
      wins: Number(d.wins),
      team: d.Constructors?.[0]?.name ?? '',
    }));
  }, [driverStandings]);

  const constructorChartData = useMemo(() => {
    if (!constructorStandings) return [];
    return constructorStandings.map(c => ({
      name: c.Constructor.name,
      points: Number(c.points),
      wins: Number(c.wins),
    }));
  }, [constructorStandings]);

  const pointsProgressionData = useMemo(() => {
    if (!raceResults || !driverStandings) return [];
    const topDrivers = driverStandings.slice(0, 5).map(d => d.Driver.driverId);
    const cumPoints: Record<string, number> = {};
    topDrivers.forEach(id => { cumPoints[id] = 0; });

    return raceResults.map(race => {
      const point: any = { race: race.Circuit.circuitId.slice(0, 5).toUpperCase() };
      race.Results.forEach(r => {
        if (topDrivers.includes(r.Driver.driverId)) {
          cumPoints[r.Driver.driverId] += Number(r.points);
          point[r.Driver.code || r.Driver.familyName] = cumPoints[r.Driver.driverId];
        }
      });
      return point;
    });
  }, [raceResults, driverStandings]);

  const gridAnalysisData = useMemo(() => {
    if (!raceResults) return [];
    return raceResults.slice(-5).map(race => {
      const mclarenResult = race.Results.find(r =>
        r.Constructor.name.toLowerCase().includes('mclaren')
      );
      return {
        race: race.raceName.replace(' Grand Prix', '').slice(0, 10),
        grid: mclarenResult ? Number(mclarenResult.grid) : null,
        finish: mclarenResult ? Number(mclarenResult.position) : null,
        points: mclarenResult ? Number(mclarenResult.points) : 0,
      };
    }).filter(d => d.grid !== null);
  }, [raceResults]);

  const loading = dsLoading || csLoading || rrLoading;

  if (loading && !driverStandings) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-6 h-6 text-[#FF8000] animate-spin" />
        <span className="ml-3 text-muted-foreground text-sm">Loading season data from Jolpica...</span>
      </div>
    );
  }

  if (!driverStandings || driverStandings.length === 0) {
    return (
      <div className="flex items-center justify-center h-64">
        <AlertCircle className="w-6 h-6 text-amber-400" />
        <span className="ml-3 text-muted-foreground text-sm">No standings data available</span>
      </div>
    );
  }

  const topDriverColors = ['#FF8000', '#00d4ff', '#22c55e', '#f59e0b', '#ef4444'];

  return (
    <div className="space-y-4">
      {/* KPI Summary */}
      <div className="grid grid-cols-4 gap-3">
        <KPI
          icon={<Trophy className="w-4 h-4 text-[#FF8000]" />}
          label="Championship Leader"
          value={driverStandings[0]?.Driver.code || '—'}
          detail={`${driverStandings[0]?.points ?? 0} points`}
        />
        <KPI
          icon={<Flag className="w-4 h-4 text-cyan-400" />}
          label="Races Completed"
          value={`${raceResults?.length ?? 0}`}
          detail="This season"
        />
        <KPI
          icon={<TrendingUp className="w-4 h-4 text-green-400" />}
          label="Top Constructor"
          value={constructorStandings?.[0]?.Constructor.name.split(' ').pop() ?? '—'}
          detail={`${constructorStandings?.[0]?.points ?? 0} points`}
        />
        <KPI
          icon={<Trophy className="w-4 h-4 text-amber-400" />}
          label="Most Wins"
          value={`${Math.max(...(driverStandings?.map(d => Number(d.wins)) ?? [0]))}`}
          detail={`${driverStandings.find(d => Number(d.wins) === Math.max(...driverStandings.map(s => Number(s.wins))))?.Driver.code ?? ''}`}
        />
      </div>

      <div className="grid grid-cols-12 gap-4">
        {/* Driver Championship Standings */}
        <div className="col-span-8 bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
          <div className="flex items-center justify-between mb-3">
            <div>
              <h3 className="text-xs text-foreground">Driver Championship Standings</h3>
              <p className="text-[10px] text-muted-foreground">Points distribution — live from Jolpica API</p>
            </div>
          </div>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={driverChartData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,128,0,0.06)" />
                <XAxis type="number" stroke="#8888a0" fontSize={10} />
                <YAxis dataKey="name" type="category" stroke="#8888a0" fontSize={10} width={50} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="points" radius={[0, 4, 4, 0]} name="Points">
                  {driverChartData.map((entry, index) => (
                    <Cell key={index} fill={teamColors[entry.team] ?? '#FF8000'} opacity={0.8} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Constructor Standings */}
        <div className="col-span-4 bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
          <h3 className="text-xs text-foreground mb-1">Constructor Standings</h3>
          <p className="text-[10px] text-muted-foreground mb-3">Team championship</p>
          <div className="space-y-2">
            {constructorChartData.map((team, index) => (
              <div key={team.name} className="flex items-center gap-2">
                <span className="text-[10px] text-muted-foreground w-4 font-mono">{index + 1}</span>
                <div
                  className="w-2 h-5 rounded-full"
                  style={{ backgroundColor: teamColors[team.name] ?? '#555' }}
                />
                <span className="text-xs text-foreground flex-1 truncate">{team.name}</span>
                <span className="text-xs font-mono text-[#FF8000]">{team.points}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-12 gap-4">
        {/* Points Progression */}
        <div className="col-span-7 bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
          <h3 className="text-xs text-foreground mb-1">Championship Points Progression</h3>
          <p className="text-[10px] text-muted-foreground mb-3">Cumulative points — top 5 drivers</p>
          <div className="h-[220px]">
            {pointsProgressionData.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={pointsProgressionData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,128,0,0.06)" />
                  <XAxis dataKey="race" stroke="#8888a0" fontSize={9} />
                  <YAxis stroke="#8888a0" fontSize={10} />
                  <Tooltip content={<CustomTooltip />} />
                  {driverStandings?.slice(0, 5).map((d, i) => (
                    <Line
                      key={d.Driver.driverId}
                      type="monotone"
                      dataKey={d.Driver.code || d.Driver.familyName}
                      stroke={topDriverColors[i]}
                      strokeWidth={2}
                      dot={false}
                      name={d.Driver.code || d.Driver.familyName}
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex items-center justify-center h-full text-muted-foreground text-xs">
                No race data available
              </div>
            )}
          </div>
        </div>

        {/* McLaren Grid vs Finish */}
        <div className="col-span-5 bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
          <h3 className="text-xs text-foreground mb-1">McLaren: Grid vs Finish</h3>
          <p className="text-[10px] text-muted-foreground mb-3">Recent races qualifying vs race position</p>
          <div className="h-[220px]">
            {gridAnalysisData.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={gridAnalysisData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,128,0,0.06)" />
                  <XAxis dataKey="race" stroke="#8888a0" fontSize={9} />
                  <YAxis stroke="#8888a0" fontSize={10} reversed domain={[1, 20]} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="grid" fill="#8888a0" opacity={0.5} radius={[3, 3, 0, 0]} name="Grid" />
                  <Bar dataKey="finish" fill="#FF8000" opacity={0.8} radius={[3, 3, 0, 0]} name="Finish" />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex items-center justify-center h-full text-muted-foreground text-xs">
                No McLaren race data
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Race Results Table */}
      <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
        <h3 className="text-xs text-muted-foreground tracking-widest mb-3">SEASON RACE RESULTS</h3>
        <div className="space-y-1">
          <div className="grid grid-cols-[40px_180px_120px_80px_80px_80px_1fr] gap-2 px-2 py-1 text-[9px] text-muted-foreground tracking-wider">
            <span>RND</span>
            <span>GRAND PRIX</span>
            <span>CIRCUIT</span>
            <span>WINNER</span>
            <span>TEAM</span>
            <span>TIME</span>
            <span>LAPS</span>
          </div>
          {raceResults?.slice(-10).reverse().map((race) => {
            const winner = race.Results[0];
            return (
              <div key={race.round} className="grid grid-cols-[40px_180px_120px_80px_80px_80px_1fr] gap-2 px-2 py-1.5 rounded-lg hover:bg-[#1a1a2e] transition-colors text-xs items-center">
                <span className="text-[#FF8000] font-mono">R{race.round}</span>
                <span className="text-foreground truncate">{race.raceName.replace(' Grand Prix', '')}</span>
                <span className="text-muted-foreground truncate">{race.Circuit.Location.locality}</span>
                <span className="text-foreground">{winner?.Driver.code ?? '—'}</span>
                <span className="text-muted-foreground truncate">{winner?.Constructor.name ?? ''}</span>
                <span className="text-muted-foreground font-mono">{winner?.Time?.time ?? '—'}</span>
                <span className="text-muted-foreground font-mono">{winner?.laps ?? '—'}</span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

/* ─── McLaren Deep Dive (CSV data from f1data/) ─── */

function McLarenDeepDive() {
  const [loading, setLoading] = useState(true);
  const [year, setYear] = useState<'2024' | '2023'>('2024');
  const [champDrivers, setChampDrivers] = useState<Record<string, string>[]>([]);
  const [champTeams, setChampTeams] = useState<Record<string, string>[]>([]);
  const [pitStops, setPitStops] = useState<Record<string, string>[]>([]);
  const [tireStints, setTireStints] = useState<Record<string, string>[]>([]);
  const [sessionResults, setSessionResults] = useState<Record<string, string>[]>([]);
  const [overtakes, setOvertakes] = useState<Record<string, string>[]>([]);
  const [startingGrid, setStartingGrid] = useState<Record<string, string>[]>([]);

  useEffect(() => {
    setLoading(true);
    Promise.allSettled([
      fetchCSV(`f1data/McResults/${year}/championship_drivers.csv`).then(csv => setChampDrivers(parseCSV(csv))),
      fetchCSV(`f1data/McResults/${year}/championship_teams.csv`).then(csv => setChampTeams(parseCSV(csv))),
      fetchCSV(`f1data/McStrategy/${year}/pit_stops.csv`).then(csv => setPitStops(parseCSV(csv))),
      fetchCSV(`f1data/McRaceContext/${year}/tire_stints.csv`).then(csv => setTireStints(parseCSV(csv))),
      fetchCSV(`f1data/McResults/${year}/session_results.csv`).then(csv => setSessionResults(parseCSV(csv))),
      fetchCSV(`f1data/McResults/${year}/overtakes.csv`).then(csv => setOvertakes(parseCSV(csv))),
      fetchCSV(`f1data/McResults/${year}/starting_grid.csv`).then(csv => setStartingGrid(parseCSV(csv))),
    ]).finally(() => setLoading(false));
  }, [year]);

  const pointsProgression = useMemo(() => {
    if (!champDrivers.length) return [];
    const byRace = new Map<string, { race: string; NOR: number; PIA: number }>();
    champDrivers.forEach(row => {
      const race = row.meeting_name?.replace(' Grand Prix', '') ?? '';
      if (!race || race.includes('Testing')) return;
      byRace.set(race, {
        race,
        NOR: row.driver_acronym === 'NOR' ? Number(row.points_current || 0) : byRace.get(race)?.NOR ?? 0,
        PIA: row.driver_acronym === 'PIA' ? Number(row.points_current || 0) : byRace.get(race)?.PIA ?? 0,
      });
    });
    return Array.from(byRace.values());
  }, [champDrivers]);

  const teamProgression = useMemo(() => {
    if (!champTeams.length) return [];
    return champTeams
      .filter(r => !r.meeting_name?.includes('Testing'))
      .map(r => ({
        race: (r.meeting_name ?? '').replace(' Grand Prix', '').slice(0, 12),
        position: Number(r.position_current || 0),
        points: Number(r.points_current || 0),
        gained: Number(r.points_gained || 0),
      }));
  }, [champTeams]);

  const pitStopData = useMemo(() => {
    if (!pitStops.length) return [];
    const raceStops = new Map<string, { race: string; NOR: number[]; PIA: number[] }>();
    pitStops.forEach(row => {
      if (!row.meeting_name || row.meeting_name.includes('Testing')) return;
      const race = row.meeting_name.replace(' Grand Prix', '').slice(0, 12);
      const dur = Number(row.pit_duration || 0);
      if (dur <= 0 || dur > 60) return;
      if (!raceStops.has(race)) raceStops.set(race, { race, NOR: [], PIA: [] });
      const entry = raceStops.get(race)!;
      if (row.driver_acronym === 'NOR') entry.NOR.push(dur);
      else if (row.driver_acronym === 'PIA') entry.PIA.push(dur);
    });
    return Array.from(raceStops.values()).map(r => ({
      race: r.race,
      NOR: r.NOR.length ? Number((r.NOR.reduce((a, b) => a + b, 0) / r.NOR.length).toFixed(1)) : null,
      PIA: r.PIA.length ? Number((r.PIA.reduce((a, b) => a + b, 0) / r.PIA.length).toFixed(1)) : null,
    }));
  }, [pitStops]);

  const compoundUsage = useMemo(() => {
    if (!tireStints.length) return [];
    const counts: Record<string, number> = {};
    tireStints.forEach(row => {
      if (row.session_type !== 'Race') return;
      const compound = row.compound || 'UNKNOWN';
      counts[compound] = (counts[compound] || 0) + 1;
    });
    return Object.entries(counts).map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value);
  }, [tireStints]);

  const gridVsFinish = useMemo(() => {
    if (!startingGrid.length || !sessionResults.length) return [];
    const races = new Map<string, any>();
    startingGrid.forEach(row => {
      if (!row.meeting_name) return;
      const race = row.meeting_name.replace(' Grand Prix', '').slice(0, 12);
      if (!races.has(race)) races.set(race, { race });
      const entry = races.get(race)!;
      if (row.driver_acronym === 'NOR') entry.norGrid = Number(row.position || 0);
      if (row.driver_acronym === 'PIA') entry.piaGrid = Number(row.position || 0);
    });
    sessionResults.forEach(row => {
      if (!row.meeting_name || row.session_type !== 'Race') return;
      const race = row.meeting_name.replace(' Grand Prix', '').slice(0, 12);
      const entry = races.get(race);
      if (!entry) return;
      if (row.driver_acronym === 'NOR') entry.norFinish = Number(row.position || 0);
      if (row.driver_acronym === 'PIA') entry.piaFinish = Number(row.position || 0);
    });
    return Array.from(races.values()).filter(r => r.norGrid || r.piaGrid);
  }, [startingGrid, sessionResults]);

  const stats = useMemo(() => {
    const latestNOR = champDrivers.filter(r => r.driver_acronym === 'NOR').pop();
    const latestPIA = champDrivers.filter(r => r.driver_acronym === 'PIA').pop();
    const latestTeam = champTeams[champTeams.length - 1];
    const totalOvertakes = overtakes.length;
    return {
      norPoints: latestNOR?.points_current ?? '—',
      norPos: latestNOR?.position_current ?? '—',
      piaPoints: latestPIA?.points_current ?? '—',
      piaPos: latestPIA?.position_current ?? '—',
      teamPoints: latestTeam?.points_current ?? '—',
      teamPos: latestTeam?.position_current ?? '—',
      totalOvertakes,
    };
  }, [champDrivers, champTeams, overtakes]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-6 h-6 text-[#FF8000] animate-spin" />
        <span className="ml-3 text-muted-foreground text-sm">Loading McLaren race data...</span>
      </div>
    );
  }

  if (!champDrivers.length) {
    return (
      <div className="flex items-center justify-center h-64">
        <AlertCircle className="w-6 h-6 text-amber-400" />
        <span className="ml-3 text-muted-foreground text-sm">No McLaren data available for {year}</span>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Year Selector + KPIs */}
      <div className="flex items-center gap-3 mb-1">
        <div className="flex items-center gap-1 bg-[#12121e] rounded-lg p-0.5 border border-[rgba(255,128,0,0.12)]">
          {(['2024', '2023'] as const).map(y => (
            <button
              key={y}
              onClick={() => setYear(y)}
              className={`text-xs px-3 py-1.5 rounded-md transition-all ${
                year === y ? 'bg-[#FF8000]/10 text-[#FF8000]' : 'text-muted-foreground hover:text-foreground'
              }`}
            >
              {y}
            </button>
          ))}
        </div>
        <span className="text-[10px] text-muted-foreground">McLaren F1 Team — Season Data from f1data/</span>
      </div>

      <div className="grid grid-cols-4 gap-3">
        <KPI icon={<Users className="w-4 h-4 text-[#FF8000]" />} label="NOR Championship" value={`P${stats.norPos}`} detail={`${stats.norPoints} pts`} />
        <KPI icon={<Users className="w-4 h-4 text-cyan-400" />} label="PIA Championship" value={`P${stats.piaPos}`} detail={`${stats.piaPoints} pts`} />
        <KPI icon={<Trophy className="w-4 h-4 text-green-400" />} label="Constructors" value={`P${stats.teamPos}`} detail={`${stats.teamPoints} pts`} />
        <KPI icon={<Flag className="w-4 h-4 text-amber-400" />} label="Overtakes" value={`${stats.totalOvertakes}`} detail={`${year} season`} />
      </div>

      <div className="grid grid-cols-12 gap-4">
        {/* Points Progression */}
        <div className="col-span-8 bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
          <h3 className="text-xs text-foreground mb-1">Driver Points Progression</h3>
          <p className="text-[10px] text-muted-foreground mb-3">Cumulative championship points — NOR vs PIA</p>
          <div className="h-[260px]">
            {pointsProgression.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={pointsProgression}>
                  <defs>
                    <linearGradient id="norGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#FF8000" stopOpacity={0.3} />
                      <stop offset="100%" stopColor="#FF8000" stopOpacity={0} />
                    </linearGradient>
                    <linearGradient id="piaGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#00d4ff" stopOpacity={0.3} />
                      <stop offset="100%" stopColor="#00d4ff" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,128,0,0.06)" />
                  <XAxis dataKey="race" stroke="#8888a0" fontSize={9} angle={-30} textAnchor="end" height={50} />
                  <YAxis stroke="#8888a0" fontSize={10} />
                  <Tooltip content={<CustomTooltip />} />
                  <Area type="monotone" dataKey="NOR" stroke="#FF8000" fill="url(#norGrad)" strokeWidth={2} dot={false} name="NOR" />
                  <Area type="monotone" dataKey="PIA" stroke="#00d4ff" fill="url(#piaGrad)" strokeWidth={2} dot={false} name="PIA" />
                </AreaChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex items-center justify-center h-full text-muted-foreground text-xs">No data</div>
            )}
          </div>
        </div>

        {/* Tire Compound Usage */}
        <div className="col-span-4 bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
          <h3 className="text-xs text-foreground mb-1">Race Tire Usage</h3>
          <p className="text-[10px] text-muted-foreground mb-3">Stint compound distribution</p>
          <div className="h-[260px]">
            {compoundUsage.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={compoundUsage} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,128,0,0.06)" />
                  <XAxis type="number" stroke="#8888a0" fontSize={10} />
                  <YAxis dataKey="name" type="category" stroke="#8888a0" fontSize={10} width={80} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="value" radius={[0, 4, 4, 0]} name="Stints">
                    {compoundUsage.map((entry) => (
                      <Cell key={entry.name} fill={compoundColors[entry.name] ?? '#8888a0'} opacity={0.8} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex items-center justify-center h-full text-muted-foreground text-xs">No tire data</div>
            )}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-12 gap-4">
        {/* Pit Stop Times */}
        <div className="col-span-6 bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
          <h3 className="text-xs text-foreground mb-1">Pit Stop Duration</h3>
          <p className="text-[10px] text-muted-foreground mb-3">Average pit duration per race (seconds)</p>
          <div className="h-[220px]">
            {pitStopData.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={pitStopData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,128,0,0.06)" />
                  <XAxis dataKey="race" stroke="#8888a0" fontSize={9} angle={-30} textAnchor="end" height={50} />
                  <YAxis stroke="#8888a0" fontSize={10} />
                  <Tooltip content={<CustomTooltip />} />
                  <Line type="monotone" dataKey="NOR" stroke="#FF8000" strokeWidth={2} dot={{ r: 3, fill: '#FF8000' }} name="NOR" connectNulls />
                  <Line type="monotone" dataKey="PIA" stroke="#00d4ff" strokeWidth={2} dot={{ r: 3, fill: '#00d4ff' }} name="PIA" connectNulls />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex items-center justify-center h-full text-muted-foreground text-xs">No pit data</div>
            )}
          </div>
        </div>

        {/* Grid vs Finish */}
        <div className="col-span-6 bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
          <h3 className="text-xs text-foreground mb-1">NOR: Grid vs Finish</h3>
          <p className="text-[10px] text-muted-foreground mb-3">Qualifying position vs race result</p>
          <div className="h-[220px]">
            {gridVsFinish.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={gridVsFinish}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,128,0,0.06)" />
                  <XAxis dataKey="race" stroke="#8888a0" fontSize={9} angle={-30} textAnchor="end" height={50} />
                  <YAxis stroke="#8888a0" fontSize={10} reversed domain={[1, 20]} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="norGrid" fill="#8888a0" opacity={0.5} radius={[3, 3, 0, 0]} name="Grid" />
                  <Bar dataKey="norFinish" fill="#FF8000" opacity={0.8} radius={[3, 3, 0, 0]} name="Finish" />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex items-center justify-center h-full text-muted-foreground text-xs">No grid data</div>
            )}
          </div>
        </div>
      </div>

      {/* Team Championship Table */}
      <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-4">
        <h3 className="text-xs text-muted-foreground tracking-widest mb-3">CONSTRUCTORS CHAMPIONSHIP PROGRESSION</h3>
        <div className="space-y-1">
          <div className="grid grid-cols-[40px_140px_60px_80px_80px] gap-2 px-2 py-1 text-[9px] text-muted-foreground tracking-wider">
            <span>RND</span>
            <span>GRAND PRIX</span>
            <span>POS</span>
            <span>POINTS</span>
            <span>GAINED</span>
          </div>
          {teamProgression.map((row, i) => (
            <div key={i} className="grid grid-cols-[40px_140px_60px_80px_80px] gap-2 px-2 py-1.5 rounded-lg hover:bg-[#1a1a2e] transition-colors text-xs items-center">
              <span className="text-[#FF8000] font-mono">R{i + 1}</span>
              <span className="text-foreground truncate">{row.race}</span>
              <span className="font-mono text-foreground">P{row.position}</span>
              <span className="font-mono text-[#FF8000]">{row.points}</span>
              <span className={`font-mono ${row.gained > 0 ? 'text-green-400' : 'text-muted-foreground'}`}>
                {row.gained > 0 ? `+${row.gained}` : row.gained}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

/* ─── Shared KPI Card ─── */

function KPI({ icon, label, value, detail }: {
  icon: React.ReactNode; label: string; value: string; detail: string;
}) {
  return (
    <div className="bg-[#12121e] border border-[rgba(255,128,0,0.08)] rounded-xl p-3">
      <div className="flex items-center gap-2 mb-1.5">
        {icon}
        <span className="text-[10px] text-muted-foreground tracking-wider">{label}</span>
      </div>
      <div className="text-lg font-mono text-foreground">{value}</div>
      <div className="text-[10px] text-green-400 mt-0.5">{detail}</div>
    </div>
  );
}
