// OpenF1 API Types
export interface OpenF1Session {
  session_key: number;
  session_name: string;
  session_type: string;
  date_start: string;
  date_end: string;
  year: number;
  circuit_key: number;
  circuit_short_name: string;
  country_name: string;
  country_key: number;
  location: string;
  meeting_key: number;
  meeting_name: string;
}

export interface OpenF1Position {
  session_key: number;
  meeting_key: number;
  driver_number: number;
  date: string;
  position: number;
}

export interface OpenF1Lap {
  session_key: number;
  meeting_key: number;
  driver_number: number;
  lap_number: number;
  lap_duration: number | null;
  duration_sector_1: number | null;
  duration_sector_2: number | null;
  duration_sector_3: number | null;
  is_pit_out_lap: boolean;
  date_start: string;
  st_speed: number | null;
}

export interface OpenF1Weather {
  session_key: number;
  meeting_key: number;
  date: string;
  air_temperature: number;
  track_temperature: number;
  humidity: number;
  pressure: number;
  rainfall: boolean;
  wind_direction: number;
  wind_speed: number;
}

export interface OpenF1Interval {
  session_key: number;
  meeting_key: number;
  driver_number: number;
  date: string;
  gap_to_leader: number | null;
  interval: number | null;
}

export interface OpenF1Pit {
  session_key: number;
  meeting_key: number;
  driver_number: number;
  date: string;
  lap_number: number;
  pit_duration: number | null;
}

export interface OpenF1Stint {
  session_key: number;
  meeting_key: number;
  driver_number: number;
  stint_number: number;
  lap_start: number;
  lap_end: number;
  compound: string;
  tyre_age_at_start: number;
}

export interface OpenF1Driver {
  session_key: number;
  meeting_key: number;
  driver_number: number;
  broadcast_name: string;
  full_name: string;
  name_acronym: string;
  team_name: string;
  team_colour: string;
  first_name: string;
  last_name: string;
  country_code: string;
  headshot_url: string;
}

export interface OpenF1RaceControl {
  date: string;
  session_key: number;
  meeting_key: number;
  driver_number: number | null;
  lap_number: number | null;
  category: string;
  flag: string | null;
  scope: string | null;
  message: string;
}

// Jolpica API Types
export interface JolpicaDriverStanding {
  position: string;
  positionText: string;
  points: string;
  wins: string;
  Driver: {
    driverId: string;
    permanentNumber: string;
    code: string;
    givenName: string;
    familyName: string;
    nationality: string;
  };
  Constructors: {
    constructorId: string;
    name: string;
    nationality: string;
  }[];
}

export interface JolpicaConstructorStanding {
  position: string;
  positionText: string;
  points: string;
  wins: string;
  Constructor: {
    constructorId: string;
    name: string;
    nationality: string;
  };
}

export interface JolpicaRaceResult {
  season: string;
  round: string;
  raceName: string;
  Circuit: {
    circuitId: string;
    circuitName: string;
    Location: {
      lat: string;
      long: string;
      locality: string;
      country: string;
    };
  };
  date: string;
  Results: {
    number: string;
    position: string;
    positionText: string;
    points: string;
    Driver: {
      driverId: string;
      code: string;
      givenName: string;
      familyName: string;
    };
    Constructor: {
      constructorId: string;
      name: string;
    };
    grid: string;
    laps: string;
    status: string;
    Time?: { millis: string; time: string };
    FastestLap?: {
      rank: string;
      lap: string;
      Time: { time: string };
      AverageSpeed: { units: string; speed: string };
    };
  }[];
}

// Pipeline Extraction Types
export interface RegulationRule {
  id: string;
  category: string;
  description: string | null;
  value: string | null;
  unit: string | null;
  condition: string | null;
  reference: string | null;
  severity: string;
  source_standard: string | null;
  _source: string;
  _page: number;
}

export interface EquipmentItem {
  tag: string;
  type: string;
  description: string;
  kks: string;
  specs: Record<string, string>;
  location_description: string | null;
  _source: string;
  _page: number;
}

export interface DimensionalData {
  component: string;
  dimension: string;
  value: number | string | null;
  unit: string;
  _source: string;
  _page: number;
}

export interface MaterialSpec {
  material: string;
  application: string;
  properties: Record<string, string>;
  _source: string;
  _page: number;
}

export interface DocumentPass {
  number: number;
  name: string;
  pages_processed: number;
  items_found: number;
  tokens_in: number;
  tokens_out: number;
  cost_usd: number;
  latency_s: number;
}

export interface DocumentMeta {
  name: string;
  title: string;
  document_type: string;
  revision: string;
  date: string;
  total_pages: number;
  passes: DocumentPass[];
  sections: { number: string; title: string }[];
  standards: string[];
  topics: string[];
}

export interface PipelineStats {
  total_pages: number;
  total_rules: number;
  total_equipment: number;
  total_dimensions: number;
  total_materials: number;
  total_tokens_in: number;
  total_tokens_out: number;
  total_cost_usd: number;
  total_latency_s: number;
}

export interface IntelligenceData {
  documents: DocumentMeta[];
  rules: RegulationRule[];
  equipment: EquipmentItem[];
  dimensional_data: DimensionalData[];
  material_specs: MaterialSpec[];
  stats: PipelineStats;
}

// View types
export type ViewType = 'dashboard' | 'mclaren-analytics' | 'car' | 'driver' | 'ai-insights' | 'regulations' | 'media' | 'chat' | 'fleet-overview';
