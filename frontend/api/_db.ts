import { MongoClient, type Db } from 'mongodb';

let cached: { client: MongoClient; db: Db } | null = null;

export async function getDb(): Promise<Db> {
  if (cached) return cached.db;
  const uri = process.env.MONGODB_URI!;
  const dbName = process.env.MONGODB_DB || 'McLaren_f1';
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(dbName);
  cached = { client, db };
  return db;
}

export const DRIVER_NUMBERS: Record<string, number> = { NOR: 4, PIA: 81 };

export const DRIVER_META: Record<string, {
  number: number; full_name: string; broadcast_name: string;
  first_name: string; last_name: string; team_name: string;
  team_colour: string; country_code: string; headshot_url: string;
}> = {
  NOR: { number: 4, full_name: 'Lando NORRIS', broadcast_name: 'L NORRIS',
    first_name: 'Lando', last_name: 'Norris', team_name: 'McLaren',
    team_colour: 'FF8000', country_code: 'GBR', headshot_url: '' },
  PIA: { number: 81, full_name: 'Oscar PIASTRI', broadcast_name: 'O PIASTRI',
    first_name: 'Oscar', last_name: 'Piastri', team_name: 'McLaren',
    team_colour: 'FF8000', country_code: 'AUS', headshot_url: '' },
};

export const GP_TO_CIRCUIT: Record<string, string> = {
  'Abu Dhabi Grand Prix': 'Yas Marina Circuit',
  'Australian Grand Prix': 'Melbourne',
  'Azerbaijan Grand Prix': 'Baku',
  'Bahrain Grand Prix': 'Sakhir',
  'Belgian Grand Prix': 'Spa-Francorchamps',
  'British Grand Prix': 'Silverstone',
  'Canadian Grand Prix': 'Montreal',
  'Chinese Grand Prix': 'Shanghai',
  'Dutch Grand Prix': 'Zandvoort',
  'Emilia Romagna Grand Prix': 'Imola',
  'Hungarian Grand Prix': 'Hungaroring',
  'Italian Grand Prix': 'Monza',
  'Japanese Grand Prix': 'Suzuka',
  'Las Vegas Grand Prix': 'Las Vegas',
  'Mexico City Grand Prix': 'Mexico City',
  'Miami Grand Prix': 'Miami',
  'Monaco Grand Prix': 'Monte Carlo',
  'Qatar Grand Prix': 'Lusail',
  'Saudi Arabian Grand Prix': 'Jeddah',
  'Singapore Grand Prix': 'Singapore',
  'Spanish Grand Prix': 'Catalunya',
  'Austrian Grand Prix': 'Spielberg',
  'United States Grand Prix': 'Austin',
  'São Paulo Grand Prix': 'Interlagos',
  'Brazilian Grand Prix': 'Interlagos',
};

export const GP_TO_COUNTRY: Record<string, string> = {
  'Abu Dhabi Grand Prix': 'UAE',
  'Australian Grand Prix': 'Australia',
  'Azerbaijan Grand Prix': 'Azerbaijan',
  'Bahrain Grand Prix': 'Bahrain',
  'Belgian Grand Prix': 'Belgium',
  'British Grand Prix': 'Great Britain',
  'Canadian Grand Prix': 'Canada',
  'Chinese Grand Prix': 'China',
  'Dutch Grand Prix': 'Netherlands',
  'Emilia Romagna Grand Prix': 'Italy',
  'Hungarian Grand Prix': 'Hungary',
  'Italian Grand Prix': 'Italy',
  'Japanese Grand Prix': 'Japan',
  'Las Vegas Grand Prix': 'United States',
  'Mexico City Grand Prix': 'Mexico',
  'Miami Grand Prix': 'United States',
  'Monaco Grand Prix': 'Monaco',
  'Qatar Grand Prix': 'Qatar',
  'Saudi Arabian Grand Prix': 'Saudi Arabia',
  'Singapore Grand Prix': 'Singapore',
  'Spanish Grand Prix': 'Spain',
  'Austrian Grand Prix': 'Austria',
  'United States Grand Prix': 'United States',
  'São Paulo Grand Prix': 'Brazil',
  'Brazilian Grand Prix': 'Brazil',
};

export function buildSessionMap(sources: string[]): {
  srcToKey: Record<string, number>;
  yearRaceToKey: Record<string, number>;
} {
  const srcToKey: Record<string, number> = {};
  const yearRaceToKey: Record<string, number> = {};
  let sessionKey = 9000;
  for (const src of sources.sort()) {
    const parts = src.replace('.csv', '').split('_');
    if (parts.length < 3) continue;
    const year = parts[0];
    const raceName = parts.slice(1).join(' ').replace(' Race', '');
    srcToKey[src] = sessionKey;
    yearRaceToKey[`${year}|${raceName}`] = sessionKey;
    sessionKey++;
  }
  return { srcToKey, yearRaceToKey };
}

export function resolveSk(yearRaceToKey: Record<string, number>, year: string, race: string): number {
  const key = `${year}|${race}`;
  if (key in yearRaceToKey) return yearRaceToKey[key];
  const key2 = `${year}|${race} Grand Prix`;
  if (key2 in yearRaceToKey) return yearRaceToKey[key2];
  return 9000;
}
