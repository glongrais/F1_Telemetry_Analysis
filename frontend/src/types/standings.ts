export interface Driver {
  position: number;
  abbreviation: string;
  firstName: string;
  lastName: string;
  team: string;
  teamColor: string;
  points: number;
  wins: number;
  headshotUrl?: string;
  countryCode: string;
}

export interface Constructor {
  position: number;
  name: string;
  color: string;
  points: number;
  wins: number;
}

export interface RaceResult {
  round: number;
  raceName: string;
  country: string;
  date: string;
  winner: string;
  winnerTeam: string;
  teamColor: string;
  gap: string;
  fastestLap: string;
}

export interface SessionEntry {
  position: number;
  driver: string;
  abbreviation: string;
  team: string;
  teamColor: string;
  bestLap: string;
  gap: string;
  sector1: string;
  sector2: string;
  sector3: string;
  s1Status: 'fastest' | 'pb' | 'normal';
  s2Status: 'fastest' | 'pb' | 'normal';
  s3Status: 'fastest' | 'pb' | 'normal';
  tyre: 'SOFT' | 'MEDIUM' | 'HARD' | 'INTERMEDIATE' | 'WET';
  laps: number;
}

export interface RaceEvent {
  round: number;
  name: string;
  country: string;
  location: string;
  date: string;
  format: 'conventional' | 'sprint';
}

export interface StandingsPoint {
  round: number;
  raceName: string;
  [driver: string]: number | string;
}
