export interface PositionData {
  lap: number;
  [driver: string]: number;
}

export interface LapTimeData {
  lap: number;
  [driver: string]: number;
}

export interface GapData {
  lap: number;
  [driver: string]: number;
}

export interface SpeedTrapEntry {
  abbreviation: string;
  teamColor: string;
  speedTrap1: number;
  speedTrap2: number;
  speedTrap3: number;
  speedTrap4: number;
  topSpeed: number;
}

export interface FastestLapEntry {
  position: number;
  abbreviation: string;
  team: string;
  teamColor: string;
  lapNumber: number;
  lapTime: string;
  gap: string;
  sector1: string;
  sector2: string;
  sector3: string;
}
