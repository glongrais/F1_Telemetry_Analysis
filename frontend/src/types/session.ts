export interface RadioMessage {
  id: number;
  timestamp: string;
  lap: number;
  driver: string;
  abbreviation: string;
  team: string;
  teamColor: string;
  message: string;
  audioUrl?: string;
}

export interface RaceControlMessage {
  id: number;
  timestamp: string;
  lap: number;
  category: "YELLOW_FLAG" | "RED_FLAG" | "SAFETY_CAR" | "VSC" | "DRS_ENABLED" | "DRS_DISABLED" | "PENALTY" | "TRACK_LIMITS" | "FLAG" | "INFO";
  message: string;
  driver?: string;
}

export interface WeatherData {
  timestamp: string;
  lap: number;
  airTemp: number;
  trackTemp: number;
  humidity: number;
  pressure: number;
  windSpeed: number;
  windDirection: number;
  rainfall: boolean;
}

export interface PitStop {
  lap: number;
  driver: string;
  abbreviation: string;
  teamColor: string;
  duration: number;
  tyreFrom: string;
  tyreTo: string;
}

export interface TyreStint {
  driver: string;
  abbreviation: string;
  teamColor: string;
  stints: { compound: string; startLap: number; endLap: number; laps: number }[];
  totalLaps: number;
}

export interface HeadToHead {
  driver1: { abbreviation: string; teamColor: string; name: string };
  driver2: { abbreviation: string; teamColor: string; name: string };
  qualiScore: [number, number];
  raceScore: [number, number];
  rounds: { round: number; raceName: string; qualiWinner: 1 | 2; raceWinner: 1 | 2 }[];
}

export interface TelemetryPoint {
  distance: number;
  speed: number;
  throttle: number;
  brake: number;
  rpm: number;
  gear: number;
  drs: number;
}

export interface DriverTelemetry {
  abbreviation: string;
  teamColor: string;
  data: TelemetryPoint[];
}
