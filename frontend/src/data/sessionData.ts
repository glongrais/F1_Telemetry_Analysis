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

// Mock data

export const mockRadioMessages: RadioMessage[] = [
  { id: 1, timestamp: "14:23:15", lap: 3, driver: "Max Verstappen", abbreviation: "VER", team: "Red Bull Racing", teamColor: "#3671C6", message: "The front tyres are graining quite a lot." },
  { id: 2, timestamp: "14:25:42", lap: 5, driver: "Charles Leclerc", abbreviation: "LEC", team: "Ferrari", teamColor: "#E80020", message: "I'm losing the rear in turn 4. Can we adjust the diff?" },
  { id: 3, timestamp: "14:31:08", lap: 10, driver: "Lando Norris", abbreviation: "NOR", team: "McLaren", teamColor: "#FF8000", message: "Pace is really good. Tyres feel great." },
  { id: 4, timestamp: "14:35:22", lap: 14, driver: "Lewis Hamilton", abbreviation: "HAM", team: "Mercedes", teamColor: "#27F4D2", message: "Box box. Box this lap." },
  { id: 5, timestamp: "14:38:55", lap: 16, driver: "Carlos Sainz", abbreviation: "SAI", team: "Ferrari", teamColor: "#E80020", message: "Blue flags! He's not moving! Come on!" },
  { id: 6, timestamp: "14:42:10", lap: 19, driver: "Oscar Piastri", abbreviation: "PIA", team: "McLaren", teamColor: "#FF8000", message: "Can I push now or are we saving?" },
  { id: 7, timestamp: "14:45:33", lap: 22, driver: "George Russell", abbreviation: "RUS", team: "Mercedes", teamColor: "#27F4D2", message: "Massive understeer into turn 1. The fronts are gone." },
  { id: 8, timestamp: "14:50:01", lap: 27, driver: "Max Verstappen", abbreviation: "VER", team: "Red Bull Racing", teamColor: "#3671C6", message: "What's the gap to Norris?" },
  { id: 9, timestamp: "14:52:18", lap: 29, driver: "Sergio Perez", abbreviation: "PER", team: "Red Bull Racing", teamColor: "#3671C6", message: "Something doesn't feel right on the brakes." },
  { id: 10, timestamp: "14:58:44", lap: 35, driver: "Charles Leclerc", abbreviation: "LEC", team: "Ferrari", teamColor: "#E80020", message: "Is P1 realistic? What do I need?" },
];

export const mockRaceControl: RaceControlMessage[] = [
  { id: 1, timestamp: "14:15:00", lap: 0, category: "INFO", message: "LIGHTS OUT AND AWAY WE GO" },
  { id: 2, timestamp: "14:16:30", lap: 1, category: "YELLOW_FLAG", message: "YELLOW FLAG — Turn 5", driver: "STR" },
  { id: 3, timestamp: "14:18:00", lap: 2, category: "FLAG", message: "GREEN FLAG — Track clear" },
  { id: 4, timestamp: "14:22:15", lap: 5, category: "DRS_ENABLED", message: "DRS ENABLED" },
  { id: 5, timestamp: "14:35:00", lap: 14, category: "TRACK_LIMITS", message: "TRACK LIMITS — Turn 9 — lap time deleted", driver: "PER" },
  { id: 6, timestamp: "14:42:30", lap: 19, category: "TRACK_LIMITS", message: "TRACK LIMITS WARNING — Turn 9", driver: "ALO" },
  { id: 7, timestamp: "14:55:00", lap: 32, category: "SAFETY_CAR", message: "SAFETY CAR DEPLOYED" },
  { id: 8, timestamp: "14:58:00", lap: 35, category: "INFO", message: "SAFETY CAR IN THIS LAP" },
  { id: 9, timestamp: "14:59:00", lap: 36, category: "DRS_ENABLED", message: "DRS ENABLED" },
  { id: 10, timestamp: "15:10:00", lap: 48, category: "PENALTY", message: "5 SECOND TIME PENALTY — Causing a collision", driver: "PER" },
];

export const mockWeather: WeatherData[] = Array.from({ length: 50 }, (_, i) => ({
  timestamp: `14:${(15 + i).toString().padStart(2, "0")}:00`,
  lap: i + 1,
  airTemp: 28 + Math.sin(i * 0.1) * 2 + Math.random() * 0.5,
  trackTemp: 42 + Math.sin(i * 0.08) * 3 + Math.random() * 0.5,
  humidity: 45 + Math.sin(i * 0.15) * 5,
  pressure: 1013 + Math.sin(i * 0.05) * 2,
  windSpeed: 8 + Math.sin(i * 0.2) * 4,
  windDirection: 180 + Math.sin(i * 0.1) * 30,
  rainfall: false,
}));

export const mockPitStops: PitStop[] = [
  { lap: 14, driver: "Lewis Hamilton", abbreviation: "HAM", teamColor: "#27F4D2", duration: 2.4, tyreFrom: "SOFT", tyreTo: "HARD" },
  { lap: 16, driver: "George Russell", abbreviation: "RUS", teamColor: "#27F4D2", duration: 2.6, tyreFrom: "SOFT", tyreTo: "HARD" },
  { lap: 18, driver: "Max Verstappen", abbreviation: "VER", teamColor: "#3671C6", duration: 2.2, tyreFrom: "MEDIUM", tyreTo: "HARD" },
  { lap: 19, driver: "Lando Norris", abbreviation: "NOR", teamColor: "#FF8000", duration: 2.3, tyreFrom: "MEDIUM", tyreTo: "HARD" },
  { lap: 20, driver: "Charles Leclerc", abbreviation: "LEC", teamColor: "#E80020", duration: 2.5, tyreFrom: "SOFT", tyreTo: "MEDIUM" },
  { lap: 22, driver: "Oscar Piastri", abbreviation: "PIA", teamColor: "#FF8000", duration: 2.4, tyreFrom: "MEDIUM", tyreTo: "HARD" },
  { lap: 35, driver: "Charles Leclerc", abbreviation: "LEC", teamColor: "#E80020", duration: 2.7, tyreFrom: "MEDIUM", tyreTo: "SOFT" },
  { lap: 37, driver: "Max Verstappen", abbreviation: "VER", teamColor: "#3671C6", duration: 2.3, tyreFrom: "HARD", tyreTo: "MEDIUM" },
  { lap: 38, driver: "Lando Norris", abbreviation: "NOR", teamColor: "#FF8000", duration: 2.5, tyreFrom: "HARD", tyreTo: "SOFT" },
];

export const mockTyreStints: TyreStint[] = [
  { driver: "Max Verstappen", abbreviation: "VER", teamColor: "#3671C6", totalLaps: 52, stints: [
    { compound: "MEDIUM", startLap: 1, endLap: 18, laps: 18 },
    { compound: "HARD", startLap: 19, endLap: 37, laps: 19 },
    { compound: "MEDIUM", startLap: 38, endLap: 52, laps: 15 },
  ]},
  { driver: "Lando Norris", abbreviation: "NOR", teamColor: "#FF8000", totalLaps: 52, stints: [
    { compound: "MEDIUM", startLap: 1, endLap: 19, laps: 19 },
    { compound: "HARD", startLap: 20, endLap: 38, laps: 19 },
    { compound: "SOFT", startLap: 39, endLap: 52, laps: 14 },
  ]},
  { driver: "Charles Leclerc", abbreviation: "LEC", teamColor: "#E80020", totalLaps: 52, stints: [
    { compound: "SOFT", startLap: 1, endLap: 20, laps: 20 },
    { compound: "MEDIUM", startLap: 21, endLap: 35, laps: 15 },
    { compound: "SOFT", startLap: 36, endLap: 52, laps: 17 },
  ]},
  { driver: "Oscar Piastri", abbreviation: "PIA", teamColor: "#FF8000", totalLaps: 52, stints: [
    { compound: "MEDIUM", startLap: 1, endLap: 22, laps: 22 },
    { compound: "HARD", startLap: 23, endLap: 52, laps: 30 },
  ]},
  { driver: "Carlos Sainz", abbreviation: "SAI", teamColor: "#E80020", totalLaps: 52, stints: [
    { compound: "SOFT", startLap: 1, endLap: 16, laps: 16 },
    { compound: "HARD", startLap: 17, endLap: 42, laps: 26 },
    { compound: "SOFT", startLap: 43, endLap: 52, laps: 10 },
  ]},
  { driver: "Lewis Hamilton", abbreviation: "HAM", teamColor: "#27F4D2", totalLaps: 52, stints: [
    { compound: "SOFT", startLap: 1, endLap: 14, laps: 14 },
    { compound: "HARD", startLap: 15, endLap: 44, laps: 30 },
    { compound: "SOFT", startLap: 45, endLap: 52, laps: 8 },
  ]},
  { driver: "George Russell", abbreviation: "RUS", teamColor: "#27F4D2", totalLaps: 52, stints: [
    { compound: "SOFT", startLap: 1, endLap: 16, laps: 16 },
    { compound: "HARD", startLap: 17, endLap: 52, laps: 36 },
  ]},
  { driver: "Sergio Perez", abbreviation: "PER", teamColor: "#3671C6", totalLaps: 52, stints: [
    { compound: "MEDIUM", startLap: 1, endLap: 20, laps: 20 },
    { compound: "HARD", startLap: 21, endLap: 52, laps: 32 },
  ]},
];

export const mockHeadToHead: HeadToHead[] = [
  {
    driver1: { abbreviation: "VER", teamColor: "#3671C6", name: "Verstappen" },
    driver2: { abbreviation: "PER", teamColor: "#3671C6", name: "Perez" },
    qualiScore: [8, 0],
    raceScore: [7, 1],
    rounds: [
      { round: 1, raceName: "BHR", qualiWinner: 1, raceWinner: 1 },
      { round: 2, raceName: "SAU", qualiWinner: 1, raceWinner: 1 },
      { round: 3, raceName: "AUS", qualiWinner: 1, raceWinner: 1 },
      { round: 4, raceName: "JPN", qualiWinner: 1, raceWinner: 1 },
      { round: 5, raceName: "CHN", qualiWinner: 1, raceWinner: 1 },
      { round: 6, raceName: "MIA", qualiWinner: 1, raceWinner: 2 },
      { round: 7, raceName: "EMI", qualiWinner: 1, raceWinner: 1 },
      { round: 8, raceName: "MON", qualiWinner: 1, raceWinner: 1 },
    ],
  },
  {
    driver1: { abbreviation: "NOR", teamColor: "#FF8000", name: "Norris" },
    driver2: { abbreviation: "PIA", teamColor: "#FF8000", name: "Piastri" },
    qualiScore: [5, 3],
    raceScore: [5, 3],
    rounds: [
      { round: 1, raceName: "BHR", qualiWinner: 2, raceWinner: 2 },
      { round: 2, raceName: "SAU", qualiWinner: 1, raceWinner: 1 },
      { round: 3, raceName: "AUS", qualiWinner: 1, raceWinner: 2 },
      { round: 4, raceName: "JPN", qualiWinner: 1, raceWinner: 1 },
      { round: 5, raceName: "CHN", qualiWinner: 2, raceWinner: 1 },
      { round: 6, raceName: "MIA", qualiWinner: 1, raceWinner: 1 },
      { round: 7, raceName: "EMI", qualiWinner: 1, raceWinner: 1 },
      { round: 8, raceName: "MON", qualiWinner: 2, raceWinner: 2 },
    ],
  },
  {
    driver1: { abbreviation: "LEC", teamColor: "#E80020", name: "Leclerc" },
    driver2: { abbreviation: "SAI", teamColor: "#E80020", name: "Sainz" },
    qualiScore: [6, 2],
    raceScore: [5, 3],
    rounds: [
      { round: 1, raceName: "BHR", qualiWinner: 1, raceWinner: 1 },
      { round: 2, raceName: "SAU", qualiWinner: 1, raceWinner: 2 },
      { round: 3, raceName: "AUS", qualiWinner: 2, raceWinner: 2 },
      { round: 4, raceName: "JPN", qualiWinner: 1, raceWinner: 1 },
      { round: 5, raceName: "CHN", qualiWinner: 1, raceWinner: 1 },
      { round: 6, raceName: "MIA", qualiWinner: 1, raceWinner: 2 },
      { round: 7, raceName: "EMI", qualiWinner: 2, raceWinner: 1 },
      { round: 8, raceName: "MON", qualiWinner: 1, raceWinner: 1 },
    ],
  },
  {
    driver1: { abbreviation: "HAM", teamColor: "#27F4D2", name: "Hamilton" },
    driver2: { abbreviation: "RUS", teamColor: "#27F4D2", name: "Russell" },
    qualiScore: [3, 5],
    raceScore: [4, 4],
    rounds: [
      { round: 1, raceName: "BHR", qualiWinner: 2, raceWinner: 2 },
      { round: 2, raceName: "SAU", qualiWinner: 1, raceWinner: 1 },
      { round: 3, raceName: "AUS", qualiWinner: 2, raceWinner: 2 },
      { round: 4, raceName: "JPN", qualiWinner: 2, raceWinner: 1 },
      { round: 5, raceName: "CHN", qualiWinner: 1, raceWinner: 1 },
      { round: 6, raceName: "MIA", qualiWinner: 2, raceWinner: 2 },
      { round: 7, raceName: "EMI", qualiWinner: 1, raceWinner: 1 },
      { round: 8, raceName: "MON", qualiWinner: 2, raceWinner: 2 },
    ],
  },
];
