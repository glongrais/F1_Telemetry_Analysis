import { driverStandings } from "@/data/mockData";

// Position changes over laps
export interface PositionData {
  lap: number;
  [driver: string]: number;
}

const driverColors: Record<string, string> = {
  VER: "#3671C6", NOR: "#FF8000", LEC: "#E80020", PIA: "#FF8000",
  SAI: "#E80020", HAM: "#27F4D2", RUS: "#27F4D2", PER: "#3671C6",
};

function generatePositionData(): PositionData[] {
  const drivers = ["VER", "NOR", "LEC", "PIA", "SAI", "HAM", "RUS", "PER"];
  const startPositions: Record<string, number> = {
    VER: 1, NOR: 2, LEC: 3, PIA: 4, SAI: 5, HAM: 6, RUS: 7, PER: 8,
  };
  const data: PositionData[] = [];
  const current = { ...startPositions };

  // Define some overtakes at specific laps
  const overtakes: [number, string, string][] = [
    [3, "NOR", "LEC"], [8, "HAM", "SAI"], [14, "LEC", "NOR"],
    [18, "PIA", "SAI"], [25, "NOR", "LEC"], [30, "RUS", "SAI"],
    [35, "LEC", "NOR"], [40, "NOR", "LEC"], [45, "PER", "RUS"],
  ];

  for (let lap = 0; lap <= 52; lap++) {
    // Apply overtakes
    for (const [oLap, d1, d2] of overtakes) {
      if (lap === oLap) {
        const p1 = current[d1];
        const p2 = current[d2];
        if (p1 > p2) { current[d1] = p2; current[d2] = p1; }
      }
    }
    data.push({ lap, ...current });
  }

  return data;
}

// Lap time data
export interface LapTimeData {
  lap: number;
  [driver: string]: number; // lap time in seconds
}

function generateLapTimes(): LapTimeData[] {
  const data: LapTimeData[] = [];
  const baseTime = 88; // ~1:28

  for (let lap = 1; lap <= 52; lap++) {
    const entry: LapTimeData = { lap };
    const drivers = ["VER", "NOR", "LEC", "PIA", "SAI", "HAM", "RUS", "PER"];

    for (const d of drivers) {
      const seed = d.charCodeAt(0) + d.charCodeAt(1);
      const fuelEffect = (52 - lap) * 0.015;
      const tyreEffect = lap < 20 ? lap * 0.03 : lap < 38 ? (lap - 20) * 0.02 : (lap - 38) * 0.04;
      const driverSkill = (seed % 5) * 0.1;
      const noise = Math.sin(lap * 0.5 + seed) * 0.3;

      // Pit stop laps are slower
      const isPit = (d === "VER" && (lap === 18 || lap === 37)) ||
                    (d === "NOR" && (lap === 19 || lap === 38)) ||
                    (d === "LEC" && (lap === 20 || lap === 35)) ||
                    (d === "HAM" && lap === 14) ||
                    (d === "RUS" && lap === 16);

      entry[d] = isPit ? baseTime + 20 + Math.random() * 5 : baseTime + fuelEffect + tyreEffect + driverSkill + noise;
    }
    data.push(entry);
  }
  return data;
}

// Gap to leader
export interface GapData {
  lap: number;
  [driver: string]: number;
}

function generateGapData(): GapData[] {
  const data: GapData[] = [];
  const gaps: Record<string, number> = {
    VER: 0, NOR: 1.2, LEC: 2.8, PIA: 5.1, SAI: 7.3, HAM: 9.8, RUS: 12.1, PER: 15.4,
  };

  for (let lap = 0; lap <= 52; lap++) {
    const drivers = Object.keys(gaps);
    for (const d of drivers) {
      if (d === "VER") continue;
      const seed = d.charCodeAt(0);
      const trend = Math.sin(lap * 0.08 + seed) * 0.4;
      gaps[d] = Math.max(0, gaps[d] + trend + (Math.random() - 0.45) * 0.3);
    }
    data.push({ lap, ...gaps });
  }
  return data;
}

// Speed traps
export interface SpeedTrapEntry {
  abbreviation: string;
  teamColor: string;
  speedTrap1: number;
  speedTrap2: number;
  speedTrap3: number;
  speedTrap4: number;
  topSpeed: number;
}

const mockSpeedTraps: SpeedTrapEntry[] = [
  { abbreviation: "VER", teamColor: "#3671C6", speedTrap1: 322, speedTrap2: 298, speedTrap3: 312, speedTrap4: 330, topSpeed: 335 },
  { abbreviation: "NOR", teamColor: "#FF8000", speedTrap1: 319, speedTrap2: 301, speedTrap3: 309, speedTrap4: 327, topSpeed: 332 },
  { abbreviation: "LEC", teamColor: "#E80020", speedTrap1: 324, speedTrap2: 295, speedTrap3: 315, speedTrap4: 332, topSpeed: 337 },
  { abbreviation: "PIA", teamColor: "#FF8000", speedTrap1: 318, speedTrap2: 299, speedTrap3: 308, speedTrap4: 326, topSpeed: 331 },
  { abbreviation: "SAI", teamColor: "#E80020", speedTrap1: 323, speedTrap2: 296, speedTrap3: 314, speedTrap4: 331, topSpeed: 336 },
  { abbreviation: "HAM", teamColor: "#27F4D2", speedTrap1: 320, speedTrap2: 297, speedTrap3: 310, speedTrap4: 328, topSpeed: 333 },
  { abbreviation: "RUS", teamColor: "#27F4D2", speedTrap1: 321, speedTrap2: 298, speedTrap3: 311, speedTrap4: 329, topSpeed: 334 },
  { abbreviation: "PER", teamColor: "#3671C6", speedTrap1: 317, speedTrap2: 294, speedTrap3: 307, speedTrap4: 325, topSpeed: 330 },
];

// Fastest laps
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

const mockFastestLaps: FastestLapEntry[] = [
  { position: 1, abbreviation: "VER", team: "Red Bull Racing", teamColor: "#3671C6", lapNumber: 44, lapTime: "1:28.877", gap: "-", sector1: "27.234", sector2: "33.112", sector3: "28.531" },
  { position: 2, abbreviation: "NOR", team: "McLaren", teamColor: "#FF8000", lapNumber: 46, lapTime: "1:29.012", gap: "+0.135", sector1: "27.301", sector2: "33.098", sector3: "28.613" },
  { position: 3, abbreviation: "LEC", team: "Ferrari", teamColor: "#E80020", lapNumber: 48, lapTime: "1:29.145", gap: "+0.268", sector1: "27.356", sector2: "33.201", sector3: "28.588" },
  { position: 4, abbreviation: "PIA", team: "McLaren", teamColor: "#FF8000", lapNumber: 42, lapTime: "1:29.287", gap: "+0.410", sector1: "27.412", sector2: "33.189", sector3: "28.686" },
  { position: 5, abbreviation: "SAI", team: "Ferrari", teamColor: "#E80020", lapNumber: 47, lapTime: "1:29.334", gap: "+0.457", sector1: "27.389", sector2: "33.245", sector3: "28.700" },
  { position: 6, abbreviation: "HAM", team: "Mercedes", teamColor: "#27F4D2", lapNumber: 45, lapTime: "1:29.501", gap: "+0.624", sector1: "27.445", sector2: "33.312", sector3: "28.744" },
  { position: 7, abbreviation: "RUS", team: "Mercedes", teamColor: "#27F4D2", lapNumber: 43, lapTime: "1:29.556", gap: "+0.679", sector1: "27.478", sector2: "33.289", sector3: "28.789" },
  { position: 8, abbreviation: "PER", team: "Red Bull Racing", teamColor: "#3671C6", lapNumber: 41, lapTime: "1:29.612", gap: "+0.735", sector1: "27.501", sector2: "33.345", sector3: "28.766" },
];

export const mockPositionData = generatePositionData();
export const mockLapTimes = generateLapTimes();
export const mockGapData = generateGapData();
export { driverColors, mockSpeedTraps, mockFastestLaps };
export type { SpeedTrapEntry as SpeedTrapType };
