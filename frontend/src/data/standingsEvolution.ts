export interface StandingsPoint {
  round: number;
  raceName: string;
  [driver: string]: number | string; // driver abbreviation -> cumulative points
}

export const driverStandingsEvolution: StandingsPoint[] = [
  { round: 1, raceName: "BHR", VER: 25, NOR: 8, LEC: 18, PIA: 6, SAI: 15, HAM: 10, RUS: 12, PER: 4 },
  { round: 2, raceName: "SAU", VER: 51, NOR: 14, LEC: 36, PIA: 16, SAI: 33, HAM: 18, RUS: 24, PER: 12 },
  { round: 3, raceName: "AUS", VER: 77, NOR: 26, LEC: 40, PIA: 34, SAI: 48, HAM: 33, RUS: 40, PER: 18 },
  { round: 4, raceName: "JPN", VER: 102, NOR: 44, LEC: 58, PIA: 52, SAI: 63, HAM: 43, RUS: 53, PER: 28 },
  { round: 5, raceName: "CHN", VER: 136, NOR: 76, LEC: 76, PIA: 68, SAI: 75, HAM: 55, RUS: 65, PER: 40 },
  { round: 6, raceName: "MIA", VER: 169, NOR: 113, LEC: 105, PIA: 98, SAI: 95, HAM: 72, RUS: 82, PER: 52 },
  { round: 7, raceName: "EMI", VER: 194, NOR: 146, LEC: 140, PIA: 124, SAI: 118, HAM: 92, RUS: 98, PER: 64 },
  { round: 8, raceName: "MON", VER: 219, NOR: 174, LEC: 172, PIA: 148, SAI: 140, HAM: 108, RUS: 112, PER: 72 },
];

export const constructorStandingsEvolution: StandingsPoint[] = [
  { round: 1, raceName: "BHR", McLaren: 14, Ferrari: 33, "Red Bull": 29, Mercedes: 22, "Aston Martin": 8 },
  { round: 2, raceName: "SAU", McLaren: 30, Ferrari: 69, "Red Bull": 63, Mercedes: 42, "Aston Martin": 15 },
  { round: 3, raceName: "AUS", McLaren: 60, Ferrari: 88, "Red Bull": 95, Mercedes: 73, "Aston Martin": 22 },
  { round: 4, raceName: "JPN", McLaren: 96, Ferrari: 121, "Red Bull": 130, Mercedes: 96, "Aston Martin": 30 },
  { round: 5, raceName: "CHN", McLaren: 144, Ferrari: 151, "Red Bull": 176, Mercedes: 120, "Aston Martin": 40 },
  { round: 6, raceName: "MIA", McLaren: 211, Ferrari: 200, "Red Bull": 221, Mercedes: 154, "Aston Martin": 50 },
  { round: 7, raceName: "EMI", McLaren: 270, Ferrari: 258, "Red Bull": 258, Mercedes: 190, "Aston Martin": 62 },
  { round: 8, raceName: "MON", McLaren: 322, Ferrari: 312, "Red Bull": 291, Mercedes: 220, "Aston Martin": 70 },
];

export const driverColors: Record<string, string> = {
  VER: "#3671C6",
  NOR: "#FF8000",
  LEC: "#E80020",
  PIA: "#FF8000",
  SAI: "#E80020",
  HAM: "#27F4D2",
  RUS: "#27F4D2",
  PER: "#3671C6",
};

export const constructorColors: Record<string, string> = {
  McLaren: "#FF8000",
  Ferrari: "#E80020",
  "Red Bull": "#3671C6",
  Mercedes: "#27F4D2",
  "Aston Martin": "#229971",
};
