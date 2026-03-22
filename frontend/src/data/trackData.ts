export interface TrackPoint {
  x: number;
  y: number;
}

export interface CarPosition {
  abbreviation: string;
  teamColor: string;
  position: number;
  x: number;
  y: number;
  speed: number;
}

export interface Corner {
  number: number;
  x: number;
  y: number;
  labelX: number;
  labelY: number;
}

export interface DRSZone {
  startIdx: number;
  endIdx: number;
}

// Bahrain-inspired circuit with smooth curves
function generateTrackPath(): TrackPoint[] {
  const points: TrackPoint[] = [];

  // Helper to add cubic bezier-interpolated points
  function cubicBezier(
    p0: TrackPoint, p1: TrackPoint, p2: TrackPoint, p3: TrackPoint, steps: number
  ) {
    for (let i = 0; i <= steps; i++) {
      const t = i / steps;
      const u = 1 - t;
      points.push({
        x: u * u * u * p0.x + 3 * u * u * t * p1.x + 3 * u * t * t * p2.x + t * t * t * p3.x,
        y: u * u * u * p0.y + 3 * u * u * t * p1.y + 3 * u * t * t * p2.y + t * t * t * p3.y,
      });
    }
  }

  // Start/finish straight
  for (let i = 0; i <= 25; i++) points.push({ x: 180 + i * 18, y: 420 });

  // Turn 1 – sweeping right
  cubicBezier({ x: 630, y: 420 }, { x: 700, y: 420 }, { x: 740, y: 380 }, { x: 740, y: 340 }, 20);

  // Short straight up
  for (let i = 1; i <= 8; i++) points.push({ x: 740, y: 340 - i * 12 });

  // Turn 2-3 chicane
  cubicBezier({ x: 740, y: 244 }, { x: 740, y: 210 }, { x: 720, y: 180 }, { x: 690, y: 170 }, 14);
  cubicBezier({ x: 690, y: 170 }, { x: 660, y: 160 }, { x: 650, y: 140 }, { x: 660, y: 120 }, 10);

  // Back straight (angled)
  for (let i = 1; i <= 20; i++) points.push({ x: 660 - i * 16, y: 120 - i * 1.5 });

  // Turn 4 – tight hairpin
  cubicBezier({ x: 340, y: 90 }, { x: 290, y: 82 }, { x: 240, y: 90 }, { x: 230, y: 130 }, 18);
  cubicBezier({ x: 230, y: 130 }, { x: 220, y: 170 }, { x: 260, y: 200 }, { x: 300, y: 200 }, 12);

  // Medium straight
  for (let i = 1; i <= 10; i++) points.push({ x: 300, y: 200 + i * 10 });

  // Turn 5-6 complex
  cubicBezier({ x: 300, y: 300 }, { x: 300, y: 340 }, { x: 270, y: 360 }, { x: 230, y: 360 }, 12);
  cubicBezier({ x: 230, y: 360 }, { x: 190, y: 360 }, { x: 160, y: 380 }, { x: 160, y: 400 }, 10);

  // Final curve back to start
  cubicBezier({ x: 160, y: 400 }, { x: 160, y: 420 }, { x: 170, y: 420 }, { x: 180, y: 420 }, 8);

  return points;
}

export const trackPoints = generateTrackPath();

// Corner positions (index into trackPoints + label offsets)
export const corners: Corner[] = [
  { number: 1, x: 740, y: 380, labelX: 760, labelY: 390 },
  { number: 2, x: 740, y: 220, labelX: 760, labelY: 220 },
  { number: 3, x: 660, y: 140, labelX: 672, labelY: 128 },
  { number: 4, x: 240, y: 110, labelX: 218, labelY: 100 },
  { number: 5, x: 300, y: 340, labelX: 316, labelY: 348 },
  { number: 6, x: 190, y: 360, labelX: 172, labelY: 352 },
];

// DRS detection zones (by index ranges along trackPoints)
export const drsZones: DRSZone[] = [
  { startIdx: 0, endIdx: 25 },       // Main straight
  { startIdx: 75, endIdx: 94 },      // Back straight
];

// Sector boundaries (index into trackPoints)
export const sectorBoundaries = [0, 55, 110];

export const sectorColors = [
  "hsl(1 100% 44%)",    // Sector 1 - primary/red
  "hsl(45 100% 50%)",   // Sector 2 - yellow
  "hsl(210 80% 55%)",   // Sector 3 - blue
];

// Place cars at various positions along the track
export function getCarPositions(selectedAbbrevs: string[]): CarPosition[] {
  const allCars: Omit<CarPosition, 'x' | 'y'>[] = [
    { abbreviation: "VER", teamColor: "#3671C6", position: 1, speed: 312 },
    { abbreviation: "NOR", teamColor: "#FF8000", position: 2, speed: 305 },
    { abbreviation: "LEC", teamColor: "#E80020", position: 3, speed: 298 },
    { abbreviation: "PIA", teamColor: "#FF8000", position: 4, speed: 287 },
    { abbreviation: "SAI", teamColor: "#E80020", position: 5, speed: 275 },
    { abbreviation: "HAM", teamColor: "#27F4D2", position: 6, speed: 261 },
    { abbreviation: "RUS", teamColor: "#27F4D2", position: 7, speed: 248 },
    { abbreviation: "PER", teamColor: "#3671C6", position: 8, speed: 234 },
  ];

  const totalPoints = trackPoints.length;
  const spacing = Math.floor(totalPoints / 10);

  return allCars
    .filter((c) => selectedAbbrevs.includes(c.abbreviation))
    .map((car, i) => {
      const idx = (i * spacing + Math.floor(totalPoints * 0.1)) % totalPoints;
      const point = trackPoints[idx];
      return { ...car, x: point.x, y: point.y };
    });
}
