export interface TelemetryPoint {
  distance: number;
  speed: number;
  throttle: number;
  brake: number;
  rpm: number;
  gear: number;
  drs: number;
}

// Simulated telemetry for ~one lap (~5.4km, sampled every ~25m = ~216 points)
function generateLapTelemetry(seed: number): TelemetryPoint[] {
  const points: TelemetryPoint[] = [];
  const lapDistance = 5400;
  const step = 25;

  // Define track segments: [startDist, endDist, type]
  const segments: [number, number, 'straight' | 'braking' | 'corner' | 'accel'][] = [
    [0, 600, 'straight'],
    [600, 750, 'braking'],
    [750, 950, 'corner'],
    [950, 1100, 'accel'],
    [1100, 1700, 'straight'],
    [1700, 1850, 'braking'],
    [1850, 2100, 'corner'],
    [2100, 2300, 'accel'],
    [2300, 2900, 'straight'],
    [2900, 3050, 'braking'],
    [3050, 3200, 'corner'],
    [3200, 3400, 'accel'],
    [3400, 4200, 'straight'],
    [4200, 4350, 'braking'],
    [4350, 4600, 'corner'],
    [4600, 4800, 'accel'],
    [4800, 5200, 'straight'],
    [5200, 5300, 'braking'],
    [5300, 5400, 'corner'],
  ];

  let speed = 280;
  let throttle = 100;
  let brake = 0;
  let rpm = 11200;
  let gear = 8;

  for (let d = 0; d <= lapDistance; d += step) {
    const seg = segments.find(([s, e]) => d >= s && d < e);
    const type = seg ? seg[2] : 'straight';
    const noise = (Math.sin(d * 0.01 + seed) * 3);

    switch (type) {
      case 'straight':
        speed = Math.min(340, speed + 4 + noise * 0.5);
        throttle = Math.min(100, throttle + 8);
        brake = 0;
        rpm = Math.min(12500, rpm + 150);
        gear = speed > 300 ? 8 : speed > 250 ? 7 : speed > 200 ? 6 : 5;
        break;
      case 'braking':
        speed = Math.max(80, speed - 18 + noise * 0.3);
        throttle = 0;
        brake = Math.min(100, 80 + Math.abs(noise));
        rpm = Math.max(8000, rpm - 400);
        gear = speed > 200 ? 5 : speed > 150 ? 4 : speed > 100 ? 3 : 2;
        break;
      case 'corner':
        speed = Math.max(70, Math.min(160, speed + noise));
        throttle = Math.min(40, Math.max(0, throttle + noise));
        brake = Math.max(0, 20 + noise);
        rpm = Math.max(7000, Math.min(9500, rpm + noise * 50));
        gear = speed > 130 ? 4 : speed > 100 ? 3 : 2;
        break;
      case 'accel':
        speed = Math.min(280, speed + 10 + noise * 0.3);
        throttle = Math.min(100, throttle + 12);
        brake = 0;
        rpm = Math.min(12000, rpm + 300);
        gear = speed > 250 ? 7 : speed > 200 ? 6 : speed > 150 ? 5 : 4;
        break;
    }

    points.push({
      distance: d,
      speed: Math.round(speed),
      throttle: Math.round(Math.max(0, Math.min(100, throttle))),
      brake: Math.round(Math.max(0, Math.min(100, brake))),
      rpm: Math.round(rpm),
      gear,
      drs: type === 'straight' && speed > 300 ? 1 : 0,
    });
  }

  return points;
}

export interface DriverTelemetry {
  abbreviation: string;
  teamColor: string;
  data: TelemetryPoint[];
}

export const allDriverTelemetry: DriverTelemetry[] = [
  { abbreviation: "VER", teamColor: "#3671C6", data: generateLapTelemetry(1) },
  { abbreviation: "NOR", teamColor: "#FF8000", data: generateLapTelemetry(2.5) },
  { abbreviation: "LEC", teamColor: "#E80020", data: generateLapTelemetry(3.8) },
  { abbreviation: "PIA", teamColor: "#FF8000", data: generateLapTelemetry(4.2) },
  { abbreviation: "SAI", teamColor: "#E80020", data: generateLapTelemetry(5.1) },
  { abbreviation: "HAM", teamColor: "#27F4D2", data: generateLapTelemetry(6.3) },
  { abbreviation: "RUS", teamColor: "#27F4D2", data: generateLapTelemetry(7.0) },
  { abbreviation: "PER", teamColor: "#3671C6", data: generateLapTelemetry(8.5) },
];

export const mockTelemetry = allDriverTelemetry;
