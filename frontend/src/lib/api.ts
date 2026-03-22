const BASE_URL = import.meta.env.VITE_API_URL ?? "/api";

async function fetchApi<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) throw new Error(`API error: ${res.status} ${res.statusText}`);
  return res.json();
}

// Standings
export const fetchDriverStandings = (year: number) =>
  fetchApi<any[]>(`/standings/drivers?year=${year}`);

export const fetchConstructorStandings = (year: number) =>
  fetchApi<any[]>(`/standings/constructors?year=${year}`);

export const fetchDriverStandingsEvolution = (year: number) =>
  fetchApi<{ data: any[]; colors: Record<string, string> }>(
    `/standings/drivers/evolution?year=${year}`
  );

export const fetchConstructorStandingsEvolution = (year: number) =>
  fetchApi<{ data: any[]; colors: Record<string, string> }>(
    `/standings/constructors/evolution?year=${year}`
  );

// Events
export const fetchEvents = (year: number) =>
  fetchApi<any[]>(`/events?year=${year}`);

export const fetchSessions = (year: number, round: number) =>
  fetchApi<any[]>(`/events/${round}/sessions?year=${year}`);

export const fetchSeasons = () => fetchApi<number[]>("/seasons");

// Results
export const fetchRecentResults = (year: number, limit = 4) =>
  fetchApi<any[]>(`/results/recent?year=${year}&limit=${limit}`);

// Head to Head
export const fetchHeadToHead = (year: number) =>
  fetchApi<any[]>(`/head-to-head?year=${year}`);

// Session data
export const fetchSessionLeaderboard = (sessionId: number) =>
  fetchApi<any[]>(`/session/${sessionId}/leaderboard`);

export const fetchSessionLaps = (sessionId: number) =>
  fetchApi<any[]>(`/session/${sessionId}/laps`);

export const fetchSessionPositions = (sessionId: number) =>
  fetchApi<any[]>(`/session/${sessionId}/positions`);

export const fetchSessionGaps = (sessionId: number) =>
  fetchApi<any[]>(`/session/${sessionId}/gaps`);

export const fetchSessionWeather = (sessionId: number) =>
  fetchApi<any[]>(`/session/${sessionId}/weather`);

export const fetchSessionStints = (sessionId: number) =>
  fetchApi<any[]>(`/session/${sessionId}/stints`);

export const fetchSessionPitStops = (sessionId: number) =>
  fetchApi<any[]>(`/session/${sessionId}/pit-stops`);

export const fetchSessionRaceControl = (sessionId: number) =>
  fetchApi<any[]>(`/session/${sessionId}/race-control`);

export const fetchSessionRadio = (sessionId: number) =>
  fetchApi<any[]>(`/session/${sessionId}/radio`);

export const fetchSessionSpeedTraps = (sessionId: number) =>
  fetchApi<any[]>(`/session/${sessionId}/speed-traps`);

export const fetchSessionFastestLaps = (sessionId: number) =>
  fetchApi<any[]>(`/session/${sessionId}/fastest-laps`);

export const fetchSessionTelemetry = (
  sessionId: number,
  drivers: string[],
  lap: number
) =>
  fetchApi<any[]>(
    `/session/${sessionId}/telemetry?drivers=${drivers.join(",")}&lap=${lap}`
  );

export const fetchAvailableLaps = (sessionId: number, driver: string) =>
  fetchApi<number[]>(`/session/${sessionId}/available-laps?driver=${driver}`);
