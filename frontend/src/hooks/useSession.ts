import { useQuery } from "@tanstack/react-query";
import {
  fetchSessionLeaderboard,
  fetchSessionLaps,
  fetchSessionPositions,
  fetchSessionGaps,
  fetchSessionWeather,
  fetchSessionStints,
  fetchSessionPitStops,
  fetchSessionRaceControl,
  fetchSessionRadio,
  fetchSessionSpeedTraps,
  fetchSessionFastestLaps,
} from "@/lib/api";

function sessionQuery<T>(key: string, sessionId: number | null, fn: (id: number) => Promise<T>) {
  return useQuery<T>({
    queryKey: [key, sessionId],
    queryFn: () => fn(sessionId!),
    enabled: sessionId !== null,
    staleTime: 60_000,
  });
}

export function useSessionLeaderboard(sessionId: number | null) {
  return sessionQuery("leaderboard", sessionId, async (id) => {
    const data = await fetchSessionLeaderboard(id);
    // Transform to match SessionEntry interface
    return data.map((e: any) => ({
      position: e.position,
      driver: e.driver,
      abbreviation: e.abbreviation,
      team: e.team,
      teamColor: e.teamColor,
      bestLap: e.bestLap ?? "",
      gap: e.gapToLeader ?? "",
      sector1: e.sector1?.time ?? e.sector1 ?? "",
      sector2: e.sector2?.time ?? e.sector2 ?? "",
      sector3: e.sector3?.time ?? e.sector3 ?? "",
      s1Status: e.sector1?.status ?? "normal",
      s2Status: e.sector2?.status ?? "normal",
      s3Status: e.sector3?.status ?? "normal",
      tyre: "SOFT" as const,
      laps: e.laps ?? 0,
    }));
  });
}

export function useSessionLaps(sessionId: number | null) {
  return sessionQuery("laps", sessionId, fetchSessionLaps);
}

export function useSessionPositions(sessionId: number | null) {
  return sessionQuery("positions", sessionId, fetchSessionPositions);
}

export function useSessionGaps(sessionId: number | null) {
  return sessionQuery("gaps", sessionId, fetchSessionGaps);
}

export function useSessionWeather(sessionId: number | null) {
  return sessionQuery("weather", sessionId, fetchSessionWeather);
}

export function useSessionStints(sessionId: number | null) {
  return sessionQuery("stints", sessionId, fetchSessionStints);
}

export function useSessionPitStops(sessionId: number | null) {
  return sessionQuery("pitStops", sessionId, fetchSessionPitStops);
}

export function useSessionRaceControl(sessionId: number | null) {
  return sessionQuery("raceControl", sessionId, fetchSessionRaceControl);
}

export function useSessionRadio(sessionId: number | null) {
  return sessionQuery("radio", sessionId, fetchSessionRadio);
}

export function useSessionSpeedTraps(sessionId: number | null) {
  return sessionQuery("speedTraps", sessionId, fetchSessionSpeedTraps);
}

export function useSessionFastestLaps(sessionId: number | null) {
  return sessionQuery("fastestLaps", sessionId, fetchSessionFastestLaps);
}
