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

function sessionQuery<T>(
  key: string,
  sessionId: number | null,
  fn: (id: number) => Promise<T>,
  enabled = true
) {
  return useQuery<T>({
    queryKey: [key, sessionId],
    queryFn: () => fn(sessionId!),
    enabled: sessionId !== null && enabled,
    staleTime: 60_000,
  });
}

export function useSessionLeaderboard(sessionId: number | null, enabled = true) {
  return sessionQuery("leaderboard", sessionId, async (id) => {
    const data = await fetchSessionLeaderboard(id);
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
  }, enabled);
}

export function useSessionLaps(sessionId: number | null, enabled = true) {
  return sessionQuery("laps", sessionId, fetchSessionLaps, enabled);
}

export function useSessionPositions(sessionId: number | null, enabled = true) {
  return sessionQuery("positions", sessionId, fetchSessionPositions, enabled);
}

export function useSessionGaps(sessionId: number | null, enabled = true) {
  return sessionQuery("gaps", sessionId, fetchSessionGaps, enabled);
}

export function useSessionWeather(sessionId: number | null, enabled = true) {
  return sessionQuery("weather", sessionId, fetchSessionWeather, enabled);
}

export function useSessionStints(sessionId: number | null, enabled = true) {
  return sessionQuery("stints", sessionId, fetchSessionStints, enabled);
}

export function useSessionPitStops(sessionId: number | null, enabled = true) {
  return sessionQuery("pitStops", sessionId, fetchSessionPitStops, enabled);
}

export function useSessionRaceControl(sessionId: number | null, enabled = true) {
  return sessionQuery("raceControl", sessionId, fetchSessionRaceControl, enabled);
}

export function useSessionRadio(sessionId: number | null, enabled = true) {
  return sessionQuery("radio", sessionId, fetchSessionRadio, enabled);
}

export function useSessionSpeedTraps(sessionId: number | null, enabled = true) {
  return sessionQuery("speedTraps", sessionId, fetchSessionSpeedTraps, enabled);
}

export function useSessionFastestLaps(sessionId: number | null, enabled = true) {
  return sessionQuery("fastestLaps", sessionId, fetchSessionFastestLaps, enabled);
}
