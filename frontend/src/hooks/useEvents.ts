import { useQuery } from "@tanstack/react-query";
import { fetchEvents, fetchSessions, fetchSeasons } from "@/lib/api";
import type { RaceEvent } from "@/data/mockData";

export function useEvents(year: number) {
  return useQuery<RaceEvent[]>({
    queryKey: ["events", year],
    queryFn: () => fetchEvents(year),
    staleTime: 300_000,
  });
}

export interface SessionInfo {
  sessionId: number;
  type: string;
  name: string;
  dateStart: string | null;
  dateEnd: string | null;
}

export function useSessions(year: number, round: number | null) {
  return useQuery<SessionInfo[]>({
    queryKey: ["sessions", year, round],
    queryFn: () => fetchSessions(year, round!),
    enabled: round !== null,
    staleTime: 300_000,
  });
}

export function useSeasons() {
  return useQuery<number[]>({
    queryKey: ["seasons"],
    queryFn: fetchSeasons,
    staleTime: 600_000,
  });
}
