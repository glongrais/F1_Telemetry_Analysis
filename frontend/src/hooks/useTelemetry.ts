import { useQuery } from "@tanstack/react-query";
import { fetchSessionTelemetry, fetchAvailableLaps } from "@/lib/api";
import type { DriverTelemetry } from "@/data/telemetryData";

export function useSessionTelemetry(
  sessionId: number | null,
  drivers: string[],
  lap: number | null
) {
  return useQuery<DriverTelemetry[]>({
    queryKey: ["telemetry", sessionId, drivers, lap],
    queryFn: () => fetchSessionTelemetry(sessionId!, drivers, lap!),
    enabled: sessionId !== null && drivers.length > 0 && lap !== null,
    staleTime: 300_000,
  });
}

export function useAvailableLaps(sessionId: number | null, driver: string | null) {
  return useQuery<number[]>({
    queryKey: ["availableLaps", sessionId, driver],
    queryFn: () => fetchAvailableLaps(sessionId!, driver!),
    enabled: sessionId !== null && driver !== null,
    staleTime: 300_000,
  });
}
