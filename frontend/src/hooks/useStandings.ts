import { useQuery } from "@tanstack/react-query";
import {
  fetchDriverStandings,
  fetchConstructorStandings,
  fetchDriverStandingsEvolution,
  fetchConstructorStandingsEvolution,
} from "@/lib/api";
import type { Driver, Constructor } from "@/data/mockData";
import type { StandingsPoint } from "@/data/standingsEvolution";

export function useDriverStandings(year: number) {
  return useQuery<Driver[]>({
    queryKey: ["driverStandings", year],
    queryFn: () => fetchDriverStandings(year),
    staleTime: 60_000,
  });
}

export function useConstructorStandings(year: number) {
  return useQuery<Constructor[]>({
    queryKey: ["constructorStandings", year],
    queryFn: () => fetchConstructorStandings(year),
    staleTime: 60_000,
  });
}

export function useDriverStandingsEvolution(year: number) {
  return useQuery<{ data: StandingsPoint[]; colors: Record<string, string> }>({
    queryKey: ["driverStandingsEvolution", year],
    queryFn: () => fetchDriverStandingsEvolution(year),
    staleTime: 60_000,
  });
}

export function useConstructorStandingsEvolution(year: number) {
  return useQuery<{ data: StandingsPoint[]; colors: Record<string, string> }>({
    queryKey: ["constructorStandingsEvolution", year],
    queryFn: () => fetchConstructorStandingsEvolution(year),
    staleTime: 60_000,
  });
}
