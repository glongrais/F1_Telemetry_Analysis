import { useQuery } from "@tanstack/react-query";
import { fetchRecentResults } from "@/lib/api";
import type { RaceResult } from "@/data/mockData";

export function useRecentResults(year: number) {
  return useQuery<RaceResult[]>({
    queryKey: ["recentResults", year],
    queryFn: async () => {
      const data = await fetchRecentResults(year);
      return data.map((r: any) => ({
        round: r.round,
        raceName: r.name,
        country: r.countryCode ?? "",
        date: "",
        winner: r.winner,
        winnerTeam: r.winnerTeam ?? "",
        teamColor: "",
        gap: r.gap ?? "",
        fastestLap: r.fastestLap ?? "",
      }));
    },
    staleTime: 60_000,
  });
}
