import { useQuery } from "@tanstack/react-query";
import { fetchHeadToHead } from "@/lib/api";
import type { HeadToHead } from "@/data/sessionData";

export function useHeadToHead(year: number) {
  return useQuery<HeadToHead[]>({
    queryKey: ["headToHead", year],
    queryFn: () => fetchHeadToHead(year),
    staleTime: 60_000,
  });
}
