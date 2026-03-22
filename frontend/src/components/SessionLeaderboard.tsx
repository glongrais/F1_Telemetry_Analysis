import { cn } from "@/lib/utils";
import { SessionEntry } from "@/types/standings";
import TeamLogo from "@/components/TeamLogo";

const tyreColors: Record<string, string> = {
  SOFT: "bg-tyre-soft",
  MEDIUM: "bg-tyre-medium",
  HARD: "bg-tyre-hard",
  INTERMEDIATE: "bg-tyre-intermediate",
  WET: "bg-tyre-wet",
};

const sectorClass: Record<string, string> = {
  fastest: "sector-fastest",
  pb: "sector-pb",
  normal: "text-foreground",
};

export default function SessionLeaderboard({
  entries,
  sessionName,
}: {
  entries: SessionEntry[];
  sessionName: string;
}) {
  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">{sessionName} — Leaderboard</h3>
        <div className="flex items-center gap-3 text-[10px]">
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-fastest" /> Fastest</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-personal-best" /> Personal Best</span>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border text-muted-foreground">
              <th className="text-left px-4 py-2 font-medium w-8">P</th>
              <th className="text-left px-2 py-2 font-medium">Driver</th>
              <th className="text-center px-2 py-2 font-medium">Tyre</th>
              <th className="text-right px-2 py-2 font-medium">S1</th>
              <th className="text-right px-2 py-2 font-medium">S2</th>
              <th className="text-right px-2 py-2 font-medium">S3</th>
              <th className="text-right px-2 py-2 font-medium">Best Lap</th>
              <th className="text-right px-2 py-2 font-medium">Gap</th>
              <th className="text-right px-4 py-2 font-medium">Laps</th>
            </tr>
          </thead>
          <tbody>
            {entries.map((e) => (
              <tr key={e.position} className="border-b border-border/50 hover:bg-secondary/50 transition-colors">
                <td className="px-4 py-2 font-display font-bold text-muted-foreground">{e.position}</td>
                <td className="px-2 py-2">
                  <div className="flex items-center gap-2">
                    <TeamLogo teamName={e.team} teamColor={e.teamColor} size={16} />
                    <span className="font-display font-bold">{e.abbreviation}</span>
                    <span className="text-muted-foreground hidden lg:inline">{e.team}</span>
                  </div>
                </td>
                <td className="px-2 py-2 text-center">
                  <span className={cn("inline-block w-3 h-3 rounded-full", tyreColors[e.tyre])} title={e.tyre} />
                </td>
                <td className={cn("px-2 py-2 text-right data-cell", sectorClass[e.s1Status])}>{e.sector1}</td>
                <td className={cn("px-2 py-2 text-right data-cell", sectorClass[e.s2Status])}>{e.sector2}</td>
                <td className={cn("px-2 py-2 text-right data-cell", sectorClass[e.s3Status])}>{e.sector3}</td>
                <td className="px-2 py-2 text-right data-cell font-semibold">
                  {e.position === 1 ? (
                    <span className="sector-fastest">{e.bestLap}</span>
                  ) : (
                    e.bestLap
                  )}
                </td>
                <td className="px-2 py-2 text-right data-cell text-muted-foreground">{e.gap}</td>
                <td className="px-4 py-2 text-right data-cell text-muted-foreground">{e.laps}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
