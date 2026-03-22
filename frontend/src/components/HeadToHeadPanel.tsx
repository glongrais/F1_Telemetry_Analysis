import { cn } from "@/lib/utils";
import { HeadToHead } from "@/data/sessionData";
import { Users } from "lucide-react";

interface HeadToHeadPanelProps {
  comparisons: HeadToHead[];
}

export default function HeadToHeadPanel({ comparisons }: HeadToHeadPanelProps) {
  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center gap-2">
        <Users size={14} className="text-muted-foreground" />
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Teammate Head-to-Head</h3>
      </div>
      <div className="divide-y divide-border/50">
        {comparisons.map((h2h) => {
          const totalQ = h2h.qualiScore[0] + h2h.qualiScore[1];
          const totalR = h2h.raceScore[0] + h2h.raceScore[1];
          return (
            <div key={`${h2h.driver1.abbreviation}-${h2h.driver2.abbreviation}`} className="p-4">
              {/* Driver names */}
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <div className="w-1.5 h-4 rounded-sm" style={{ backgroundColor: h2h.driver1.teamColor }} />
                  <span className="font-display text-sm font-bold">{h2h.driver1.abbreviation}</span>
                  <span className="text-[10px] text-muted-foreground">{h2h.driver1.name}</span>
                </div>
                <span className="text-[10px] text-muted-foreground font-display">VS</span>
                <div className="flex items-center gap-2">
                  <span className="text-[10px] text-muted-foreground">{h2h.driver2.name}</span>
                  <span className="font-display text-sm font-bold">{h2h.driver2.abbreviation}</span>
                  <div className="w-1.5 h-4 rounded-sm" style={{ backgroundColor: h2h.driver2.teamColor }} />
                </div>
              </div>

              {/* Qualifying bar */}
              <div className="mb-2">
                <div className="flex items-center justify-between mb-1">
                  <span className="text-[10px] font-display font-bold">{h2h.qualiScore[0]}</span>
                  <span className="text-[9px] uppercase tracking-widest text-muted-foreground">Qualifying</span>
                  <span className="text-[10px] font-display font-bold">{h2h.qualiScore[1]}</span>
                </div>
                <div className="flex h-1.5 rounded-sm overflow-hidden gap-px">
                  <div
                    className="h-full rounded-l-sm"
                    style={{
                      width: `${(h2h.qualiScore[0] / totalQ) * 100}%`,
                      backgroundColor: h2h.driver1.teamColor,
                    }}
                  />
                  <div
                    className="h-full rounded-r-sm"
                    style={{
                      width: `${(h2h.qualiScore[1] / totalQ) * 100}%`,
                      backgroundColor: h2h.driver2.teamColor,
                      opacity: 0.5,
                    }}
                  />
                </div>
              </div>

              {/* Race bar */}
              <div>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-[10px] font-display font-bold">{h2h.raceScore[0]}</span>
                  <span className="text-[9px] uppercase tracking-widest text-muted-foreground">Race</span>
                  <span className="text-[10px] font-display font-bold">{h2h.raceScore[1]}</span>
                </div>
                <div className="flex h-1.5 rounded-sm overflow-hidden gap-px">
                  <div
                    className="h-full rounded-l-sm"
                    style={{
                      width: `${(h2h.raceScore[0] / totalR) * 100}%`,
                      backgroundColor: h2h.driver1.teamColor,
                    }}
                  />
                  <div
                    className="h-full rounded-r-sm"
                    style={{
                      width: `${(h2h.raceScore[1] / totalR) * 100}%`,
                      backgroundColor: h2h.driver2.teamColor,
                      opacity: 0.5,
                    }}
                  />
                </div>
              </div>

              {/* Round dots */}
              <div className="flex items-center gap-1 mt-2 justify-center">
                {h2h.rounds.map((r) => (
                  <div
                    key={r.round}
                    className="w-3 h-3 rounded-sm flex items-center justify-center text-[7px] font-bold"
                    style={{
                      backgroundColor: r.raceWinner === 1 ? h2h.driver1.teamColor : h2h.driver2.teamColor,
                      opacity: r.raceWinner === 1 ? 1 : 0.5,
                    }}
                    title={`R${r.round} ${r.raceName}`}
                  >
                    {r.round}
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
