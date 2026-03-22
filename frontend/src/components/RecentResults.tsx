import { Link } from "react-router-dom";
import { RaceResult } from "@/types/standings";
import { Flag, Clock, MapPin } from "lucide-react";
import { countryFlag } from "@/lib/countryFlag";
import { countryToCircuitId } from "@/lib/circuitMapping";

export default function RecentResults({ results }: { results: RaceResult[] }) {
  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border">
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Recent Results</h3>
      </div>
      <div className="divide-y divide-border/50">
        {results.map((r) => (
          <div key={r.round} className="px-4 py-3 hover:bg-secondary/30 transition-colors">
            <div className="flex items-center justify-between mb-1">
              <div className="flex items-center gap-2">
                <span className="font-display text-xs font-bold text-muted-foreground">R{r.round}</span>
                <span className="text-sm leading-none">{countryFlag(r.country)}</span>
                <span className="font-display text-sm font-semibold">{r.raceName}</span>
                {countryToCircuitId[r.country] && (
                  <Link
                    to={`/track/${countryToCircuitId[r.country]}`}
                    className="text-[9px] px-1.5 py-0.5 bg-accent text-muted-foreground hover:text-primary hover:bg-primary/10 rounded-sm transition-colors font-semibold flex items-center gap-1"
                  >
                    <MapPin size={8} />
                    Circuit
                  </Link>
                )}
              </div>
              <span className="text-[10px] text-muted-foreground">{r.date}</span>
            </div>
            <div className="flex items-center gap-4 mt-1.5">
              <div className="flex items-center gap-2">
                <div className="w-1 h-4 rounded-sm" style={{ backgroundColor: r.teamColor }} />
                <Flag size={12} className="text-primary" />
                <span className="text-xs font-medium">{r.winner}</span>
                <span className="text-[10px] text-muted-foreground">({r.winnerTeam})</span>
              </div>
              <div className="flex items-center gap-1 ml-auto">
                <Clock size={10} className="text-fastest" />
                <span className="text-[10px] data-cell text-fastest">{r.fastestLap}</span>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
