import { cn } from "@/lib/utils";
import { TyreStint, PitStop } from "@/data/sessionData";
import { ArrowDown } from "lucide-react";

const tyreColorMap: Record<string, string> = {
  SOFT: "bg-tyre-soft",
  MEDIUM: "bg-tyre-medium",
  HARD: "bg-tyre-hard",
  INTERMEDIATE: "bg-tyre-intermediate",
  WET: "bg-tyre-wet",
};

const tyreBorderMap: Record<string, string> = {
  SOFT: "border-tyre-soft",
  MEDIUM: "border-tyre-medium",
  HARD: "border-tyre-hard",
  INTERMEDIATE: "border-tyre-intermediate",
  WET: "border-tyre-wet",
};

interface TyreStrategyPanelProps {
  stints: TyreStint[];
  pitStops: PitStop[];
}

export default function TyreStrategyPanel({ stints, pitStops }: TyreStrategyPanelProps) {
  const maxLaps = Math.max(...stints.map((s) => s.totalLaps));

  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Tyre Strategy</h3>
        <div className="flex items-center gap-3 text-[10px]">
          {["SOFT", "MEDIUM", "HARD"].map((t) => (
            <span key={t} className="flex items-center gap-1">
              <span className={cn("w-2.5 h-2.5 rounded-full", tyreColorMap[t])} />
              <span className="text-muted-foreground">{t[0] + t.slice(1).toLowerCase()}</span>
            </span>
          ))}
        </div>
      </div>
      <div className="p-4 space-y-2">
        {stints.map((driver) => (
          <div key={driver.abbreviation} className="flex items-center gap-2">
            <div className="w-8 flex items-center gap-1 shrink-0">
              <div className="w-1 h-3 rounded-sm" style={{ backgroundColor: driver.teamColor }} />
              <span className="font-display text-[11px] font-bold">{driver.abbreviation}</span>
            </div>
            <div className="flex-1 flex h-5 rounded-sm overflow-hidden gap-px">
              {driver.stints.map((stint, i) => (
                <div
                  key={i}
                  className={cn(
                    "h-full flex items-center justify-center text-[8px] font-display font-bold border-t-2",
                    tyreBorderMap[stint.compound]
                  )}
                  style={{
                    width: `${(stint.laps / maxLaps) * 100}%`,
                    backgroundColor: "hsl(220 12% 14%)",
                  }}
                  title={`${stint.compound} — Laps ${stint.startLap}-${stint.endLap} (${stint.laps} laps)`}
                >
                  <span className="text-muted-foreground">{stint.laps}</span>
                </div>
              ))}
            </div>
            <span className="text-[10px] data-cell text-muted-foreground w-6 text-right">{driver.totalLaps}</span>
          </div>
        ))}
      </div>

      {/* Pit stops list */}
      <div className="px-4 py-3 border-t border-border">
        <div className="flex items-center gap-2 mb-2">
          <ArrowDown size={12} className="text-muted-foreground" />
          <span className="text-[10px] uppercase tracking-widest text-muted-foreground font-semibold">Pit Stops</span>
        </div>
        <div className="grid grid-cols-2 lg:grid-cols-3 gap-2">
          {pitStops.map((ps, i) => (
            <div key={i} className="flex items-center gap-2 text-[11px]">
              <span className="data-cell text-muted-foreground w-6">L{ps.lap}</span>
              <div className="w-1 h-3 rounded-sm" style={{ backgroundColor: ps.teamColor }} />
              <span className="font-display font-bold">{ps.abbreviation}</span>
              <div className="flex items-center gap-0.5 ml-auto">
                <span className={cn("w-2 h-2 rounded-full", tyreColorMap[ps.tyreFrom])} />
                <span className="text-muted-foreground">→</span>
                <span className={cn("w-2 h-2 rounded-full", tyreColorMap[ps.tyreTo])} />
                <span className="data-cell text-muted-foreground ml-1">{ps.duration.toFixed(1)}s</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
