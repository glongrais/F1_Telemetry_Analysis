import { cn } from "@/lib/utils";
import { SpeedTrapEntry } from "@/data/raceAnalysis";
import { Gauge } from "lucide-react";
import TeamLogo from "@/components/TeamLogo";

interface SpeedTrapPanelProps {
  data: SpeedTrapEntry[];
}

export default function SpeedTrapPanel({ data }: SpeedTrapPanelProps) {
  const maxSpeed = Math.max(...data.map((d) => d.topSpeed));
  const sorted = [...data].sort((a, b) => b.topSpeed - a.topSpeed);

  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center gap-2">
        <Gauge size={14} className="text-muted-foreground" />
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Speed Traps</h3>
        <span className="text-[10px] text-muted-foreground ml-auto">km/h</span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border text-muted-foreground">
              <th className="text-left px-4 py-2 font-medium">Driver</th>
              <th className="text-right px-2 py-2 font-medium">ST1</th>
              <th className="text-right px-2 py-2 font-medium">ST2</th>
              <th className="text-right px-2 py-2 font-medium">ST3</th>
              <th className="text-right px-2 py-2 font-medium">ST4</th>
              <th className="text-right px-4 py-2 font-medium">Top</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((d, i) => {
              const isMax = d.topSpeed === maxSpeed;
              return (
                <tr key={d.abbreviation} className="border-b border-border/50 hover:bg-secondary/50 transition-colors">
                  <td className="px-4 py-2">
                    <div className="flex items-center gap-2">
                      <div className="w-1 h-4 rounded-sm" style={{ backgroundColor: d.teamColor }} />
                      <span className="font-display font-bold">{d.abbreviation}</span>
                    </div>
                  </td>
                  <td className="px-2 py-2 text-right data-cell">{d.speedTrap1}</td>
                  <td className="px-2 py-2 text-right data-cell">{d.speedTrap2}</td>
                  <td className="px-2 py-2 text-right data-cell">{d.speedTrap3}</td>
                  <td className="px-2 py-2 text-right data-cell">{d.speedTrap4}</td>
                  <td className={cn("px-4 py-2 text-right data-cell font-bold", isMax && "sector-fastest")}>
                    {d.topSpeed}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
