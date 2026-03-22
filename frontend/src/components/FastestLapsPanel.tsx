import { cn } from "@/lib/utils";
import { FastestLapEntry } from "@/types/analysis";
import { Timer } from "lucide-react";

interface FastestLapsPanelProps {
  data: FastestLapEntry[];
}

export default function FastestLapsPanel({ data }: FastestLapsPanelProps) {
  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center gap-2">
        <Timer size={14} className="text-fastest" />
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Fastest Laps</h3>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border text-muted-foreground">
              <th className="text-left px-4 py-2 font-medium w-8">#</th>
              <th className="text-left px-2 py-2 font-medium">Driver</th>
              <th className="text-right px-2 py-2 font-medium">Lap</th>
              <th className="text-right px-2 py-2 font-medium">S1</th>
              <th className="text-right px-2 py-2 font-medium">S2</th>
              <th className="text-right px-2 py-2 font-medium">S3</th>
              <th className="text-right px-4 py-2 font-medium">Time</th>
              <th className="text-right px-4 py-2 font-medium">Gap</th>
            </tr>
          </thead>
          <tbody>
            {data.map((d) => (
              <tr key={d.abbreviation} className="border-b border-border/50 hover:bg-secondary/50 transition-colors">
                <td className="px-4 py-2 font-display font-bold text-muted-foreground">{d.position}</td>
                <td className="px-2 py-2">
                  <div className="flex items-center gap-2">
                    <div className="w-1 h-4 rounded-sm" style={{ backgroundColor: d.teamColor }} />
                    <span className="font-display font-bold">{d.abbreviation}</span>
                    <span className="text-muted-foreground hidden lg:inline">{d.team}</span>
                  </div>
                </td>
                <td className="px-2 py-2 text-right data-cell text-muted-foreground">{d.lapNumber}</td>
                <td className="px-2 py-2 text-right data-cell">{d.sector1}</td>
                <td className="px-2 py-2 text-right data-cell">{d.sector2}</td>
                <td className="px-2 py-2 text-right data-cell">{d.sector3}</td>
                <td className={cn("px-4 py-2 text-right data-cell font-bold", d.position === 1 && "sector-fastest")}>
                  {d.lapTime}
                </td>
                <td className="px-4 py-2 text-right data-cell text-muted-foreground">{d.gap}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
