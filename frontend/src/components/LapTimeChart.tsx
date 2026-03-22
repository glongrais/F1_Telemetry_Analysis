import { useState } from "react";
import {
  ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ZAxis,
} from "recharts";
import { cn } from "@/lib/utils";
import type { LapTimeData } from "@/types/analysis";
import { driverColors } from "@/data/raceAnalysis";

const allDrivers = ["VER", "NOR", "LEC", "PIA", "SAI", "HAM", "RUS", "PER"];

interface LapTimeChartProps {
  data: LapTimeData[];
}

function formatTime(seconds: number): string {
  const mins = Math.floor(seconds / 60);
  const secs = (seconds % 60).toFixed(3);
  return `${mins}:${secs.padStart(6, "0")}`;
}

export default function LapTimeChart({ data }: LapTimeChartProps) {
  const [activeDrivers, setActiveDrivers] = useState<string[]>(["VER", "NOR", "LEC"]);

  const toggleDriver = (d: string) => {
    setActiveDrivers((prev) =>
      prev.includes(d) ? prev.filter((x) => x !== d) : [...prev, d]
    );
  };

  // Filter out pit stop laps (>100s) for cleaner display
  const scatterData = activeDrivers.flatMap((driver) =>
    data
      .filter((row) => row[driver] < 100)
      .map((row) => ({
        lap: row.lap,
        time: row[driver],
        driver,
      }))
  );

  // Group by driver for coloring
  const grouped = activeDrivers.map((driver) => ({
    driver,
    data: scatterData.filter((d) => d.driver === driver),
  }));

  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between flex-wrap gap-2">
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Lap Times</h3>
        <div className="flex items-center gap-1">
          {allDrivers.map((d) => (
            <button
              key={d}
              onClick={() => toggleDriver(d)}
              className={cn(
                "px-1.5 py-0.5 text-[10px] font-display font-bold rounded-sm transition-colors border",
                activeDrivers.includes(d)
                  ? "border-transparent"
                  : "border-border text-muted-foreground/50 hover:text-muted-foreground"
              )}
              style={activeDrivers.includes(d) ? {
                backgroundColor: driverColors[d] + "25",
                color: driverColors[d],
              } : undefined}
            >
              {d}
            </button>
          ))}
        </div>
      </div>
      <div className="p-4 h-[280px]">
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(220 10% 15%)" />
            <XAxis
              dataKey="lap"
              type="number"
              domain={[1, 52]}
              tick={{ fontSize: 9, fill: "hsl(218 11% 45%)" }}
              tickFormatter={(v) => `L${v}`}
              axisLine={{ stroke: "hsl(220 10% 18%)" }}
              tickLine={false}
            />
            <YAxis
              dataKey="time"
              type="number"
              domain={["auto", "auto"]}
              tick={{ fontSize: 9, fill: "hsl(218 11% 45%)" }}
              axisLine={false}
              tickLine={false}
              width={46}
              tickFormatter={(v) => formatTime(v)}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "hsl(220 12% 11%)",
                border: "1px solid hsl(220 10% 20%)",
                borderRadius: "2px",
                fontSize: "11px",
              }}
              formatter={(value: number) => [formatTime(value), "Lap Time"]}
              labelFormatter={(v) => `Lap ${v}`}
            />
            {grouped.map((g) => (
              <Scatter
                key={g.driver}
                name={g.driver}
                data={g.data}
                fill={driverColors[g.driver]}
                r={2}
              />
            ))}
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
