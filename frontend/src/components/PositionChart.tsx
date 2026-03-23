import { memo, useMemo } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";
import { cn } from "@/lib/utils";
import type { PositionData } from "@/types/analysis";
import { driverColors } from "@/data/raceAnalysis";

interface PositionChartProps {
  data: PositionData[];
}

export default memo(function PositionChart({ data }: PositionChartProps) {
  // Derive drivers from the data instead of hardcoding
  const drivers = useMemo(() => {
    if (data.length === 0) return [];
    return Object.keys(data[0]).filter((k) => k !== "lap");
  }, [data]);

  const dashed = useMemo(() => {
    // Dash lines for the second half of drivers
    const half = Math.ceil(drivers.length / 2);
    return new Set(drivers.slice(half));
  }, [drivers]);
  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Position Changes</h3>
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
          {drivers.map((d) => (
            <span key={d} className="flex items-center gap-1 text-[10px]">
              <span className="w-3 h-[2px] rounded-sm" style={{ backgroundColor: driverColors[d] }} />
              <span className="font-display font-semibold text-muted-foreground">{d}</span>
            </span>
          ))}
        </div>
      </div>
      <div className="p-4 h-[280px]">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(220 10% 15%)" vertical={false} />
            <XAxis
              dataKey="lap"
              tick={{ fontSize: 9, fill: "hsl(218 11% 45%)" }}
              tickFormatter={(v) => `L${v}`}
              axisLine={{ stroke: "hsl(220 10% 18%)" }}
              tickLine={false}
              interval={9}
            />
            <YAxis
              reversed
              domain={[1, Math.max(drivers.length, 2)]}
              tick={{ fontSize: 9, fill: "hsl(218 11% 45%)" }}
              axisLine={false}
              tickLine={false}
              width={20}
              tickFormatter={(v) => `P${v}`}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "hsl(220 12% 11%)",
                border: "1px solid hsl(220 10% 20%)",
                borderRadius: "2px",
                fontSize: "11px",
                fontFamily: "Inter",
              }}
              labelFormatter={(v) => `Lap ${v}`}
              formatter={(value: number, name: string) => [`P${value}`, name]}
            />
            {drivers.map((d) => (
              <Line
                key={d}
                type="stepAfter"
                dataKey={d}
                stroke={driverColors[d]}
                strokeWidth={dashed.has(d) ? 1.5 : 2}
                strokeDasharray={dashed.has(d) ? "4 3" : undefined}
                dot={false}
                activeDot={{ r: 3, strokeWidth: 0 }}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});
