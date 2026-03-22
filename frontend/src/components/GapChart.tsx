import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";
import type { GapData } from "@/types/analysis";
import { driverColors } from "@/data/raceAnalysis";

const drivers = ["NOR", "LEC", "PIA", "SAI", "HAM", "RUS", "PER"];
const dashed = new Set(["PIA", "RUS", "PER", "SAI"]);

interface GapChartProps {
  data: GapData[];
}

export default function GapChart({ data }: GapChartProps) {
  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Gap to Leader</h3>
        <span className="text-[10px] text-muted-foreground">Seconds behind VER</span>
      </div>
      <div className="p-4 h-[260px]">
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
              tick={{ fontSize: 9, fill: "hsl(218 11% 45%)" }}
              axisLine={false}
              tickLine={false}
              width={30}
              tickFormatter={(v) => `${v}s`}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "hsl(220 12% 11%)",
                border: "1px solid hsl(220 10% 20%)",
                borderRadius: "2px",
                fontSize: "11px",
              }}
              labelFormatter={(v) => `Lap ${v}`}
              formatter={(value: number, name: string) => [`+${value.toFixed(1)}s`, name]}
            />
            {drivers.map((d) => (
              <Line
                key={d}
                type="monotone"
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
}
