import { useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";
import { cn } from "@/lib/utils";
import { useDriverStandingsEvolution, useConstructorStandingsEvolution } from "@/hooks/useStandings";
import { getTeamLogo } from "@/lib/teamLogos";

type Mode = "drivers" | "constructors";

/** Renders a label at the end of each line */
function EndLabel({ viewBox, value, color, isConstructor }: any) {
  const { x, y } = viewBox;
  if (x == null || y == null) return null;

  if (isConstructor) {
    const logo = getTeamLogo(value);
    if (logo) {
      return (
        <image
          href={logo}
          x={x + 4}
          y={y - 8}
          width={16}
          height={16}
        />
      );
    }
  }

  return (
    <text
      x={x + 6}
      y={y}
      dy={4}
      fill={color}
      fontSize={9}
      fontWeight={700}
      fontFamily="Formula1, sans-serif"
    >
      {value}
    </text>
  );
}

export default function StandingsEvolution({ year }: { year: number }) {
  const [mode, setMode] = useState<Mode>("drivers");

  const { data: driverEvo } = useDriverStandingsEvolution(year);
  const { data: constructorEvo } = useConstructorStandingsEvolution(year);

  const isDrivers = mode === "drivers";
  const evoData = isDrivers ? driverEvo : constructorEvo;
  const data = evoData?.data ?? [];
  const colors = evoData?.colors ?? {};
  const keys = Object.keys(colors);

  const dashed = isDrivers ? new Set(["PIA", "RUS", "PER", "SAI"]) : new Set<string>();

  if (data.length === 0) {
    return (
      <div className="bg-card rounded-sm border border-border overflow-hidden p-8 text-center text-muted-foreground text-sm">
        Loading standings evolution...
      </div>
    );
  }

  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">
          Championship Evolution
        </h3>
        <div className="flex items-center gap-1">
          {(["drivers", "constructors"] as Mode[]).map((m) => (
            <button
              key={m}
              onClick={() => setMode(m)}
              className={cn(
                "px-2.5 py-1 text-[10px] font-display font-semibold uppercase tracking-wider rounded-sm transition-colors",
                mode === m
                  ? "bg-primary/15 text-primary"
                  : "bg-secondary text-muted-foreground hover:text-foreground"
              )}
            >
              {m}
            </button>
          ))}
        </div>
      </div>

      <div className="p-4">
        <div className="h-[320px]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 8, right: isDrivers ? 40 : 28, bottom: 4, left: 0 }}>
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="hsl(var(--chart-grid))"
                vertical={false}
              />
              <XAxis
                dataKey="raceName"
                tick={{ fontSize: 10, fill: "hsl(var(--chart-tick))", fontFamily: "Formula1, sans-serif" }}
                axisLine={{ stroke: "hsl(var(--chart-axis))" }}
                tickLine={false}
              />
              <YAxis
                tick={{ fontSize: 10, fill: "hsl(var(--chart-tick))" }}
                axisLine={false}
                tickLine={false}
                width={36}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: "hsl(var(--chart-tooltip-bg))",
                  border: "1px solid hsl(var(--chart-tooltip-border))",
                  borderRadius: "2px",
                  fontSize: "11px",
                  fontFamily: "Inter",
                  color: "hsl(var(--foreground))",
                }}
                labelStyle={{ fontFamily: "Titillium Web", fontWeight: 700, marginBottom: 4 }}
              />
              {keys.map((key) => (
                <Line
                  key={key}
                  type="monotone"
                  dataKey={key}
                  stroke={colors[key]}
                  strokeWidth={2}
                  strokeDasharray={dashed.has(key) ? "4 3" : undefined}
                  dot={{ r: 2.5, strokeWidth: 0, fill: colors[key] }}
                  activeDot={{ r: 4, strokeWidth: 0 }}
                  label={false}
                >
                </Line>
              ))}
              {/* Render end-of-line labels manually */}
              {keys.map((key) => (
                <Line
                  key={`label-${key}`}
                  dataKey={key}
                  stroke="transparent"
                  dot={false}
                  activeDot={false}
                  isAnimationActive={false}
                  label={({ x, y, index }: any) => {
                    if (index !== data.length - 1) return null;
                    return (
                      <EndLabel
                        viewBox={{ x, y }}
                        value={key}
                        color={colors[key]}
                        isConstructor={!isDrivers}
                      />
                    );
                  }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Legend */}
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 mt-3 px-1">
          {keys.map((key) => (
            <span key={key} className="flex items-center gap-1.5 text-[11px]">
              <span
                className="inline-block w-4 h-[2px] rounded-sm"
                style={{
                  backgroundColor: colors[key],
                  borderStyle: dashed.has(key) ? "dashed" : "solid",
                }}
              />
              <span className="font-display font-semibold text-muted-foreground">{key}</span>
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}
