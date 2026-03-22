import { useState, useEffect } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";
import { cn } from "@/lib/utils";
import { useSessionTelemetry, useAvailableLaps } from "@/hooks/useTelemetry";
import type { DriverTelemetry } from "@/types/session";
import { X, Plus } from "lucide-react";

type TraceType = "speed" | "throttle" | "brake" | "rpm" | "gear";

const traceConfig: Record<TraceType, { label: string; unit: string; domain: [number, number] }> = {
  speed: { label: "Speed", unit: "km/h", domain: [0, 370] },
  throttle: { label: "Throttle", unit: "%", domain: [0, 100] },
  brake: { label: "Brake", unit: "%", domain: [0, 100] },
  rpm: { label: "RPM", unit: "rpm", domain: [5000, 13000] },
  gear: { label: "Gear", unit: "", domain: [0, 9] },
};

const allTraces: TraceType[] = ["speed", "throttle", "brake", "rpm", "gear"];

export default function TelemetryPanel({ sessionId }: { sessionId: number | null }) {
  const [selectedAbbrevs, setSelectedAbbrevs] = useState<string[]>(["VER", "NOR"]);
  const [activeTraces, setActiveTraces] = useState<TraceType[]>(["speed", "throttle", "brake"]);
  const [showPicker, setShowPicker] = useState(false);
  const [selectedLap, setSelectedLap] = useState<number | null>(null);

  const { data: availableLaps = [] } = useAvailableLaps(sessionId, selectedAbbrevs[0] ?? null);
  const { data: telemetryData = [], isLoading } = useSessionTelemetry(
    sessionId,
    selectedAbbrevs,
    selectedLap
  );

  // Auto-select fastest lap when available
  useEffect(() => {
    if (availableLaps.length > 0 && selectedLap === null) {
      // Pick a lap near the middle of the race (often representative)
      const mid = Math.floor(availableLaps.length / 2);
      setSelectedLap(availableLaps[mid]);
    }
  }, [availableLaps, selectedLap]);

  const selectedDrivers: DriverTelemetry[] = telemetryData;
  // Build available drivers list from leaderboard-like info (we only know selected)
  const availableDriverCodes = ["VER", "NOR", "LEC", "PIA", "SAI", "HAM", "RUS", "PER", "ALO", "STR"]
    .filter((d) => !selectedAbbrevs.includes(d));

  const addDriver = (abbrev: string) => {
    setSelectedAbbrevs((prev) => [...prev, abbrev]);
    setShowPicker(false);
  };

  const removeDriver = (abbrev: string) => {
    if (selectedAbbrevs.length <= 1) return;
    setSelectedAbbrevs((prev) => prev.filter((a) => a !== abbrev));
  };

  const toggleTrace = (t: TraceType) => {
    setActiveTraces((prev) =>
      prev.includes(t) ? prev.filter((x) => x !== t) : [...prev, t]
    );
  };

  // Merge telemetry data by distance
  const mergedData = selectedDrivers[0]?.data.map((point, i) => {
    const row: Record<string, number> = { distance: point.distance };
    selectedDrivers.forEach((d) => {
      const dp = d.data[i];
      if (dp) {
        allTraces.forEach((t) => {
          row[`${d.abbreviation}_${t}`] = dp[t];
        });
      }
    });
    return row;
  }) ?? [];

  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-3">
          <h3 className="font-display text-sm font-bold uppercase tracking-wider">Telemetry</h3>

          {/* Driver chips */}
          <div className="flex items-center gap-1.5 relative">
            {selectedDrivers.map((d) => (
              <button
                key={d.abbreviation}
                onClick={() => removeDriver(d.abbreviation)}
                className="group flex items-center gap-1.5 pl-2 pr-1.5 py-1 rounded-sm border border-border bg-secondary/50 hover:bg-secondary transition-colors"
              >
                <span className="w-2 h-3 rounded-sm" style={{ backgroundColor: d.teamColor }} />
                <span className="text-xs font-display font-bold">{d.abbreviation}</span>
                {selectedAbbrevs.length > 1 && (
                  <X size={10} className="text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity" />
                )}
              </button>
            ))}
            {/* Show chips for selected drivers not yet loaded */}
            {selectedAbbrevs.filter((a) => !selectedDrivers.find((d) => d.abbreviation === a)).map((a) => (
              <span key={a} className="flex items-center gap-1.5 pl-2 pr-1.5 py-1 rounded-sm border border-border bg-secondary/50 opacity-50">
                <span className="text-xs font-display font-bold">{a}</span>
              </span>
            ))}

            {availableDriverCodes.length > 0 && (
              <div className="relative">
                <button
                  onClick={() => setShowPicker(!showPicker)}
                  className="flex items-center justify-center w-7 h-7 rounded-sm border border-dashed border-border text-muted-foreground hover:text-foreground hover:border-primary/50 transition-colors"
                >
                  <Plus size={12} />
                </button>

                {showPicker && (
                  <>
                    <div className="fixed inset-0 z-20" onClick={() => setShowPicker(false)} />
                    <div className="absolute top-full left-0 mt-1 z-30 bg-card border border-border rounded-sm shadow-lg p-1 min-w-[140px]">
                      <div className="px-2 py-1 text-[10px] uppercase tracking-widest text-muted-foreground font-semibold">
                        Add driver
                      </div>
                      {availableDriverCodes.map((code) => (
                        <button
                          key={code}
                          onClick={() => addDriver(code)}
                          className="w-full flex items-center gap-2 px-2 py-1.5 text-xs rounded-sm hover:bg-secondary transition-colors"
                        >
                          <span className="font-display font-bold">{code}</span>
                        </button>
                      ))}
                    </div>
                  </>
                )}
              </div>
            )}
          </div>

          {/* Lap selector */}
          {availableLaps.length > 0 && (
            <select
              value={selectedLap ?? ""}
              onChange={(e) => setSelectedLap(Number(e.target.value))}
              className="text-xs bg-secondary border border-border rounded-sm px-2 py-1 font-display"
            >
              {availableLaps.map((lap) => (
                <option key={lap} value={lap}>Lap {lap}</option>
              ))}
            </select>
          )}
        </div>

        <div className="flex items-center gap-1">
          {allTraces.map((t) => (
            <button
              key={t}
              onClick={() => toggleTrace(t)}
              className={cn(
                "px-2 py-1 text-[10px] font-display font-semibold uppercase tracking-wider rounded-sm transition-colors",
                activeTraces.includes(t)
                  ? "bg-primary/15 text-primary"
                  : "bg-secondary text-muted-foreground hover:text-foreground"
              )}
            >
              {traceConfig[t].label}
            </button>
          ))}
        </div>
      </div>

      <div className="p-4 space-y-1">
        {isLoading && (
          <div className="text-center text-muted-foreground text-sm py-8">Loading telemetry...</div>
        )}
        {!isLoading && mergedData.length === 0 && (
          <div className="text-center text-muted-foreground text-sm py-8">
            Select a session and lap to view telemetry
          </div>
        )}
        {mergedData.length > 0 && activeTraces.map((trace) => (
          <div key={trace} className="h-32">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-[10px] font-display font-semibold uppercase tracking-widest text-muted-foreground">
                {traceConfig[trace].label}
              </span>
              <span className="text-[9px] text-muted-foreground/60">{traceConfig[trace].unit}</span>
            </div>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={mergedData} margin={{ top: 2, right: 8, bottom: 2, left: 0 }}>
                <CartesianGrid
                  strokeDasharray="3 3"
                  stroke="hsl(220 10% 15%)"
                  vertical={false}
                />
                <XAxis
                  dataKey="distance"
                  tick={{ fontSize: 9, fill: "hsl(218 11% 45%)" }}
                  tickFormatter={(v) => `${Number(v).toFixed(1)}km`}
                  axisLine={{ stroke: "hsl(220 10% 18%)" }}
                  tickLine={false}
                  interval={20}
                />
                <YAxis
                  domain={traceConfig[trace].domain}
                  tick={{ fontSize: 9, fill: "hsl(218 11% 45%)" }}
                  axisLine={false}
                  tickLine={false}
                  width={40}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: "hsl(220 12% 11%)",
                    border: "1px solid hsl(220 10% 20%)",
                    borderRadius: "2px",
                    fontSize: "11px",
                    fontFamily: "Inter",
                  }}
                  labelFormatter={(v) => `${Number(v).toFixed(2)} km`}
                />
                {selectedDrivers.map((d) => (
                  <Line
                    key={d.abbreviation}
                    type="monotone"
                    dataKey={`${d.abbreviation}_${trace}`}
                    name={d.abbreviation}
                    stroke={d.teamColor}
                    strokeWidth={1.5}
                    dot={false}
                    activeDot={{ r: 3, strokeWidth: 0 }}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        ))}
      </div>
    </div>
  );
}
