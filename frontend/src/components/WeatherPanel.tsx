import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";
import { WeatherData } from "@/types/session";
import { Cloud, Thermometer, Droplets, Wind } from "lucide-react";

interface WeatherPanelProps {
  data: WeatherData[];
}

export default function WeatherPanel({ data }: WeatherPanelProps) {
  const latest = data[data.length - 1];

  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center gap-2">
        <Cloud size={14} className="text-muted-foreground" />
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Weather</h3>
        <span className="text-[10px] text-personal-best ml-auto font-display font-semibold uppercase">
          {latest?.rainfall ? "WET" : "DRY"}
        </span>
      </div>

      {/* Current conditions */}
      <div className="grid grid-cols-4 gap-px bg-border/50">
        {[
          { icon: <Thermometer size={12} />, label: "Air", value: `${latest?.airTemp.toFixed(1)}°C` },
          { icon: <Thermometer size={12} />, label: "Track", value: `${latest?.trackTemp.toFixed(1)}°C` },
          { icon: <Droplets size={12} />, label: "Humidity", value: `${latest?.humidity.toFixed(0)}%` },
          { icon: <Wind size={12} />, label: "Wind", value: `${latest?.windSpeed.toFixed(1)} km/h` },
        ].map((item) => (
          <div key={item.label} className="bg-card p-3 text-center">
            <div className="flex items-center justify-center gap-1 text-muted-foreground mb-1">
              {item.icon}
              <span className="text-[9px] uppercase tracking-widest">{item.label}</span>
            </div>
            <span className="font-display text-sm font-bold">{item.value}</span>
          </div>
        ))}
      </div>

      {/* Temperature chart */}
      <div className="p-4 h-[140px]">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 4, right: 8, bottom: 4, left: 0 }}>
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
              domain={['auto', 'auto']}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "hsl(220 12% 11%)",
                border: "1px solid hsl(220 10% 20%)",
                borderRadius: "2px",
                fontSize: "11px",
              }}
              labelFormatter={(v) => `Lap ${v}`}
            />
            <Line type="monotone" dataKey="airTemp" name="Air °C" stroke="hsl(210 80% 60%)" strokeWidth={1.5} dot={false} />
            <Line type="monotone" dataKey="trackTemp" name="Track °C" stroke="hsl(25 90% 55%)" strokeWidth={1.5} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
