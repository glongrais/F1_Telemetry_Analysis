import { useParams, Link } from "react-router-dom";
import {
  ArrowLeft, MapPin, Gauge, RotateCw, Timer, Ruler, Mountain,
  Trophy, Calendar, Zap, ArrowUpRight, Flag,
} from "lucide-react";
import { getCircuitById, circuits } from "@/data/circuitData";
import { countryFlag } from "@/lib/countryFlag";
import TrackMap from "@/components/TrackMap";

export default function TrackProfile() {
  const { id } = useParams<{ id: string }>();
  const circuit = getCircuitById(id ?? "");

  if (!circuit) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <h1 className="font-display text-2xl font-bold text-foreground mb-2">Circuit Not Found</h1>
          <Link to="/" className="text-primary text-sm hover:underline">← Back to Dashboard</Link>
        </div>
      </div>
    );
  }

  const typeColors: Record<string, string> = {
    street: "hsl(var(--destructive))",
    permanent: "hsl(var(--primary))",
    "semi-permanent": "hsl(45 100% 50%)",
  };

  return (
    <div className="min-h-screen bg-background">
      {/* Hero */}
      <div className="relative border-b border-border overflow-hidden">
        <div
          className="absolute inset-0 opacity-[0.06]"
          style={{
            background: `radial-gradient(ellipse at 70% 50%, ${typeColors[circuit.type]}, transparent 70%)`,
          }}
        />
        <div className="relative px-4 md:px-8 py-6 md:py-10 max-w-6xl mx-auto">
          <Link
            to="/"
            className="inline-flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground mb-4 transition-colors"
          >
            <ArrowLeft size={14} />
            Back to Dashboard
          </Link>

          <div className="flex flex-col md:flex-row md:items-end gap-4 md:gap-8">
            <div className="flex-1">
              <div className="flex items-center gap-3 mb-2">
                <span className="text-2xl">{countryFlag(circuit.countryCode)}</span>
                <span
                  className="text-[10px] font-display font-bold uppercase tracking-widest px-2 py-0.5 rounded-sm"
                  style={{ backgroundColor: typeColors[circuit.type], color: "#000" }}
                >
                  {circuit.type}
                </span>
              </div>
              <h1 className="font-display text-3xl md:text-4xl font-black text-foreground leading-tight">
                {circuit.name}
              </h1>
              <div className="flex items-center gap-2 mt-2 text-muted-foreground">
                <MapPin size={14} />
                <span className="text-sm">{circuit.location}, {circuit.country}</span>
              </div>
            </div>

            <div className="flex items-center gap-6">
              <HeroStat label="Length" value={`${circuit.lengthKm} km`} icon={<Ruler size={14} />} />
              <HeroStat label="Turns" value={circuit.turns.toString()} icon={<RotateCw size={14} />} />
              <HeroStat label="DRS Zones" value={circuit.drsZones.toString()} icon={<Zap size={14} />} />
              <HeroStat label="First GP" value={circuit.firstGP.toString()} icon={<Calendar size={14} />} />
            </div>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="px-4 md:px-8 py-6 max-w-6xl mx-auto space-y-6">
        {/* Stats grid */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <InfoCard label="Lap Record" value={circuit.lapRecord} sub={`${circuit.lapRecordHolder} (${circuit.lapRecordYear})`} icon={<Timer size={16} />} />
          <InfoCard label="Top Speed" value={`${circuit.topSpeedKmh} km/h`} sub="Fastest recorded" icon={<Gauge size={16} />} />
          <InfoCard label="Avg Speed" value={`${circuit.avgSpeedKmh} km/h`} sub="Average lap speed" icon={<ArrowUpRight size={16} />} />
          <InfoCard label="Altitude" value={`${circuit.altitude}m`} sub={`${circuit.direction} direction`} icon={<Mountain size={16} />} />
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          {/* Track map */}
          <div className="lg:col-span-2">
            <TrackMap selectedDrivers={["VER", "NOR", "LEC", "HAM"]} />
          </div>

          {/* Race info */}
          <div className="space-y-4">
            {/* Race distance */}
            <div className="bg-card rounded-sm border border-border overflow-hidden">
              <div className="px-4 py-3 border-b border-border">
                <h3 className="font-display text-sm font-bold uppercase tracking-wider">Race Distance</h3>
              </div>
              <div className="p-4 space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Laps</span>
                  <span className="font-display text-sm font-bold">{circuit.raceDistance}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Distance</span>
                  <span className="font-display text-sm font-bold">{circuit.raceDistanceKm} km</span>
                </div>
                <div className="h-px bg-border" />
                <h4 className="text-[10px] uppercase tracking-widest text-muted-foreground font-semibold">Sector Lengths</h4>
                <div className="flex gap-1">
                  {circuit.sectorLengths.map((len, i) => {
                    const total = circuit.sectorLengths.reduce((a, b) => a + b, 0);
                    const sectorColors = ["hsl(var(--primary))", "hsl(45 100% 50%)", "hsl(210 80% 55%)"];
                    return (
                      <div key={i} className="flex-1">
                        <div
                          className="h-2 rounded-sm mb-1"
                          style={{
                            backgroundColor: sectorColors[i],
                            width: `${(len / total) * 100}%`,
                            minWidth: "100%",
                            opacity: 0.7,
                          }}
                        />
                        <div className="flex justify-between">
                          <span className="text-[9px] font-bold text-muted-foreground">S{i + 1}</span>
                          <span className="text-[9px] font-display font-bold">{len} km</span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>

            {/* Circuit characteristics */}
            <div className="bg-card rounded-sm border border-border overflow-hidden">
              <div className="px-4 py-3 border-b border-border">
                <h3 className="font-display text-sm font-bold uppercase tracking-wider">Characteristics</h3>
              </div>
              <div className="p-4 space-y-2">
                {[
                  ["Type", circuit.type.charAt(0).toUpperCase() + circuit.type.slice(1)],
                  ["Direction", circuit.direction.charAt(0).toUpperCase() + circuit.direction.slice(1)],
                  ["Altitude", `${circuit.altitude}m above sea level`],
                  ["DRS Zones", circuit.drsZones.toString()],
                ].map(([label, val]) => (
                  <div key={label} className="flex items-center justify-between py-1">
                    <span className="text-xs text-muted-foreground">{label}</span>
                    <span className="text-xs font-display font-bold">{val}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Previous winners */}
        <div className="bg-card rounded-sm border border-border overflow-hidden">
          <div className="px-4 py-3 border-b border-border flex items-center gap-2">
            <Trophy size={14} className="text-primary" />
            <h3 className="font-display text-sm font-bold uppercase tracking-wider">Previous Winners</h3>
          </div>
          <div className="divide-y divide-border/50">
            {circuit.previousWinners.map((w) => (
              <div key={w.year} className="px-4 py-3 flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <span className="font-display text-xs font-bold text-muted-foreground w-10">{w.year}</span>
                  <Flag size={12} style={{ color: w.teamColor }} />
                  <span className="text-sm font-medium">{w.driver}</span>
                </div>
                <span className="text-xs text-muted-foreground">{w.team}</span>
              </div>
            ))}
          </div>
        </div>

        {/* All circuits navigation */}
        <div className="bg-card rounded-sm border border-border overflow-hidden">
          <div className="px-4 py-3 border-b border-border">
            <h3 className="font-display text-sm font-bold uppercase tracking-wider">All Circuits</h3>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-px bg-border">
            {circuits.map((c) => (
              <Link
                key={c.id}
                to={`/track/${c.id}`}
                className={`bg-card px-4 py-3 hover:bg-accent transition-colors ${c.id === circuit.id ? "ring-1 ring-primary ring-inset" : ""}`}
              >
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-sm">{countryFlag(c.countryCode)}</span>
                  <span className="font-display text-[10px] font-bold uppercase tracking-wider truncate">{c.location}</span>
                </div>
                <span className="text-[9px] text-muted-foreground">{c.lengthKm} km · {c.turns} turns</span>
              </Link>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function HeroStat({ label, value, icon }: { label: string; value: string; icon: React.ReactNode }) {
  return (
    <div className="text-center">
      <div className="text-muted-foreground mb-1">{icon}</div>
      <span className="font-display text-xl md:text-2xl font-black block text-foreground">{value}</span>
      <span className="text-[9px] uppercase tracking-widest text-muted-foreground">{label}</span>
    </div>
  );
}

function InfoCard({ label, value, sub, icon }: { label: string; value: string; sub: string; icon: React.ReactNode }) {
  return (
    <div className="bg-card rounded-sm border border-border p-4">
      <div className="flex items-center justify-between mb-3">
        <span className="text-[10px] uppercase tracking-widest text-muted-foreground font-semibold">{label}</span>
        <div className="text-muted-foreground">{icon}</div>
      </div>
      <div className="font-display text-xl font-black text-foreground">{value}</div>
      <div className="text-[11px] text-muted-foreground mt-1">{sub}</div>
    </div>
  );
}
