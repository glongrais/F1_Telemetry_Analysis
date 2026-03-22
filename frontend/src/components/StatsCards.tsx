import { Trophy, Flag, Timer, Zap } from "lucide-react";
import type { Driver, RaceResult, RaceEvent } from "@/types/standings";

interface StatCardProps {
  label: string;
  value: string;
  sub: string;
  icon: React.ReactNode;
  accentColor?: string;
}

function StatCard({ label, value, sub, icon, accentColor }: StatCardProps) {
  return (
    <div
      className="bg-card rounded-sm border border-border p-4 relative overflow-hidden transition-all duration-200 hover:-translate-y-0.5 hover:shadow-lg hover:shadow-black/20 group"
    >
      {accentColor && (
        <div
          className="absolute left-0 top-0 bottom-0 w-[3px] transition-all duration-200 group-hover:w-1"
          style={{ backgroundColor: accentColor }}
        />
      )}
      <div className="flex items-center justify-between mb-3">
        <span className="text-[10px] uppercase tracking-widest text-muted-foreground font-semibold">{label}</span>
        <div
          className="transition-colors duration-200"
          style={{ color: accentColor ?? "hsl(var(--muted-foreground))" }}
        >
          {icon}
        </div>
      </div>
      <div
        className="font-display text-2xl font-black"
        style={accentColor ? { color: accentColor } : undefined}
      >
        {value}
      </div>
      <div className="text-xs text-muted-foreground mt-1">{sub}</div>
    </div>
  );
}

interface StatsCardsProps {
  driverStandings: Driver[];
  recentResults: RaceResult[];
  events: RaceEvent[];
  year: number;
}

export default function StatsCards({ driverStandings, recentResults, events, year }: StatsCardsProps) {
  const leader = driverStandings[0];
  const lastRace = recentResults[0];

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
      <StatCard
        label="Championship Leader"
        value={leader?.abbreviation ?? "—"}
        sub={leader ? `${leader.points} points` : ""}
        icon={<Trophy size={16} />}
        accentColor={leader?.teamColor}
      />
      <StatCard
        label="Last Race Winner"
        value={lastRace?.winner ?? "—"}
        sub={lastRace?.raceName ?? ""}
        icon={<Flag size={16} />}
        accentColor={undefined}
      />
      <StatCard
        label="Fastest Lap"
        value={lastRace?.fastestLap ?? "—"}
        sub={lastRace ? lastRace.raceName : ""}
        icon={<Timer size={16} />}
        accentColor="hsl(var(--fastest))"
      />
      <StatCard
        label="Rounds Complete"
        value={`${events.length}`}
        sub={`Season ${year}`}
        icon={<Zap size={16} />}
        accentColor="hsl(var(--personal-best))"
      />
    </div>
  );
}
