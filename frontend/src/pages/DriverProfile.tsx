import { useParams, Link } from "react-router-dom";
import { ArrowLeft, Trophy, Flag, Timer, TrendingUp, Medal } from "lucide-react";
import { useDriverStandings, useConstructorStandings, useDriverStandingsEvolution } from "@/hooks/useStandings";
import { useRecentResults } from "@/hooks/useResults";
import { countryFlag } from "@/lib/countryFlag";
import TeamLogo from "@/components/TeamLogo";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

export default function DriverProfile() {
  const { abbreviation } = useParams<{ abbreviation: string }>();
  const currentYear = new Date().getFullYear();
  const { data: driverStandings = [] } = useDriverStandings(currentYear);
  const { data: constructorStandings = [] } = useConstructorStandings(currentYear);
  const { data: recentResults = [] } = useRecentResults(currentYear);
  const { data: evo } = useDriverStandingsEvolution(currentYear);

  const driver = driverStandings.find((d) => d.abbreviation === abbreviation?.toUpperCase());

  if (!driver) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <h1 className="font-display text-2xl font-bold text-foreground mb-2">Driver Not Found</h1>
          <Link to="/" className="text-primary text-sm hover:underline">&larr; Back to Dashboard</Link>
        </div>
      </div>
    );
  }

  const constructor = constructorStandings.find((c) => c.name === driver.team);
  const driverWins = recentResults.filter((r) => r.winner.includes(driver.lastName));
  const color = driver.teamColor;

  const evolutionData = evo?.data ?? [];
  const pointsData = evolutionData.map((round) => ({
    round: round.raceName,
    points: (round[driver.abbreviation] as number) ?? 0,
  }));

  const pointsPerRace = pointsData.map((d, i) => ({
    round: d.round,
    points: i === 0 ? d.points : d.points - pointsData[i - 1].points,
  }));

  return (
    <div className="min-h-screen bg-background">
      {/* Hero header */}
      <div className="relative border-b border-border overflow-hidden">
        <div className="absolute inset-0 opacity-10" style={{ background: `linear-gradient(135deg, ${color}, transparent 70%)` }} />
        <div className="relative px-4 md:px-8 py-6 md:py-10 max-w-6xl mx-auto">
          <Link to="/" className="inline-flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground mb-4 transition-colors">
            <ArrowLeft size={14} />
            Back to Dashboard
          </Link>

          <div className="flex flex-col md:flex-row md:items-end gap-4 md:gap-8">
            <div className="flex-1">
              <div className="flex items-center gap-3 mb-2">
                <span className="text-2xl">{countryFlag(driver.countryCode)}</span>
                <span className="font-display text-xs font-bold text-muted-foreground">#{driver.position}</span>
              </div>
              <h1 className="font-display text-3xl md:text-4xl font-black text-foreground">
                <span className="text-muted-foreground font-normal">{driver.firstName}</span>{" "}
                {driver.lastName.toUpperCase()}
              </h1>
              <div className="flex items-center gap-3 mt-3">
                <TeamLogo teamName={driver.team} teamColor={color} size={24} />
                <span className="font-display text-sm font-semibold text-muted-foreground">{driver.team}</span>
              </div>
            </div>

            <div className="flex items-center gap-6">
              <StatBlock label="Points" value={driver.points.toString()} icon={<TrendingUp size={14} />} color={color} />
              <StatBlock label="Wins" value={driver.wins.toString()} icon={<Trophy size={14} />} color={color} />
              <StatBlock label="Standing" value={`P${driver.position}`} icon={<Medal size={14} />} color={color} />
            </div>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="px-4 md:px-8 py-6 max-w-6xl mx-auto space-y-6">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          {/* Season points evolution */}
          <div className="lg:col-span-2 bg-card rounded-sm border border-border overflow-hidden">
            <div className="px-4 py-3 border-b border-border">
              <h3 className="font-display text-sm font-bold uppercase tracking-wider">Points Evolution</h3>
            </div>
            <div className="p-4 h-[300px]">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={pointsData} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(220 10% 15%)" vertical={false} />
                  <XAxis dataKey="round" tick={{ fontSize: 10, fill: "hsl(218 11% 45%)" }} axisLine={{ stroke: "hsl(220 10% 18%)" }} tickLine={false} />
                  <YAxis tick={{ fontSize: 10, fill: "hsl(218 11% 45%)" }} axisLine={false} tickLine={false} width={36} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "hsl(220 12% 11%)",
                      border: "1px solid hsl(220 10% 20%)",
                      borderRadius: "2px",
                      fontSize: "11px",
                    }}
                  />
                  <Line type="monotone" dataKey="points" stroke={color} strokeWidth={2.5} dot={{ r: 3, fill: color, strokeWidth: 0 }} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Points per race */}
          <div className="bg-card rounded-sm border border-border overflow-hidden">
            <div className="px-4 py-3 border-b border-border">
              <h3 className="font-display text-sm font-bold uppercase tracking-wider">Points Per Race</h3>
            </div>
            <div className="p-4 space-y-2">
              {pointsPerRace.map((d) => (
                <div key={d.round} className="flex items-center gap-3">
                  <span className="text-[10px] font-display font-bold text-muted-foreground w-8">{d.round}</span>
                  <div className="flex-1 h-2 bg-secondary rounded-sm overflow-hidden">
                    <div
                      className="h-full rounded-sm transition-all"
                      style={{ width: `${(d.points / 26) * 100}%`, backgroundColor: color }}
                    />
                  </div>
                  <span className="text-xs font-display font-bold w-6 text-right">{d.points}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Recent wins */}
        {driverWins.length > 0 && (
          <div className="bg-card rounded-sm border border-border overflow-hidden">
            <div className="px-4 py-3 border-b border-border flex items-center gap-2">
              <Flag size={14} className="text-primary" />
              <h3 className="font-display text-sm font-bold uppercase tracking-wider">Race Wins</h3>
            </div>
            <div className="divide-y divide-border/50">
              {driverWins.map((r) => (
                <div key={r.round} className="px-4 py-3 flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <span className="font-display text-xs font-bold text-muted-foreground">R{r.round}</span>
                    <span className="text-sm">{countryFlag(r.country)}</span>
                    <span className="text-sm font-medium">{r.raceName}</span>
                  </div>
                  <div className="flex items-center gap-3 text-xs text-muted-foreground">
                    <span>{r.gap}</span>
                    <span className="flex items-center gap-1">
                      <Timer size={10} className="text-fastest" />
                      <span className="text-fastest data-cell">{r.fastestLap}</span>
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Teammate comparison */}
        {(() => {
          const teammate = driverStandings.find((d) => d.team === driver.team && d.abbreviation !== driver.abbreviation);
          if (!teammate) return null;
          return (
            <div className="bg-card rounded-sm border border-border overflow-hidden">
              <div className="px-4 py-3 border-b border-border">
                <h3 className="font-display text-sm font-bold uppercase tracking-wider">
                  vs {teammate.firstName} {teammate.lastName}
                </h3>
              </div>
              <div className="p-4">
                <div className="grid grid-cols-3 gap-4 text-center">
                  <div>
                    <span className="font-display text-2xl font-black" style={{ color }}>{driver.points}</span>
                    <p className="text-[10px] text-muted-foreground uppercase tracking-widest mt-1">Points</p>
                  </div>
                  <div className="flex items-center justify-center">
                    <span className="text-xs text-muted-foreground font-display">VS</span>
                  </div>
                  <div>
                    <span className="font-display text-2xl font-black" style={{ color: teammate.teamColor }}>{teammate.points}</span>
                    <p className="text-[10px] text-muted-foreground uppercase tracking-widest mt-1">Points</p>
                  </div>
                </div>
                <div className="flex h-2 rounded-sm overflow-hidden mt-4 gap-px">
                  <div className="h-full rounded-l-sm" style={{ width: `${(driver.points / (driver.points + teammate.points)) * 100}%`, backgroundColor: color }} />
                  <div className="h-full rounded-r-sm" style={{ width: `${(teammate.points / (driver.points + teammate.points)) * 100}%`, backgroundColor: teammate.teamColor, opacity: 0.5 }} />
                </div>
                <div className="flex justify-between mt-2">
                  <span className="font-display text-xs font-bold">{driver.abbreviation}</span>
                  <span className="font-display text-xs font-bold text-muted-foreground">{teammate.abbreviation}</span>
                </div>
              </div>
            </div>
          );
        })()}
      </div>
    </div>
  );
}

function StatBlock({ label, value, icon, color }: { label: string; value: string; icon: React.ReactNode; color: string }) {
  return (
    <div className="text-center">
      <div className="text-muted-foreground mb-1">{icon}</div>
      <span className="font-display text-xl md:text-2xl font-black block" style={{ color }}>{value}</span>
      <span className="text-[9px] uppercase tracking-widest text-muted-foreground">{label}</span>
    </div>
  );
}
