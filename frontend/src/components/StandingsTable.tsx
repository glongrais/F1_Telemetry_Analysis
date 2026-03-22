import { Link } from "react-router-dom";
import { cn } from "@/lib/utils";
import { Driver, Constructor } from "@/data/mockData";
import TeamLogo from "@/components/TeamLogo";

export function DriverStandings({ drivers }: { drivers: Driver[] }) {
  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Driver Standings</h3>
        <span className="text-[10px] text-muted-foreground uppercase tracking-widest">2024</span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border text-muted-foreground">
              <th className="text-left px-4 py-2 font-medium w-8">#</th>
              <th className="text-left px-2 py-2 font-medium">Driver</th>
              <th className="text-left px-2 py-2 font-medium">Team</th>
              <th className="text-right px-2 py-2 font-medium">Wins</th>
              <th className="text-right px-4 py-2 font-medium">Points</th>
            </tr>
          </thead>
          <tbody>
            {drivers.map((d) => (
              <tr key={d.abbreviation} className="border-b border-border/50 hover:bg-secondary/50 transition-colors">
                <td className="px-4 py-2 font-display font-bold text-muted-foreground">{d.position}</td>
                <td className="px-2 py-2">
                  <div className="flex items-center gap-2">
                    <div className="w-1 h-4 rounded-sm" style={{ backgroundColor: d.teamColor }} />
                    <Link to={`/driver/${d.abbreviation}`} className="font-display font-bold text-foreground hover:text-primary transition-colors">{d.abbreviation}</Link>
                    <span className="text-muted-foreground hidden sm:inline">{d.firstName} {d.lastName}</span>
                  </div>
                </td>
                <td className="px-2 py-2">
                  <div className="flex items-center gap-2">
                    <TeamLogo teamName={d.team} teamColor={d.teamColor} size={16} />
                    <span className="text-muted-foreground">{d.team}</span>
                  </div>
                </td>
                <td className="px-2 py-2 text-right font-display font-semibold">{d.wins}</td>
                <td className="px-4 py-2 text-right font-display font-bold text-foreground">{d.points}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function ConstructorStandings({ constructors }: { constructors: Constructor[] }) {
  const maxPoints = constructors[0]?.points ?? 1;

  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Constructor Standings</h3>
        <span className="text-[10px] text-muted-foreground uppercase tracking-widest">2024</span>
      </div>
      <div className="p-4 space-y-3">
        {constructors.map((c) => (
          <div key={c.name} className="flex items-center gap-3">
            <span className="font-display font-bold text-muted-foreground w-5 text-right text-xs">{c.position}</span>
            <TeamLogo teamName={c.name} teamColor={c.color} size={20} />
            <div className="flex-1 min-w-0">
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs font-display font-semibold">{c.name}</span>
                <span className="text-xs font-display font-bold">{c.points}</span>
              </div>
              <div className="h-1.5 bg-secondary rounded-sm overflow-hidden">
                <div
                  className="h-full rounded-sm transition-all duration-500"
                  style={{
                    width: `${(c.points / maxPoints) * 100}%`,
                    backgroundColor: c.color,
                  }}
                />
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
