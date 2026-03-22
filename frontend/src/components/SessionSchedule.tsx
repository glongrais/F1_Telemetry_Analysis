import { Calendar, Clock } from "lucide-react";
import { cn } from "@/lib/utils";
import { RaceEvent } from "@/types/standings";
import { countryFlag } from "@/lib/countryFlag";

interface SessionInfo {
  name: string;
  day: string;
  time: string;
  status: "completed" | "upcoming" | "live";
}

function getSessionSchedule(event: RaceEvent): SessionInfo[] {
  const raceDate = new Date(event.date);
  const fri = new Date(raceDate);
  fri.setDate(raceDate.getDate() - 2);
  const sat = new Date(raceDate);
  sat.setDate(raceDate.getDate() - 1);

  const fmt = (d: Date) =>
    d.toLocaleDateString("en-GB", { weekday: "short", day: "numeric", month: "short" });

  if (event.format === "sprint") {
    return [
      { name: "FP1", day: fmt(fri), time: "13:30", status: "completed" },
      { name: "Sprint Qualifying", day: fmt(fri), time: "17:30", status: "completed" },
      { name: "Sprint", day: fmt(sat), time: "13:00", status: "completed" },
      { name: "Qualifying", day: fmt(sat), time: "17:00", status: "completed" },
      { name: "Race", day: fmt(raceDate), time: "15:00", status: "completed" },
    ];
  }

  return [
    { name: "FP1", day: fmt(fri), time: "13:30", status: "completed" },
    { name: "FP2", day: fmt(fri), time: "17:00", status: "completed" },
    { name: "FP3", day: fmt(sat), time: "12:30", status: "completed" },
    { name: "Qualifying", day: fmt(sat), time: "16:00", status: "completed" },
    { name: "Race", day: fmt(raceDate), time: "15:00", status: "completed" },
  ];
}

const sessionTypeColors: Record<string, string> = {
  Race: "bg-primary/20 text-primary",
  Qualifying: "bg-fastest/20 text-fastest",
  Sprint: "bg-warning/20 text-warning",
  "Sprint Qualifying": "bg-warning/15 text-warning/80",
  FP1: "bg-muted text-muted-foreground",
  FP2: "bg-muted text-muted-foreground",
  FP3: "bg-muted text-muted-foreground",
};

interface SessionScheduleProps {
  event: RaceEvent;
  selectedSession: string | null;
  onSessionChange: (session: string) => void;
}

export default function SessionSchedule({ event, selectedSession, onSessionChange }: SessionScheduleProps) {
  const sessions = getSessionSchedule(event);

  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Calendar size={14} className="text-muted-foreground" />
          <h3 className="font-display text-sm font-bold uppercase tracking-wider">Session Schedule</h3>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-sm">{countryFlag(event.country)}</span>
          <span className="text-[10px] text-muted-foreground">{event.location}</span>
        </div>
      </div>
      <div className="divide-y divide-border/50">
        {sessions.map((s) => (
          <button
            key={s.name}
            onClick={() => onSessionChange(s.name)}
            className={cn(
              "w-full flex items-center gap-3 px-4 py-2.5 text-xs transition-colors",
              selectedSession === s.name
                ? "bg-secondary/80"
                : "hover:bg-secondary/40"
            )}
          >
            <span
              className={cn(
                "px-2 py-0.5 rounded-sm text-[10px] font-display font-bold uppercase w-[110px] text-center",
                sessionTypeColors[s.name] ?? "bg-muted text-muted-foreground"
              )}
            >
              {s.name}
            </span>
            <span className="text-muted-foreground">{s.day}</span>
            <div className="flex items-center gap-1 ml-auto">
              <Clock size={10} className="text-muted-foreground" />
              <span className="data-cell text-muted-foreground">{s.time}</span>
            </div>
            {selectedSession === s.name && (
              <div className="w-1.5 h-1.5 rounded-full bg-primary" />
            )}
          </button>
        ))}
      </div>
    </div>
  );
}
