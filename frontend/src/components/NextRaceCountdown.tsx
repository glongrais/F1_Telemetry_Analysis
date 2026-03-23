import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { MapPin, Calendar } from "lucide-react";
import { useEvents } from "@/hooks/useEvents";
import { countryFlag } from "@/lib/countryFlag";
import { roundToCircuitId } from "@/lib/circuitMapping";

function getNextEvent(events: any[]) {
  const now = new Date();
  const upcoming = events.find((e) => new Date(e.date) > now);
  if (upcoming) return upcoming;
  // Fallback: use the last event with a future-shifted date
  if (events.length > 0) {
    const last = events[events.length - 1];
    const d = new Date();
    d.setDate(d.getDate() + 3);
    return { ...last, date: d.toISOString().slice(0, 10) };
  }
  return null;
}

interface TimeLeft {
  days: number;
  hours: number;
  minutes: number;
  seconds: number;
}

function calcTimeLeft(targetDate: string): TimeLeft {
  const diff = Math.max(0, new Date(targetDate).getTime() - Date.now());
  return {
    days: Math.floor(diff / (1000 * 60 * 60 * 24)),
    hours: Math.floor((diff / (1000 * 60 * 60)) % 24),
    minutes: Math.floor((diff / (1000 * 60)) % 60),
    seconds: Math.floor((diff / 1000) % 60),
  };
}

function CountdownUnit({ value, label }: { value: number; label: string }) {
  return (
    <div className="flex flex-col items-center">
      <div className="bg-secondary/80 border border-border rounded-sm px-3 py-1.5 min-w-[52px] flex items-center justify-center">
        <span className="font-display text-2xl md:text-3xl font-black text-foreground tabular-nums">
          {String(value).padStart(2, "0")}
        </span>
      </div>
      <span className="text-[9px] uppercase tracking-widest text-muted-foreground mt-1.5">{label}</span>
    </div>
  );
}

export default function NextRaceCountdown() {
  const { data: events = [] } = useEvents(new Date().getFullYear());
  const event = getNextEvent(events);
  const [timeLeft, setTimeLeft] = useState<TimeLeft>({ days: 0, hours: 0, minutes: 0, seconds: 0 });

  useEffect(() => {
    if (!event) return;
    setTimeLeft(calcTimeLeft(event.date));
    const interval = setInterval(() => setTimeLeft(calcTimeLeft(event.date)), 1000);
    return () => clearInterval(interval);
  }, [event?.date]);

  if (!event) {
    return (
      <div className="bg-card rounded-sm border border-border p-6 text-center text-muted-foreground text-sm">
        Loading schedule...
      </div>
    );
  }

  const raceDate = new Date(event.date);
  const formattedDate = raceDate.toLocaleDateString("en-GB", {
    weekday: "long",
    day: "numeric",
    month: "long",
    year: "numeric",
  });

  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="relative p-5 md:p-6">
        <div className="absolute top-0 left-0 w-full h-1 bg-gradient-to-r from-primary via-primary/60 to-transparent" />

        <div className="flex flex-col md:flex-row md:items-center gap-4 md:gap-8">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-[10px] uppercase tracking-widest text-primary font-display font-bold">
                Next Race — Round {event.round}
              </span>
              {event.format === "sprint" && (
                <span className="text-[9px] px-1.5 py-0.5 bg-primary/20 text-primary rounded-sm font-bold font-display">
                  SPRINT
                </span>
              )}
            </div>
            <h2 className="font-display text-lg md:text-xl font-black text-foreground flex items-center gap-2">
              <span className="text-xl">{countryFlag(event.country)}</span>
              {event.name}
            </h2>
            <div className="flex items-center gap-4 mt-2 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <MapPin size={12} />
                {event.location}
              </span>
              <span className="flex items-center gap-1">
                <Calendar size={12} />
                {formattedDate}
              </span>
              {roundToCircuitId[event.round] && (
                <Link
                  to={`/track/${roundToCircuitId[event.round]}`}
                  className="flex items-center gap-1 text-primary hover:underline font-medium"
                >
                  Circuit Info →
                </Link>
              )}
            </div>
          </div>

          <div className="flex items-center gap-2 md:gap-3">
            <CountdownUnit value={timeLeft.days} label="Days" />
            <span className="text-muted-foreground/40 font-display text-xl mt-[-16px]">:</span>
            <CountdownUnit value={timeLeft.hours} label="Hrs" />
            <span className="text-muted-foreground/40 font-display text-xl mt-[-16px]">:</span>
            <CountdownUnit value={timeLeft.minutes} label="Min" />
            <span className="text-muted-foreground/40 font-display text-xl mt-[-16px]">:</span>
            <CountdownUnit value={timeLeft.seconds} label="Sec" />
          </div>
        </div>
      </div>
    </div>
  );
}
