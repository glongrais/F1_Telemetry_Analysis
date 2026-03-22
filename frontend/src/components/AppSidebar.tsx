import { useState } from "react";
import { Link } from "react-router-dom";
import { ChevronDown, ChevronRight, Trophy, Calendar, BarChart3, PanelLeftClose, PanelLeft, X, MapPin } from "lucide-react";
import { cn } from "@/lib/utils";
import { useEvents, useSeasons } from "@/hooks/useEvents";
import { countryFlag } from "@/lib/countryFlag";
import { roundToCircuitId } from "@/lib/circuitMapping";

interface AppSidebarProps {
  collapsed: boolean;
  onToggle: () => void;
  selectedSeason: number;
  onSeasonChange: (season: number) => void;
  selectedRound: number | null;
  onRoundChange: (round: number | null) => void;
  selectedSession: string | null;
  onSessionChange: (session: string | null) => void;
  mobileOpen: boolean;
  onMobileToggle: () => void;
}

const sessionTypes = ["FP1", "FP2", "FP3", "Qualifying", "Race"];
const sprintSessions = ["FP1", "Sprint Qualifying", "Sprint", "Qualifying", "Race"];

export default function AppSidebar({
  collapsed,
  onToggle,
  selectedSeason,
  onSeasonChange,
  selectedRound,
  onRoundChange,
  selectedSession,
  onSessionChange,
  mobileOpen,
  onMobileToggle,
}: AppSidebarProps) {
  const [expandedRound, setExpandedRound] = useState<number | null>(null);

  const { data: events = [] } = useEvents(selectedSeason);
  const { data: seasons = [2024] } = useSeasons();

  const sidebarContent = (
    <>
      {/* Header */}
      <div className="flex items-center justify-between p-3 border-b border-sidebar-border">
        {!collapsed && (
          <div className="flex items-center gap-2">
            <div className="w-2 h-6 bg-primary rounded-sm" />
            <span className="font-display text-sm font-bold tracking-wider text-sidebar-foreground uppercase">
              F1 Data
            </span>
          </div>
        )}
        <div className="flex items-center gap-1">
          <button
            onClick={onToggle}
            className="p-1 text-muted-foreground hover:text-sidebar-foreground transition-colors hidden md:block"
          >
            {collapsed ? <PanelLeft size={18} /> : <PanelLeftClose size={18} />}
          </button>
          <button
            onClick={onMobileToggle}
            className="p-1 text-muted-foreground hover:text-sidebar-foreground transition-colors md:hidden"
          >
            <X size={18} />
          </button>
        </div>
      </div>

      {collapsed ? (
        <div className="flex flex-col items-center gap-1 pt-3">
          <button
            onClick={() => { onRoundChange(null); onSessionChange(null); }}
            className={cn("p-2 rounded-sm transition-colors", selectedRound === null ? "text-primary bg-primary/10" : "text-muted-foreground hover:text-sidebar-foreground")}
            title="Dashboard"
          >
            <Trophy size={16} />
          </button>
          <button
            onClick={() => { if (!selectedRound) onRoundChange(1); onToggle(); }}
            className="p-2 text-muted-foreground hover:text-sidebar-foreground rounded-sm transition-colors"
            title="Select Round"
          >
            <Calendar size={16} />
          </button>
          <button
            onClick={() => { if (!selectedRound) { onRoundChange(1); } if (!selectedSession) { onSessionChange("Qualifying"); } }}
            className={cn("p-2 rounded-sm transition-colors", selectedSession ? "text-primary bg-primary/10" : "text-muted-foreground hover:text-sidebar-foreground")}
            title="Telemetry"
          >
            <BarChart3 size={16} />
          </button>
        </div>
      ) : (
        <div className="flex-1 overflow-y-auto">
          {/* Season selector */}
          <div className="p-3 border-b border-sidebar-border">
            <label className="text-[10px] uppercase tracking-widest text-muted-foreground font-semibold">Season</label>
            <div className="flex gap-1 mt-1.5">
              {seasons.map((s) => (
                <button
                  key={s}
                  onClick={() => { onSeasonChange(s); onRoundChange(null); onSessionChange(null); }}
                  className={cn(
                    "px-2.5 py-1 text-xs font-display font-semibold rounded-sm transition-colors",
                    selectedSeason === s
                      ? "bg-primary text-primary-foreground"
                      : "bg-sidebar-accent text-muted-foreground hover:text-sidebar-foreground"
                  )}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>

          {/* Navigation */}
          <nav className="p-2">
            <button
              onClick={() => { onRoundChange(null); onSessionChange(null); onMobileToggle(); }}
              className={cn(
                "w-full flex items-center gap-2 px-2 py-1.5 text-xs font-medium rounded-sm transition-colors",
                selectedRound === null
                  ? "bg-sidebar-accent text-sidebar-foreground"
                  : "text-muted-foreground hover:text-sidebar-foreground hover:bg-sidebar-accent"
              )}
            >
              <Trophy size={14} />
              Dashboard
            </button>

            <div className="mt-3">
              <span className="px-2 text-[10px] uppercase tracking-widest text-muted-foreground font-semibold">
                Rounds
              </span>
              <div className="mt-1 space-y-0.5">
                {events.map((event) => (
                  <div key={event.round}>
                    <button
                      onClick={() => {
                        const newExpanded = expandedRound === event.round ? null : event.round;
                        setExpandedRound(newExpanded);
                        onRoundChange(event.round);
                        onSessionChange(null);
                      }}
                      className={cn(
                        "w-full flex items-center gap-2 px-2 py-1.5 text-xs rounded-sm transition-colors",
                        selectedRound === event.round
                          ? "bg-sidebar-accent text-sidebar-foreground"
                          : "text-muted-foreground hover:text-sidebar-foreground hover:bg-sidebar-accent"
                      )}
                    >
                      {expandedRound === event.round ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                      <span className="font-display font-semibold text-muted-foreground mr-1">R{event.round}</span>
                      <span className="text-sm leading-none">{countryFlag(event.country)}</span>
                      <span className="truncate">{event.location}</span>
                      {event.format === "sprint" && (
                        <span className="ml-auto text-[9px] px-1 py-0.5 bg-primary/20 text-primary rounded-sm font-semibold">S</span>
                      )}
                    </button>

                    {expandedRound === event.round && (
                      <div className="ml-5 mt-0.5 space-y-0.5 border-l border-sidebar-border pl-2">
                        {roundToCircuitId[event.round] && (
                          <Link
                            to={`/track/${roundToCircuitId[event.round]}`}
                            onClick={onMobileToggle}
                            className="w-full flex items-center gap-1.5 px-2 py-1 text-[11px] rounded-sm transition-colors text-muted-foreground hover:text-primary hover:bg-primary/10"
                          >
                            <MapPin size={10} />
                            Circuit Info
                          </Link>
                        )}
                        {(event.format === "sprint" ? sprintSessions : sessionTypes).map((session) => (
                          <button
                            key={session}
                            onClick={() => { onRoundChange(event.round); onSessionChange(session); onMobileToggle(); }}
                            className={cn(
                              "w-full text-left px-2 py-1 text-[11px] rounded-sm transition-colors",
                              selectedSession === session && selectedRound === event.round
                                ? "bg-primary/15 text-primary font-medium"
                                : "text-muted-foreground hover:text-sidebar-foreground"
                            )}
                          >
                            {session}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          </nav>
        </div>
      )}
    </>
  );

  return (
    <>
      {/* Desktop sidebar */}
      <aside
        className={cn(
          "hidden md:flex h-screen bg-sidebar border-r border-sidebar-border flex-col transition-all duration-200",
          collapsed ? "w-12" : "w-64"
        )}
      >
        {sidebarContent}
      </aside>

      {/* Mobile overlay */}
      {mobileOpen && (
        <div className="fixed inset-0 z-50 md:hidden">
          <div className="absolute inset-0 bg-background/80 backdrop-blur-sm" onClick={onMobileToggle} />
          <aside className="absolute left-0 top-0 h-full w-72 bg-sidebar border-r border-sidebar-border flex flex-col z-10">
            {sidebarContent}
          </aside>
        </div>
      )}
    </>
  );
}
