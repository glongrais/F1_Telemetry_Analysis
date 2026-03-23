import { useState } from "react";
import { Menu } from "lucide-react";
import AppSidebar from "@/components/AppSidebar";
import { DriverStandings, ConstructorStandings } from "@/components/StandingsTable";
import RecentResults from "@/components/RecentResults";
import SessionLeaderboard from "@/components/SessionLeaderboard";
import StatsCards from "@/components/StatsCards";
import TelemetryPanel from "@/components/TelemetryPanel";
import TrackMap from "@/components/TrackMap";
import StandingsEvolution from "@/components/StandingsEvolution";
import SessionSchedule from "@/components/SessionSchedule";
import TeamRadioFeed from "@/components/TeamRadioFeed";
import RaceControlPanel from "@/components/RaceControlPanel";
import WeatherPanel from "@/components/WeatherPanel";
import TyreStrategyPanel from "@/components/TyreStrategyPanel";
import HeadToHeadPanel from "@/components/HeadToHeadPanel";
import PositionChart from "@/components/PositionChart";
import GapChart from "@/components/GapChart";
import LapTimeChart from "@/components/LapTimeChart";
import SpeedTrapPanel from "@/components/SpeedTrapPanel";
import FastestLapsPanel from "@/components/FastestLapsPanel";
import NextRaceCountdown from "@/components/NextRaceCountdown";
import ThemeToggle from "@/components/ThemeToggle";
import { cn } from "@/lib/utils";
import { useDriverStandings, useConstructorStandings } from "@/hooks/useStandings";
import { useEvents, useSessions } from "@/hooks/useEvents";
import { useRecentResults } from "@/hooks/useResults";
import { useHeadToHead } from "@/hooks/useHeadToHead";
import {
  useSessionLeaderboard,
  useSessionLaps,
  useSessionPositions,
  useSessionGaps,
  useSessionWeather,
  useSessionStints,
  useSessionPitStops,
  useSessionRaceControl,
  useSessionRadio,
  useSessionSpeedTraps,
  useSessionFastestLaps,
} from "@/hooks/useSession";

type SessionTab = "overview" | "telemetry" | "analysis" | "strategy" | "radio";

const sessionTabs: { id: SessionTab; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "analysis", label: "Analysis" },
  { id: "telemetry", label: "Telemetry" },
  { id: "strategy", label: "Strategy" },
  { id: "radio", label: "Radio & Control" },
];

export default function Index() {
  const [collapsed, setCollapsed] = useState(false);
  const [selectedSeason, setSelectedSeason] = useState(new Date().getFullYear());
  const [selectedRound, setSelectedRound] = useState<number | null>(null);
  const [selectedSession, setSelectedSession] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<SessionTab>("overview");
  const [mobileOpen, setMobileOpen] = useState(false);

  // Data hooks - season level
  const { data: events = [] } = useEvents(selectedSeason);
  const { data: driverStandings = [] } = useDriverStandings(selectedSeason);
  const { data: constructorStandings = [] } = useConstructorStandings(selectedSeason);
  const { data: recentResults = [] } = useRecentResults(selectedSeason);
  const { data: headToHead = [] } = useHeadToHead(selectedSeason);

  // Resolve session ID from selected round + session name
  const sessionNameMap: Record<string, string> = {
    "FP1": "Practice 1", "FP2": "Practice 2", "FP3": "Practice 3",
    "Qualifying": "Qualifying", "Race": "Race",
    "Sprint": "Sprint", "Sprint Qualifying": "Sprint Qualifying", "Sprint Shootout": "Sprint Shootout",
  };
  const { data: sessionList = [] } = useSessions(selectedSeason, selectedRound);
  const resolvedSession = selectedSession
    ? sessionList.find((s) => s.name === selectedSession || s.type === selectedSession || s.name === sessionNameMap[selectedSession] || s.type === sessionNameMap[selectedSession])
    : sessionList.find((s) => s.type === "Race") ?? sessionList[0];
  const sessionId = resolvedSession?.sessionId ?? null;

  // Data hooks - session level (only fetch when sessionId is resolved)
  const { data: leaderboard = [] } = useSessionLeaderboard(sessionId);
  const { data: positions = [] } = useSessionPositions(sessionId);
  const { data: gaps = [] } = useSessionGaps(sessionId);
  const { data: lapTimes = [] } = useSessionLaps(sessionId);
  const { data: weather = [] } = useSessionWeather(sessionId);
  const { data: stints = [] } = useSessionStints(sessionId);
  const { data: pitStops = [] } = useSessionPitStops(sessionId);
  const { data: raceControl = [] } = useSessionRaceControl(sessionId);
  const { data: radio = [] } = useSessionRadio(sessionId);
  const { data: speedTraps = [] } = useSessionSpeedTraps(sessionId);
  const { data: fastestLaps = [] } = useSessionFastestLaps(sessionId);

  const selectedEvent = selectedRound ? events.find((e) => e.round === selectedRound) : null;
  const isSessionOrRound = selectedSession || (selectedRound && selectedEvent);

  return (
    <div className="flex h-screen overflow-hidden">
      <AppSidebar
        collapsed={collapsed}
        onToggle={() => setCollapsed(!collapsed)}
        selectedSeason={selectedSeason}
        onSeasonChange={setSelectedSeason}
        selectedRound={selectedRound}
        onRoundChange={(r) => { setSelectedRound(r); setActiveTab("overview"); }}
        selectedSession={selectedSession}
        onSessionChange={(s) => { setSelectedSession(s); setActiveTab("overview"); }}
        mobileOpen={mobileOpen}
        onMobileToggle={() => setMobileOpen(false)}
      />

      <main className="flex-1 overflow-y-auto">
        {/* Header bar */}
        <header className="sticky top-0 z-10 bg-background/80 backdrop-blur-sm border-b border-border px-4 md:px-6 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <button onClick={() => setMobileOpen(true)} className="md:hidden p-1 text-muted-foreground hover:text-foreground">
              <Menu size={20} />
            </button>
            {selectedEvent ? (
              <div className="flex items-center gap-2 md:gap-3">
                <span className="font-display text-xs font-bold text-primary">R{selectedEvent.round}</span>
                <h1 className="font-display text-sm md:text-lg font-bold truncate">{selectedEvent.name}</h1>
                {selectedSession && (
                  <span className="text-xs bg-secondary px-2 py-0.5 rounded-sm text-muted-foreground font-medium hidden sm:inline">
                    {selectedSession}
                  </span>
                )}
              </div>
            ) : (
              <h1 className="font-display text-sm md:text-lg font-bold">
                Season {selectedSeason} — <span className="text-primary">Overview</span>
              </h1>
            )}
          </div>
          <div className="flex items-center gap-3">
            <ThemeToggle />
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-personal-best animate-pulse-glow" />
              <span className="text-[10px] text-muted-foreground uppercase tracking-widest hidden sm:inline">Live Data</span>
            </div>
          </div>
        </header>

        {/* Session/Round tabs */}
        {isSessionOrRound && (
          <div className="border-b border-border px-3 md:px-6 flex items-center gap-1 bg-background overflow-x-auto">
            {sessionTabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={cn(
                  "px-2 md:px-3 py-2 text-[10px] md:text-xs font-display font-semibold uppercase tracking-wider border-b-2 transition-colors whitespace-nowrap",
                  activeTab === tab.id
                    ? "border-primary text-primary"
                    : "border-transparent text-muted-foreground hover:text-foreground"
                )}
              >
                {tab.label}
              </button>
            ))}
          </div>
        )}

        <div className="p-4 md:p-6 space-y-6">
          {isSessionOrRound ? (
            <>
              {activeTab === "overview" && (
                <div className="space-y-4">
                  <div className="grid grid-cols-1 xl:grid-cols-4 gap-4">
                    <div className="xl:col-span-3">
                      <SessionLeaderboard entries={leaderboard} sessionName={selectedSession ?? "Qualifying"} />
                    </div>
                    <div className="space-y-4">
                      {!selectedSession && selectedEvent && (
                        <SessionSchedule
                          event={selectedEvent}
                          selectedSession={selectedSession}
                          onSessionChange={setSelectedSession}
                        />
                      )}
                      <TrackMap selectedDrivers={leaderboard.map((e: any) => e.abbreviation)} />
                      <WeatherPanel data={weather} />
                    </div>
                  </div>
                </div>
              )}

              {activeTab === "analysis" && (
                <div className="space-y-4">
                  <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                    <PositionChart data={positions} />
                    <GapChart data={gaps} />
                  </div>
                  <LapTimeChart data={lapTimes} />
                  <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                    <FastestLapsPanel data={fastestLaps} />
                    <SpeedTrapPanel data={speedTraps} />
                  </div>
                </div>
              )}

              {activeTab === "telemetry" && <TelemetryPanel sessionId={sessionId} />}

              {activeTab === "strategy" && (
                <TyreStrategyPanel stints={stints} pitStops={pitStops} />
              )}

              {activeTab === "radio" && (
                <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                  <TeamRadioFeed messages={radio} />
                  <RaceControlPanel messages={raceControl} />
                </div>
              )}
            </>
          ) : (
            <>
              <NextRaceCountdown />
              <StatsCards driverStandings={driverStandings} recentResults={recentResults} events={events} year={selectedSeason} />

              <div>
                <h2 className="font-display text-xs uppercase tracking-[0.2em] text-muted-foreground mb-3 flex items-center gap-2">
                  <span className="w-4 h-[2px] bg-primary rounded-full" />
                  Championship Standings
                </h2>
                <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
                  <div className="xl:col-span-2">
                    <DriverStandings drivers={driverStandings} />
                  </div>
                  <ConstructorStandings constructors={constructorStandings} />
                </div>
              </div>

              <div>
                <h2 className="font-display text-xs uppercase tracking-[0.2em] text-muted-foreground mb-3 flex items-center gap-2">
                  <span className="w-4 h-[2px] bg-primary rounded-full" />
                  Season Progress
                </h2>
                <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
                  <div className="xl:col-span-2">
                    <StandingsEvolution year={selectedSeason} />
                  </div>
                  <RecentResults results={recentResults} />
                </div>
              </div>

              <div>
                <h2 className="font-display text-xs uppercase tracking-[0.2em] text-muted-foreground mb-3 flex items-center gap-2">
                  <span className="w-4 h-[2px] bg-primary rounded-full" />
                  Teammate Battles
                </h2>
                <HeadToHeadPanel comparisons={headToHead} />
              </div>
            </>
          )}
        </div>
      </main>
    </div>
  );
}
