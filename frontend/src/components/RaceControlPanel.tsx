import { RaceControlMessage } from "@/data/sessionData";
import { cn } from "@/lib/utils";
import { AlertTriangle, Flag, ShieldAlert, Info, Zap } from "lucide-react";

const categoryConfig: Record<string, { icon: React.ReactNode; color: string }> = {
  YELLOW_FLAG: { icon: <Flag size={12} />, color: "text-warning bg-warning/10 border-warning/30" },
  RED_FLAG: { icon: <Flag size={12} />, color: "text-primary bg-primary/10 border-primary/30" },
  SAFETY_CAR: { icon: <AlertTriangle size={12} />, color: "text-warning bg-warning/10 border-warning/30" },
  VSC: { icon: <AlertTriangle size={12} />, color: "text-warning bg-warning/10 border-warning/30" },
  DRS_ENABLED: { icon: <Zap size={12} />, color: "text-personal-best bg-personal-best/10 border-personal-best/30" },
  DRS_DISABLED: { icon: <Zap size={12} />, color: "text-muted-foreground bg-muted border-border" },
  PENALTY: { icon: <ShieldAlert size={12} />, color: "text-primary bg-primary/10 border-primary/30" },
  TRACK_LIMITS: { icon: <ShieldAlert size={12} />, color: "text-muted-foreground bg-muted border-border" },
  FLAG: { icon: <Flag size={12} />, color: "text-personal-best bg-personal-best/10 border-personal-best/30" },
  INFO: { icon: <Info size={12} />, color: "text-muted-foreground bg-muted border-border" },
};

interface RaceControlPanelProps {
  messages: RaceControlMessage[];
}

export default function RaceControlPanel({ messages }: RaceControlPanelProps) {
  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center gap-2">
        <Flag size={14} className="text-warning" />
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Race Control</h3>
      </div>
      <div className="max-h-[400px] overflow-y-auto divide-y divide-border/30">
        {messages.map((msg) => {
          const cfg = categoryConfig[msg.category] ?? categoryConfig.INFO;
          return (
            <div key={msg.id} className="px-4 py-2.5 flex items-start gap-3 hover:bg-secondary/30 transition-colors">
              <div className={cn("p-1 rounded-sm border mt-0.5", cfg.color)}>
                {cfg.icon}
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-0.5">
                  <span className="text-[10px] text-muted-foreground">Lap {msg.lap}</span>
                  <span className="text-[10px] data-cell text-muted-foreground">{msg.timestamp}</span>
                  {msg.driver && (
                    <span className="text-[10px] font-display font-bold text-foreground">{msg.driver}</span>
                  )}
                </div>
                <p className="text-xs font-medium">{msg.message}</p>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
