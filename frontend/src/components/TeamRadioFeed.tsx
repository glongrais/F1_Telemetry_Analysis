import { RadioMessage } from "@/data/sessionData";
import { Radio, Volume2 } from "lucide-react";

interface TeamRadioFeedProps {
  messages: RadioMessage[];
}

export default function TeamRadioFeed({ messages }: TeamRadioFeedProps) {
  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center gap-2">
        <Radio size={14} className="text-primary" />
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">Team Radio</h3>
        <span className="text-[10px] text-muted-foreground ml-auto">{messages.length} messages</span>
      </div>
      <div className="max-h-[400px] overflow-y-auto divide-y divide-border/30">
        {messages.map((msg) => (
          <div key={msg.id} className="px-4 py-3 hover:bg-secondary/30 transition-colors">
            <div className="flex items-center gap-2 mb-1.5">
              <div className="w-1.5 h-4 rounded-sm" style={{ backgroundColor: msg.teamColor }} />
              <span className="font-display text-xs font-bold">{msg.abbreviation}</span>
              <span className="text-[10px] text-muted-foreground">Lap {msg.lap}</span>
              <span className="text-[10px] data-cell text-muted-foreground ml-auto">{msg.timestamp}</span>
              {msg.audioUrl && (
                <button className="p-1 text-muted-foreground hover:text-foreground transition-colors">
                  <Volume2 size={12} />
                </button>
              )}
            </div>
            <p className="text-xs text-foreground/90 leading-relaxed pl-3.5 border-l-2 border-border ml-[3px]">
              "{msg.message}"
            </p>
          </div>
        ))}
      </div>
    </div>
  );
}
