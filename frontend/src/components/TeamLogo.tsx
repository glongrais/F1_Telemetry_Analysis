import { getTeamLogo } from "@/lib/teamLogos";

interface TeamLogoProps {
  teamName: string;
  teamColor: string;
  size?: number;
}

export default function TeamLogo({ teamName, teamColor, size = 20 }: TeamLogoProps) {
  const logo = getTeamLogo(teamName);

  if (logo) {
    return (
      <img
        src={logo}
        alt={teamName}
        className="object-contain shrink-0"
        style={{ width: size, height: size }}
      />
    );
  }

  // Fallback: colored abbreviation badge
  const abbrev = teamName.split(" ").map((w) => w[0]).join("").slice(0, 3);
  return (
    <div
      className="flex items-center justify-center rounded-sm text-[8px] font-display font-bold shrink-0"
      style={{
        width: size,
        height: size,
        backgroundColor: teamColor + "30",
        color: teamColor,
      }}
    >
      {abbrev}
    </div>
  );
}
