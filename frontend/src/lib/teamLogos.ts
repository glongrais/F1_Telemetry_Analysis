/** Maps team names to their logo file paths in /logos/ */
const teamLogoMap: Record<string, string> = {
  "McLaren": "/logos/mclaren.webp",
  "Ferrari": "/logos/ferrari.webp",
  "Mercedes": "/logos/mercedes.webp",
  "RB": "/logos/rb.webp",
  "Haas": "/logos/haas.webp",
  "Alpine": "/logos/alpine.webp",
  "Williams": "/logos/williams.webp",
  // These use fallback color badges (logos couldn't be sourced)
};

export function getTeamLogo(teamName: string): string | null {
  // Try exact match first
  if (teamLogoMap[teamName]) return teamLogoMap[teamName];
  // Try partial match
  for (const [key, val] of Object.entries(teamLogoMap)) {
    if (teamName.toLowerCase().includes(key.toLowerCase())) return val;
  }
  return null;
}
