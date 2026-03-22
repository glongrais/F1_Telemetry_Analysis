export interface Driver {
  position: number;
  abbreviation: string;
  firstName: string;
  lastName: string;
  team: string;
  teamColor: string;
  points: number;
  wins: number;
  headshotUrl?: string;
  countryCode: string;
}

export interface Constructor {
  position: number;
  name: string;
  color: string;
  points: number;
  wins: number;
}

export interface RaceResult {
  round: number;
  raceName: string;
  country: string;
  date: string;
  winner: string;
  winnerTeam: string;
  teamColor: string;
  gap: string;
  fastestLap: string;
}

export interface SessionEntry {
  position: number;
  driver: string;
  abbreviation: string;
  team: string;
  teamColor: string;
  bestLap: string;
  gap: string;
  sector1: string;
  sector2: string;
  sector3: string;
  s1Status: 'fastest' | 'pb' | 'normal';
  s2Status: 'fastest' | 'pb' | 'normal';
  s3Status: 'fastest' | 'pb' | 'normal';
  tyre: 'SOFT' | 'MEDIUM' | 'HARD' | 'INTERMEDIATE' | 'WET';
  laps: number;
}

export interface RaceEvent {
  round: number;
  name: string;
  country: string;
  location: string;
  date: string;
  format: 'conventional' | 'sprint';
}

export const seasons = [2024, 2023, 2022];

export const events2024: RaceEvent[] = [
  { round: 1, name: "Bahrain Grand Prix", country: "BH", location: "Sakhir", date: "2024-03-02", format: "conventional" },
  { round: 2, name: "Saudi Arabian Grand Prix", country: "SA", location: "Jeddah", date: "2024-03-09", format: "conventional" },
  { round: 3, name: "Australian Grand Prix", country: "AU", location: "Melbourne", date: "2024-03-24", format: "conventional" },
  { round: 4, name: "Japanese Grand Prix", country: "JP", location: "Suzuka", date: "2024-04-07", format: "conventional" },
  { round: 5, name: "Chinese Grand Prix", country: "CN", location: "Shanghai", date: "2024-04-21", format: "sprint" },
  { round: 6, name: "Miami Grand Prix", country: "US", location: "Miami", date: "2024-05-05", format: "sprint" },
  { round: 7, name: "Emilia Romagna Grand Prix", country: "IT", location: "Imola", date: "2024-05-19", format: "conventional" },
  { round: 8, name: "Monaco Grand Prix", country: "MC", location: "Monaco", date: "2024-05-26", format: "conventional" },
];

export const driverStandings: Driver[] = [
  { position: 1, abbreviation: "VER", firstName: "Max", lastName: "Verstappen", team: "Red Bull Racing", teamColor: "#3671C6", points: 437, wins: 9, countryCode: "NL" },
  { position: 2, abbreviation: "NOR", firstName: "Lando", lastName: "Norris", team: "McLaren", teamColor: "#FF8000", points: 374, wins: 4, countryCode: "GB" },
  { position: 3, abbreviation: "LEC", firstName: "Charles", lastName: "Leclerc", team: "Ferrari", teamColor: "#E80020", points: 356, wins: 3, countryCode: "MC" },
  { position: 4, abbreviation: "PIA", firstName: "Oscar", lastName: "Piastri", team: "McLaren", teamColor: "#FF8000", points: 292, wins: 2, countryCode: "AU" },
  { position: 5, abbreviation: "SAI", firstName: "Carlos", lastName: "Sainz", team: "Ferrari", teamColor: "#E80020", points: 290, wins: 2, countryCode: "ES" },
  { position: 6, abbreviation: "HAM", firstName: "Lewis", lastName: "Hamilton", team: "Mercedes", teamColor: "#27F4D2", points: 211, wins: 2, countryCode: "GB" },
  { position: 7, abbreviation: "RUS", firstName: "George", lastName: "Russell", team: "Mercedes", teamColor: "#27F4D2", points: 207, wins: 1, countryCode: "GB" },
  { position: 8, abbreviation: "PER", firstName: "Sergio", lastName: "Perez", team: "Red Bull Racing", teamColor: "#3671C6", points: 152, wins: 0, countryCode: "MX" },
  { position: 9, abbreviation: "ALO", firstName: "Fernando", lastName: "Alonso", team: "Aston Martin", teamColor: "#229971", points: 70, wins: 0, countryCode: "ES" },
  { position: 10, abbreviation: "STR", firstName: "Lance", lastName: "Stroll", team: "Aston Martin", teamColor: "#229971", points: 24, wins: 0, countryCode: "CA" },
];

export const constructorStandings: Constructor[] = [
  { position: 1, name: "McLaren", color: "#FF8000", points: 666, wins: 6 },
  { position: 2, name: "Ferrari", color: "#E80020", points: 652, wins: 5 },
  { position: 3, name: "Red Bull Racing", color: "#3671C6", points: 589, wins: 9 },
  { position: 4, name: "Mercedes", color: "#27F4D2", points: 425, wins: 3 },
  { position: 5, name: "Aston Martin", color: "#229971", points: 94, wins: 0 },
  { position: 6, name: "RB", color: "#6692FF", points: 46, wins: 0 },
  { position: 7, name: "Haas", color: "#B6BABD", points: 58, wins: 0 },
  { position: 8, name: "Alpine", color: "#0093CC", points: 16, wins: 0 },
  { position: 9, name: "Williams", color: "#64C4FF", points: 17, wins: 0 },
  { position: 10, name: "Sauber", color: "#52E252", points: 4, wins: 0 },
];

export const recentResults: RaceResult[] = [
  { round: 8, raceName: "Monaco Grand Prix", country: "MC", date: "2024-05-26", winner: "Charles Leclerc", winnerTeam: "Ferrari", teamColor: "#E80020", gap: "+7.152s", fastestLap: "1:14.250" },
  { round: 7, raceName: "Emilia Romagna Grand Prix", country: "IT", date: "2024-05-19", winner: "Max Verstappen", winnerTeam: "Red Bull Racing", teamColor: "#3671C6", gap: "+0.725s", fastestLap: "1:18.589" },
  { round: 6, raceName: "Miami Grand Prix", country: "US", date: "2024-05-05", winner: "Lando Norris", winnerTeam: "McLaren", teamColor: "#FF8000", gap: "+7.612s", fastestLap: "1:30.634" },
  { round: 5, raceName: "Chinese Grand Prix", country: "CN", date: "2024-04-21", winner: "Max Verstappen", winnerTeam: "Red Bull Racing", teamColor: "#3671C6", gap: "+13.7s", fastestLap: "1:37.286" },
];

export const qualiResults: SessionEntry[] = [
  { position: 1, driver: "Max Verstappen", abbreviation: "VER", team: "Red Bull Racing", teamColor: "#3671C6", bestLap: "1:28.877", gap: "-", sector1: "27.234", sector2: "33.112", sector3: "28.531", s1Status: "fastest", s2Status: "pb", s3Status: "fastest", tyre: "SOFT", laps: 18 },
  { position: 2, driver: "Lando Norris", abbreviation: "NOR", team: "McLaren", teamColor: "#FF8000", bestLap: "1:29.012", gap: "+0.135", sector1: "27.301", sector2: "33.098", sector3: "28.613", s1Status: "pb", s2Status: "fastest", s3Status: "pb", tyre: "SOFT", laps: 19 },
  { position: 3, driver: "Charles Leclerc", abbreviation: "LEC", team: "Ferrari", teamColor: "#E80020", bestLap: "1:29.145", gap: "+0.268", sector1: "27.356", sector2: "33.201", sector3: "28.588", s1Status: "pb", s2Status: "normal", s3Status: "pb", tyre: "SOFT", laps: 17 },
  { position: 4, driver: "Oscar Piastri", abbreviation: "PIA", team: "McLaren", teamColor: "#FF8000", bestLap: "1:29.287", gap: "+0.410", sector1: "27.412", sector2: "33.189", sector3: "28.686", s1Status: "normal", s2Status: "pb", s3Status: "normal", tyre: "SOFT", laps: 20 },
  { position: 5, driver: "Carlos Sainz", abbreviation: "SAI", team: "Ferrari", teamColor: "#E80020", bestLap: "1:29.334", gap: "+0.457", sector1: "27.389", sector2: "33.245", sector3: "28.700", s1Status: "normal", s2Status: "normal", s3Status: "normal", tyre: "SOFT", laps: 18 },
  { position: 6, driver: "Lewis Hamilton", abbreviation: "HAM", team: "Mercedes", teamColor: "#27F4D2", bestLap: "1:29.501", gap: "+0.624", sector1: "27.445", sector2: "33.312", sector3: "28.744", s1Status: "normal", s2Status: "normal", s3Status: "normal", tyre: "SOFT", laps: 19 },
  { position: 7, driver: "George Russell", abbreviation: "RUS", team: "Mercedes", teamColor: "#27F4D2", bestLap: "1:29.556", gap: "+0.679", sector1: "27.478", sector2: "33.289", sector3: "28.789", s1Status: "normal", s2Status: "normal", s3Status: "normal", tyre: "SOFT", laps: 18 },
  { position: 8, driver: "Sergio Perez", abbreviation: "PER", team: "Red Bull Racing", teamColor: "#3671C6", bestLap: "1:29.612", gap: "+0.735", sector1: "27.501", sector2: "33.345", sector3: "28.766", s1Status: "normal", s2Status: "normal", s3Status: "normal", tyre: "SOFT", laps: 17 },
];
