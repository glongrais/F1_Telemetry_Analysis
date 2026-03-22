// Maps event round numbers to circuit IDs
export const roundToCircuitId: Record<number, string> = {
  1: "bahrain",
  2: "jeddah",
  3: "melbourne",
  4: "suzuka",
  5: "shanghai",
  6: "miami",
  7: "imola",
  8: "monaco",
};

// Maps country codes to circuit IDs
export const countryToCircuitId: Record<string, string> = {
  BH: "bahrain",
  SA: "jeddah",
  AU: "melbourne",
  JP: "suzuka",
  CN: "shanghai",
  US: "miami",
  IT: "imola",
  MC: "monaco",
};
