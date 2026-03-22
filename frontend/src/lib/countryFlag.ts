/** Convert ISO 3166-1 alpha-2 country code to flag emoji */
export function countryFlag(code: string): string {
  const upper = code.toUpperCase();
  const offset = 0x1F1E6 - 65; // 'A' char code
  return String.fromCodePoint(
    upper.charCodeAt(0) + offset,
    upper.charCodeAt(1) + offset
  );
}
