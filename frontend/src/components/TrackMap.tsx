import { useMemo, useState } from "react";
import {
  trackPoints,
  getCarPositions,
  corners,
  drsZones,
  sectorBoundaries,
  sectorColors,
} from "@/data/trackData";

interface TrackMapProps {
  selectedDrivers: string[];
}

export default function TrackMap({ selectedDrivers }: TrackMapProps) {
  const [hoveredCar, setHoveredCar] = useState<string | null>(null);

  // Build smooth SVG path
  const pathD = useMemo(() => {
    if (trackPoints.length === 0) return "";
    const [first, ...rest] = trackPoints;
    return (
      `M ${first.x} ${first.y} ` +
      rest.map((p) => `L ${p.x} ${p.y}`).join(" ") +
      " Z"
    );
  }, []);

  // Build sector sub-paths
  const sectorPaths = useMemo(() => {
    const boundaries = [...sectorBoundaries, trackPoints.length];
    return boundaries.slice(0, -1).map((start, i) => {
      const end = boundaries[i + 1];
      const slice = trackPoints.slice(start, end + 1);
      if (slice.length === 0) return "";
      const [f, ...r] = slice;
      return `M ${f.x} ${f.y} ` + r.map((p) => `L ${p.x} ${p.y}`).join(" ");
    });
  }, []);

  // Build DRS zone sub-paths
  const drsPaths = useMemo(() => {
    return drsZones.map((zone) => {
      const slice = trackPoints.slice(zone.startIdx, zone.endIdx + 1);
      if (slice.length === 0) return "";
      const [f, ...r] = slice;
      return `M ${f.x} ${f.y} ` + r.map((p) => `L ${p.x} ${p.y}`).join(" ");
    });
  }, []);

  const cars = useMemo(() => getCarPositions(selectedDrivers), [selectedDrivers]);

  // Compute bounds
  const padding = 50;
  const minX = Math.min(...trackPoints.map((p) => p.x)) - padding;
  const minY = Math.min(...trackPoints.map((p) => p.y)) - padding;
  const maxX = Math.max(...trackPoints.map((p) => p.x)) + padding;
  const maxY = Math.max(...trackPoints.map((p) => p.y)) + padding;

  return (
    <div className="bg-card rounded-sm border border-border overflow-hidden">
      <div className="px-4 py-3 border-b border-border flex items-center justify-between">
        <h3 className="font-display text-sm font-bold uppercase tracking-wider">
          Track Map
        </h3>
        <div className="flex items-center gap-3">
          {/* Sector legend */}
          <div className="flex items-center gap-2">
            {sectorColors.map((c, i) => (
              <span key={i} className="flex items-center gap-1">
                <span
                  className="w-2 h-2 rounded-full"
                  style={{ backgroundColor: c }}
                />
                <span className="text-[9px] text-muted-foreground font-semibold">
                  S{i + 1}
                </span>
              </span>
            ))}
          </div>
          <span className="text-[10px] text-muted-foreground uppercase tracking-widest">
            {cars.length} cars
          </span>
        </div>
      </div>
      <div className="p-4">
        <svg
          viewBox={`${minX} ${minY} ${maxX - minX} ${maxY - minY}`}
          className="w-full h-auto max-h-[360px]"
          style={{ aspectRatio: `${maxX - minX} / ${maxY - minY}` }}
        >
          <defs>
            {/* Glow filter for DRS */}
            <filter id="drsGlow" x="-20%" y="-20%" width="140%" height="140%">
              <feGaussianBlur stdDeviation="3" result="blur" />
              <feMerge>
                <feMergeNode in="blur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
            {/* Glow filter for hovered car */}
            <filter id="carGlow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="4" result="blur" />
              <feMerge>
                <feMergeNode in="blur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>

          {/* Track outline (wide dark stroke) */}
          <path
            d={pathD}
            fill="none"
            stroke="hsl(220 10% 18%)"
            strokeWidth="16"
            strokeLinecap="round"
            strokeLinejoin="round"
          />

          {/* Track surface */}
          <path
            d={pathD}
            fill="none"
            stroke="hsl(220 10% 13%)"
            strokeWidth="12"
            strokeLinecap="round"
            strokeLinejoin="round"
          />

          {/* Sector overlays */}
          {sectorPaths.map((d, i) => (
            <path
              key={`sector-${i}`}
              d={d}
              fill="none"
              stroke={sectorColors[i]}
              strokeWidth="12"
              strokeLinecap="round"
              strokeLinejoin="round"
              opacity="0.12"
            />
          ))}

          {/* DRS zones (bright green overlay) */}
          {drsPaths.map((d, i) => (
            <path
              key={`drs-${i}`}
              d={d}
              fill="none"
              stroke="hsl(142 71% 45%)"
              strokeWidth="12"
              strokeLinecap="round"
              strokeLinejoin="round"
              opacity="0.25"
              filter="url(#drsGlow)"
            />
          ))}

          {/* DRS labels */}
          {drsZones.map((zone, i) => {
            const midIdx = Math.floor((zone.startIdx + zone.endIdx) / 2);
            const pt = trackPoints[midIdx];
            return (
              <text
                key={`drs-label-${i}`}
                x={pt.x}
                y={pt.y - 14}
                textAnchor="middle"
                fill="hsl(142 71% 45%)"
                fontSize="8"
                fontWeight="700"
                opacity="0.7"
              >
                DRS
              </text>
            );
          })}

          {/* Start/finish line */}
          <line
            x1={trackPoints[0].x}
            y1={trackPoints[0].y - 12}
            x2={trackPoints[0].x}
            y2={trackPoints[0].y + 12}
            stroke="hsl(0 0% 70%)"
            strokeWidth="2"
          />
          <rect
            x={trackPoints[0].x - 1}
            y={trackPoints[0].y - 12}
            width="2"
            height="6"
            fill="hsl(0 0% 90%)"
          />
          <rect
            x={trackPoints[0].x - 1}
            y={trackPoints[0].y + 6}
            width="2"
            height="6"
            fill="hsl(0 0% 90%)"
          />

          {/* Corner markers */}
          {corners.map((c) => (
            <g key={`corner-${c.number}`}>
              <circle
                cx={c.x}
                cy={c.y}
                r="3"
                fill="none"
                stroke="hsl(0 0% 40%)"
                strokeWidth="1"
              />
              <text
                x={c.labelX}
                y={c.labelY}
                textAnchor="middle"
                fill="hsl(0 0% 45%)"
                fontSize="8"
                fontWeight="600"
              >
                T{c.number}
              </text>
            </g>
          ))}

          {/* Car positions */}
          {cars.map((car) => {
            const isHovered = hoveredCar === car.abbreviation;
            return (
              <g
                key={car.abbreviation}
                onMouseEnter={() => setHoveredCar(car.abbreviation)}
                onMouseLeave={() => setHoveredCar(null)}
                style={{ cursor: "pointer" }}
              >
                {/* Animated pulse ring */}
                <circle
                  cx={car.x}
                  cy={car.y}
                  r={isHovered ? 16 : 12}
                  fill={car.teamColor}
                  opacity={isHovered ? 0.15 : 0.08}
                  style={{
                    transition: "r 0.2s, opacity 0.2s",
                  }}
                >
                  <animate
                    attributeName="r"
                    values={isHovered ? "14;18;14" : "10;14;10"}
                    dur="2s"
                    repeatCount="indefinite"
                  />
                  <animate
                    attributeName="opacity"
                    values={isHovered ? "0.2;0.05;0.2" : "0.12;0.03;0.12"}
                    dur="2s"
                    repeatCount="indefinite"
                  />
                </circle>

                {/* Car dot */}
                <circle
                  cx={car.x}
                  cy={car.y}
                  r={isHovered ? 7 : 5}
                  fill={car.teamColor}
                  stroke="hsl(220 14% 7%)"
                  strokeWidth="2"
                  filter={isHovered ? "url(#carGlow)" : undefined}
                  style={{ transition: "r 0.2s" }}
                />

                {/* Driver label */}
                <text
                  x={car.x}
                  y={car.y - (isHovered ? 14 : 10)}
                  textAnchor="middle"
                  fill={car.teamColor}
                  fontSize={isHovered ? "10" : "8"}
                  fontWeight="700"
                  style={{ transition: "font-size 0.2s" }}
                >
                  {car.abbreviation}
                </text>

                {/* Tooltip on hover */}
                {isHovered && (
                  <g>
                    <rect
                      x={car.x - 32}
                      y={car.y + 12}
                      width="64"
                      height="22"
                      rx="3"
                      fill="hsl(220 12% 11%)"
                      stroke={car.teamColor}
                      strokeWidth="1"
                      opacity="0.95"
                    />
                    <text
                      x={car.x}
                      y={car.y + 21}
                      textAnchor="middle"
                      fill="hsl(0 0% 80%)"
                      fontSize="7"
                      fontWeight="600"
                    >
                      P{car.position}
                    </text>
                    <text
                      x={car.x}
                      y={car.y + 30}
                      textAnchor="middle"
                      fill={car.teamColor}
                      fontSize="7"
                      fontWeight="700"
                    >
                      {car.speed} km/h
                    </text>
                  </g>
                )}
              </g>
            );
          })}
        </svg>
      </div>
    </div>
  );
}
