import React from 'react';
import { SceneModel, StationModel } from './model';

export interface Scene2DProps {
  model: SceneModel;
  paused?: boolean;
}

// Inline SVG Icon per state
function StateIcon({ state, x, y }: { state: StationModel['state']; x: number; y: number }) {
  const gProps = { transform: `translate(${x}, ${y})`, 'data-testid': `icon-${state}` };
  switch (state) {
    case 'active':
      return (
        <g {...gProps}>
          <circle cx="8" cy="8" r="6" stroke="#38bdf8" strokeWidth="2" fill="none" strokeDasharray="16 8" />
        </g>
      );
    case 'done':
      return (
        <g {...gProps}>
          <circle cx="8" cy="8" r="6" stroke="#34d399" strokeWidth="1.5" fill="none" />
          <path d="M5 8.5 L7 10.5 L11 6.5" stroke="#34d399" strokeWidth="1.5" fill="none" strokeLinecap="round" />
        </g>
      );
    case 'error':
      return (
        <g {...gProps}>
          <circle cx="8" cy="8" r="6" stroke="#f43f5e" strokeWidth="1.5" fill="none" />
          <line x1="8" y1="5" x2="8" y2="8.5" stroke="#f43f5e" strokeWidth="1.5" strokeLinecap="round" />
          <circle cx="8" cy="11" r="0.75" fill="#f43f5e" />
        </g>
      );
    case 'faulted':
      return (
        <g {...gProps}>
          <polygon points="8,3 14,13 2,13" stroke="#f43f5e" strokeWidth="1.5" fill="none" />
          <line x1="8" y1="6.5" x2="8" y2="9.5" stroke="#f43f5e" strokeWidth="1.5" strokeLinecap="round" />
          <circle cx="8" cy="11.5" r="0.75" fill="#f43f5e" />
        </g>
      );
    case 'recovering':
      return (
        <g {...gProps}>
          <circle cx="8" cy="8" r="6" stroke="#fbbf24" strokeWidth="1.5" fill="none" strokeDasharray="10 6" />
        </g>
      );
    case 'skipped':
      return (
        <g {...gProps}>
          <line x1="4" y1="8" x2="12" y2="8" stroke="#94a3b8" strokeWidth="2" strokeDasharray="3 2" />
        </g>
      );
    case 'pending':
    default:
      return (
        <g {...gProps}>
          <circle cx="8" cy="8" r="6" stroke="#64748b" strokeWidth="1.5" fill="none" />
          <polyline points="8 5 8 8 10 9" stroke="#64748b" strokeWidth="1.5" fill="none" strokeLinecap="round" />
        </g>
      );
  }
}

const STAGE_THEMES: Record<
  StationModel['key'],
  { primary: string; fill: string; border: string; glow: string }
> = {
  source: {
    primary: '#38bdf8', // sky-400
    fill: 'rgba(56, 189, 248, 0.25)',
    border: 'rgba(56, 189, 248, 0.5)',
    glow: 'rgba(56, 189, 248, 0.15)',
  },
  kafka: {
    primary: '#fb923c', // orange-400
    fill: 'rgba(251, 146, 60, 0.25)',
    border: 'rgba(251, 146, 60, 0.5)',
    glow: 'rgba(251, 146, 60, 0.15)',
  },
  spark: {
    primary: '#a855f7', // purple-500
    fill: 'rgba(168, 85, 247, 0.25)',
    border: 'rgba(168, 85, 247, 0.5)',
    glow: 'rgba(168, 85, 247, 0.15)',
  },
  hdfs: {
    primary: '#34d399', // emerald-400
    fill: 'rgba(52, 211, 153, 0.25)',
    border: 'rgba(52, 211, 153, 0.5)',
    glow: 'rgba(52, 211, 153, 0.15)',
  },
};

const STATION_X = [50, 300, 550, 800];
const VESSEL_WIDTH = 150;
const VESSEL_HEIGHT = 150;
const VESSEL_Y = 55;

export const Scene2D: React.FC<Scene2DProps> = ({ model, paused = false }) => {
  return (
    <div className="w-full h-full flex flex-col justify-center items-center">
      <svg
        data-testid="viz-canvas"
        viewBox="0 0 1000 300"
        className="w-full h-auto max-h-[300px] select-none"
        aria-label="Pipeline 2D Flow Visualization"
      >
        <defs>
          <linearGradient id="shimmerGradient" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stopColor="rgba(255, 255, 255, 0.05)" />
            <stop offset="50%" stopColor="rgba(255, 255, 255, 0.3)" />
            <stop offset="100%" stopColor="rgba(255, 255, 255, 0.05)" />
          </linearGradient>

          {model.stations.map((s, idx) => (
            <clipPath key={`clip-${s.key}`} id={`vessel-clip-${s.key}`}>
              <rect
                x={STATION_X[idx]}
                y={VESSEL_Y}
                width={VESSEL_WIDTH}
                height={VESSEL_HEIGHT}
                rx="10"
                ry="10"
              />
            </clipPath>
          ))}

          <style>
            {`
              @keyframes shimmerMove {
                0% { transform: translateY(150px); }
                100% { transform: translateY(-150px); }
              }
              .viz-shimmer {
                animation: shimmerMove 2s linear infinite;
              }
              @keyframes dashFlow {
                to { stroke-dashoffset: -20; }
              }
              .viz-pipe-flow {
                animation: dashFlow 1.2s linear infinite;
              }
              .viz-paused .viz-shimmer,
              .viz-paused .viz-pipe-flow {
                animation: none !important;
              }
            `}
          </style>
        </defs>

        <g className={paused ? 'viz-paused' : ''}>
          {/* Pipes between stations */}
          {model.pipes.map((pipe, idx) => {
            const x1 = STATION_X[idx] + VESSEL_WIDTH;
            const x2 = STATION_X[idx + 1];
            const y = VESSEL_Y + VESSEL_HEIGHT / 2;

            if (pipe.blocked) {
              return (
                <g key={`pipe-${pipe.from}-${pipe.to}`} data-testid={`pipe-blocked-${pipe.from}-${pipe.to}`}>
                  <line
                    x1={x1}
                    y1={y}
                    x2={x2}
                    y2={y}
                    stroke="#f59e0b"
                    strokeWidth="5"
                    strokeDasharray="6 4"
                  />
                  <rect
                    x={(x1 + x2) / 2 - 28}
                    y={y - 12}
                    width="56"
                    height="24"
                    rx="4"
                    fill="#1e293b"
                    stroke="#f59e0b"
                    strokeWidth="1.5"
                  />
                  <text
                    x={(x1 + x2) / 2}
                    y={y + 4}
                    textAnchor="middle"
                    fill="#f59e0b"
                    fontSize="10"
                    fontWeight="bold"
                  >
                    BLOCKED
                  </text>
                </g>
              );
            }

            const isFlowing = pipe.flow > 0 && !paused;
            return (
              <g key={`pipe-${pipe.from}-${pipe.to}`} data-testid={`pipe-${pipe.from}-${pipe.to}`}>
                <line
                  x1={x1}
                  y1={y}
                  x2={x2}
                  y2={y}
                  stroke="#334155"
                  strokeWidth="6"
                  strokeLinecap="round"
                />
                {pipe.flow > 0 && (
                  <line
                    x1={x1}
                    y1={y}
                    x2={x2}
                    y2={y}
                    stroke="#38bdf8"
                    strokeWidth="3"
                    strokeDasharray="6 6"
                    className={isFlowing ? 'viz-pipe-flow' : ''}
                    strokeLinecap="round"
                  />
                )}
              </g>
            );
          })}

          {/* Stations */}
          {model.stations.map((station, idx) => {
            const x = STATION_X[idx];
            const theme = STAGE_THEMES[station.key];
            const isFaulted = station.state === 'faulted';
            const isError = station.state === 'error';

            let borderColor = theme.border;
            if (isFaulted || isError) borderColor = '#f43f5e';
            else if (station.state === 'done') borderColor = '#34d399';
            else if (station.state === 'active') borderColor = theme.primary;

            const fillHeight =
              station.level !== null
                ? Math.round(station.level * VESSEL_HEIGHT)
                : 0;
            const fillY = VESSEL_Y + (VESSEL_HEIGHT - fillHeight);

            return (
              <g
                key={station.key}
                data-testid={`station-${station.key}`}
                opacity={station.state === 'pending' ? 0.4 : 1}
              >
                {/* Station Label & Icon */}
                <StateIcon state={station.state} x={x + 10} y={VESSEL_Y - 26} />
                <text
                  x={x + VESSEL_WIDTH / 2}
                  y={VESSEL_Y - 22}
                  textAnchor="middle"
                  fill="#f8fafc"
                  fontSize="15"
                  fontWeight="600"
                >
                  {station.label}
                </text>

                {/* State line */}
                <text
                  x={x + VESSEL_WIDTH / 2}
                  y={VESSEL_Y - 6}
                  textAnchor="middle"
                  fill={
                    isFaulted || isError
                      ? '#f43f5e'
                      : station.state === 'done'
                      ? '#34d399'
                      : station.state === 'active'
                      ? theme.primary
                      : '#94a3b8'
                  }
                  fontSize="12"
                  fontWeight="500"
                >
                  {station.state}
                </text>

                {/* Vessel Outer Box */}
                <rect
                  x={x}
                  y={VESSEL_Y}
                  width={VESSEL_WIDTH}
                  height={VESSEL_HEIGHT}
                  rx="10"
                  ry="10"
                  fill="#0f172a"
                  stroke={borderColor}
                  strokeWidth={station.state === 'active' ? '2.5' : '1.5'}
                  strokeDasharray={station.state === 'skipped' ? '6 4' : undefined}
                />

                {/* Fill Level */}
                {station.level !== null ? (
                  <rect
                    data-level={station.level}
                    data-testid={`fill-${station.key}`}
                    clipPath={`url(#vessel-clip-${station.key})`}
                    x={x}
                    y={fillY}
                    width={VESSEL_WIDTH}
                    height={fillHeight}
                    fill={isFaulted ? 'rgba(244, 63, 94, 0.4)' : theme.fill}
                    style={{
                      transition: paused ? 'none' : 'height 400ms ease-out, y 400ms ease-out',
                    }}
                  />
                ) : (
                  // Indeterminate shimmer when active
                  <g clipPath={`url(#vessel-clip-${station.key})`}>
                    <rect
                      data-level="indeterminate"
                      data-testid={`fill-${station.key}`}
                      x={x}
                      y={VESSEL_Y}
                      width={VESSEL_WIDTH}
                      height={VESSEL_HEIGHT}
                      fill="transparent"
                    />
                    {station.working && (
                      <rect
                        data-testid={`shimmer-${station.key}`}
                        x={x}
                        y={VESSEL_Y}
                        width={VESSEL_WIDTH}
                        height="60"
                        fill="url(#shimmerGradient)"
                        className={paused ? '' : 'viz-shimmer'}
                      />
                    )}
                  </g>
                )}

                {/* Faulted "STOPPED" diagonal ribbon */}
                {isFaulted && (
                  <g data-testid={`stopped-ribbon-${station.key}`}>
                    <path
                      d={`M ${x} ${VESSEL_Y + 36} L ${x + 36} ${VESSEL_Y} L ${x + 76} ${VESSEL_Y} L ${x} ${VESSEL_Y + 76} Z`}
                      fill="#e11d48"
                    />
                    <text
                      transform={`rotate(-45 ${x + 28} ${VESSEL_Y + 28})`}
                      x={x + 28}
                      y={VESSEL_Y + 32}
                      textAnchor="middle"
                      fill="#ffffff"
                      fontSize="9"
                      fontWeight="bold"
                      letterSpacing="0.05em"
                    >
                      STOPPED
                    </text>
                  </g>
                )}

                {/* Counter text */}
                {station.counter && (
                  <text
                    x={x + VESSEL_WIDTH / 2}
                    y={VESSEL_Y + VESSEL_HEIGHT + 20}
                    textAnchor="middle"
                    fill="#e2e8f0"
                    fontSize="11"
                    fontFamily="monospace"
                  >
                    {station.counter}
                  </text>
                )}

                {/* Note text */}
                {station.note && (
                  <text
                    x={x + VESSEL_WIDTH / 2}
                    y={VESSEL_Y + VESSEL_HEIGHT + 36}
                    textAnchor="middle"
                    fill={isFaulted || isError ? '#f43f5e' : '#fbbf24'}
                    fontSize="10"
                    fontWeight="500"
                  >
                    {station.note.length > 24 ? `${station.note.slice(0, 24)}...` : station.note}
                  </text>
                )}
              </g>
            );
          })}
        </g>
      </svg>
    </div>
  );
};
