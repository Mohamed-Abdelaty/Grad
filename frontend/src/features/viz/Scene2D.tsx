import React from 'react';
import { SceneModel, StationModel, PipeModel } from './model';

export interface Scene2DProps {
  model: SceneModel;
  paused?: boolean;
}

// Inline SVGs for states
function StateIcon({ state }: { state: StationModel['state'] }) {
  switch (state) {
    case 'active':
      return (
        <svg
          data-testid="icon-active"
          className="inline-block w-4 h-4 mr-1 animate-spin"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <path d="M21 12a9 9 0 1 1-6.219-8.56" />
        </svg>
      );
    case 'done':
      return (
        <svg
          data-testid="icon-done"
          className="inline-block w-4 h-4 mr-1 text-emerald-400"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <path d="M20 6 9 17l-5-5" />
        </svg>
      );
    case 'error':
      return (
        <svg
          data-testid="icon-error"
          className="inline-block w-4 h-4 mr-1 text-rose-500"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <circle cx="12" cy="12" r="10" />
          <line x1="12" y1="8" x2="12" y2="12" />
          <line x1="12" y1="16" x2="12.01" y2="16" />
        </svg>
      );
    case 'faulted':
      return (
        <svg
          data-testid="icon-faulted"
          className="inline-block w-4 h-4 mr-1 text-rose-400"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z" />
          <line x1="12" y1="9" x2="12" y2="13" />
          <line x1="12" y1="17" x2="12.01" y2="17" />
        </svg>
      );
    case 'recovering':
      return (
        <svg
          data-testid="icon-recovering"
          className="inline-block w-4 h-4 mr-1 text-amber-400 animate-spin"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" />
          <path d="M21 3v5h-5" />
          <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" />
          <path d="M3 21v-5h5" />
        </svg>
      );
    case 'skipped':
      return (
        <svg
          data-testid="icon-skipped"
          className="inline-block w-4 h-4 mr-1 text-slate-400"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <line x1="5" y1="12" x2="19" y2="12" />
        </svg>
      );
    case 'pending':
    default:
      return (
        <svg
          data-testid="icon-pending"
          className="inline-block w-4 h-4 mr-1 text-slate-500"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <circle cx="12" cy="12" r="10" />
          <polyline points="12 6 12 12 16 14" />
        </svg>
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
