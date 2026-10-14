import React, { useRef, useMemo, useEffect, useState, useCallback } from 'react';
import * as THREE from 'three';
import { Canvas, useFrame, useThree } from '@react-three/fiber';
import { OrbitControls, Html } from '@react-three/drei';
import { SceneModel, StationModel, PipeModel } from './model';

export interface PipelineScene3DProps {
  model: SceneModel;
  quality: 'high' | 'low';
  paused: boolean;
  onFps?: (avg: number) => void;
  onContextLost?: () => void;
}

const STATION_POSITIONS: Record<StationModel['key'], [number, number, number]> = {
  source: [-4.5, 0, 0],
  kafka: [-1.5, 0, 0],
  spark: [1.5, 0, 0],
  hdfs: [4.5, 0, 0],
};

const STAGE_COLORS: Record<StationModel['key'], string> = {
  source: '#38bdf8', // sky-400
  kafka: '#fb923c',  // orange-400
  spark: '#a855f7',  // purple-500
  hdfs: '#34d399',   // emerald-400
};

// FPS Monitor Component inside Canvas
function FpsTracker({ onFps, paused }: { onFps?: (avg: number) => void; paused: boolean }) {
  const frameCount = useRef(0);
  const lastTime = useRef(performance.now());

  useFrame(() => {
    if (!onFps || paused) return;
    frameCount.current++;
    const now = performance.now();
    const elapsed = now - lastTime.current;
    if (elapsed >= 1000) {
      const fps = (frameCount.current * 1000) / elapsed;
      onFps(Math.round(fps));
      frameCount.current = 0;
      lastTime.current = now;
    }
  });

  return null;
}

// Camera auto-adjust to fit stations
function CameraController() {
  const { camera, size } = useThree();
  useEffect(() => {
    if (camera instanceof THREE.PerspectiveCamera) {
      const aspect = size.width / Math.max(1, size.height);
      if (aspect < 1.6) {
        // Narrower container: pull back camera
        camera.position.z = 9 + (1.6 - aspect) * 6;
      } else {
        camera.position.z = 9;
      }
      camera.updateProjectionMatrix();
    }
  }, [camera, size]);
  return null;
}

// Pipe Instanced Particles
function PipeParticles({
  pipe,
  quality,
  paused,
  fromPos,
  toPos,
}: {
  pipe: PipeModel;
  quality: 'high' | 'low';
  paused: boolean;
  fromPos: [number, number, number];
  toPos: [number, number, number];
}) {
  const count = quality === 'high' ? 60 : 20;
  const meshRef = useRef<THREE.InstancedMesh>(null);
  const tempObject = useMemo(() => new THREE.Object3D(), []);

  // Preallocated progress array [0..1]
  const progressArr = useMemo(() => {
    const arr = new Float32Array(count);
    for (let i = 0; i < count; i++) {
      arr[i] = i / count;
    }
    return arr;
  }, [count]);

  const startX = fromPos[0] + 0.9;
  const endX = toPos[0] - 0.9;
  const pipeDist = endX - startX;

  useFrame((_, delta) => {
    if (!meshRef.current) return;
    if (pipe.flow === 0) {
      // Hide all particles
      for (let i = 0; i < count; i++) {
        tempObject.scale.set(0, 0, 0);
        tempObject.updateMatrix();
        meshRef.current.setMatrixAt(i, tempObject.matrix);
      }
      meshRef.current.instanceMatrix.needsUpdate = true;
      return;
    }

    if (!paused) {
      const speed = delta * pipe.flow * 0.8;
      for (let i = 0; i < count; i++) {
        if (pipe.blocked) {
          // Particles queue up near the end
          if (progressArr[i] < 0.9) {
            progressArr[i] += speed;
          }
        } else {
          progressArr[i] = (progressArr[i] + speed) % 1;
        }

        const t = progressArr[i];
        const px = startX + t * pipeDist;
        const py = fromPos[1] + Math.sin(t * Math.PI * 4 + i) * 0.04;
        const pz = fromPos[2] + Math.cos(t * Math.PI * 4 + i) * 0.04;

        tempObject.position.set(px, py, pz);
        tempObject.scale.set(1, 1, 1);
        tempObject.updateMatrix();
        meshRef.current.setMatrixAt(i, tempObject.matrix);
      }
      meshRef.current.instanceMatrix.needsUpdate = true;
    }
  });

  return (
    <instancedMesh
      ref={meshRef}
      args={[undefined, undefined, count]}
      frustumCulled={false}
    >
      <sphereGeometry args={[0.04, 8, 8]} />
      <meshBasicMaterial color={pipe.blocked ? '#f59e0b' : '#38bdf8'} />
    </instancedMesh>
  );
}

// Single Station in 3D
function Station3D({
  station,
  position,
  paused,
}: {
  station: StationModel;
  position: [number, number, number];
  paused: boolean;
}) {
  const groupRef = useRef<THREE.Group>(null);
  const liquidRef = useRef<THREE.Mesh>(null);
  const shimmerRef = useRef<THREE.Mesh>(null);
  const gear1Ref = useRef<THREE.Mesh>(null);
  const gear2Ref = useRef<THREE.Mesh>(null);
  const gear3Ref = useRef<THREE.Mesh>(null);
  const arcMeshRef = useRef<THREE.Mesh>(null);

  // Eased level
  const currentLevelRef = useRef<number>(station.level ?? 0);
  const prevStateRef = useRef<string>(station.state);
  const transitionTimeRef = useRef<number>(0);

  // Gears rotation
  const gearRotRef = useRef<number>(0);

  // Spark ring geometry quantized to 0.01
  const quantizedLevel = useMemo(() => {
    if (station.key !== 'spark' || station.level === null) return 0;
    return Math.max(0.01, Math.round(station.level * 100) / 100);
  }, [station.key, station.level]);

  const arcGeo = useMemo(() => {
    if (station.key !== 'spark') return null;
    return new THREE.RingGeometry(0.7, 0.85, 48, 1, 0, quantizedLevel * 2 * Math.PI);
  }, [station.key, quantizedLevel]);

  // Clean up geometries on unmount
  useEffect(() => {
    return () => {
      arcGeo?.dispose();
    };
  }, [arcGeo]);

  useFrame((state, delta) => {
    if (!groupRef.current) return;

    // Detect state changes for one-shot animations
    if (station.state !== prevStateRef.current) {
      prevStateRef.current = station.state;
      transitionTimeRef.current = state.clock.getElapsedTime();
    }

    const timeSinceTransition = state.clock.getElapsedTime() - transitionTimeRef.current;

    // Level easing
    const targetLevel = station.level ?? 0;
    if (paused) {
      currentLevelRef.current = targetLevel;
    } else {
      currentLevelRef.current = THREE.MathUtils.damp(
        currentLevelRef.current,
        targetLevel,
        4,
        delta
      );
    }

    // Liquid mesh adjustment
    if (liquidRef.current) {
      if (station.level !== null) {
        liquidRef.current.visible = true;
        const lvl = currentLevelRef.current;
        liquidRef.current.scale.y = Math.max(0.01, lvl);
        liquidRef.current.position.y = -1.0 + lvl * 1.0;
      } else {
        liquidRef.current.visible = false;
      }
    }

    // Indeterminate shimmer
    if (shimmerRef.current) {
      if (station.level === null && station.working && !paused) {
        shimmerRef.current.visible = true;
        shimmerRef.current.position.y = Math.sin(state.clock.getElapsedTime() * 3) * 0.9;
      } else {
        shimmerRef.current.visible = false;
      }
    }

    // Motion per state
    if (!paused) {
      // 1. Working gears on Spark
      if (station.working && station.key === 'spark') {
        gearRotRef.current += delta * 2.5;
        if (gear1Ref.current) gear1Ref.current.rotation.z = gearRotRef.current;
        if (gear2Ref.current) gear2Ref.current.rotation.z = -gearRotRef.current;
        if (gear3Ref.current) gear3Ref.current.rotation.z = gearRotRef.current;
      }

      // 2. Pulse on done
      if (station.state === 'done' && timeSinceTransition < 0.6) {
        const pulse = 1 + Math.sin((timeSinceTransition / 0.6) * Math.PI) * 0.05;
        groupRef.current.scale.set(pulse, pulse, pulse);
      } else if (station.state === 'error' && timeSinceTransition < 0.4) {
        // Shake on error
        const shake = Math.sin((timeSinceTransition / 0.4) * Math.PI * 8) * 0.08;
        groupRef.current.position.x = position[0] + shake;
      } else if (station.state === 'faulted') {
        // Flicker / fault state
        groupRef.current.scale.set(1, 1, 1);
        groupRef.current.position.x = position[0];
      } else if (station.state === 'recovering') {
        // Amber pulse
        const pulse = 1 + Math.sin(state.clock.getElapsedTime() * 9.4) * 0.03;
        groupRef.current.scale.set(pulse, pulse, pulse);
        groupRef.current.position.x = position[0];
      } else {
        groupRef.current.scale.set(1, 1, 1);
        groupRef.current.position.x = position[0];
      }
    }
  });

  const baseColor = STAGE_COLORS[station.key];
  const isDone = station.state === 'done';
  const isFaulted = station.state === 'faulted';
  const isError = station.state === 'error';
  const isPending = station.state === 'pending';

  let vesselColor = baseColor;
  if (isDone) vesselColor = '#34d399';
  else if (isFaulted || isError) vesselColor = '#f43f5e';
  else if (station.state === 'recovering') vesselColor = '#fbbf24';

  return (
    <group ref={groupRef} position={position}>
      {/* Station Outer Vessel */}
      {station.key === 'spark' ? (
        // Spark: Rounded box housing
        <mesh>
          <boxGeometry args={[1.8, 2.2, 1.2]} />
          <meshStandardMaterial
            color={vesselColor}
            transparent
            opacity={isPending ? 0.2 : 0.35}
            roughness={0.4}
            metalness={0.6}
          />
        </mesh>
      ) : station.key === 'hdfs' ? (
        // HDFS: Stacked block segments
        <group>
          {[0, 1, 2, 3, 4, 5, 6, 7].map((bIdx) => {
            const blockY = -0.875 + bIdx * 0.25;
            const lit = station.level === 1 || (station.level !== null && station.level >= (bIdx + 1) / 8);
            return (
              <mesh key={bIdx} position={[0, blockY, 0]}>
                <boxGeometry args={[1.4, 0.2, 1.0]} />
                <meshStandardMaterial
                  color={lit ? '#34d399' : vesselColor}
                  transparent
                  opacity={lit ? 0.8 : isPending ? 0.2 : 0.35}
                  emissive={lit ? '#34d399' : '#000000'}
                  emissiveIntensity={lit ? 0.4 : 0}
                />
              </mesh>
            );
          })}
        </group>
      ) : (
        // Source & Kafka: Cylinder vessel
        <mesh>
          <cylinderGeometry args={[0.8, 0.8, 2.2, 24, 1, true]} />
          <meshPhysicalMaterial
            color={vesselColor}
            transparent
            opacity={isPending ? 0.2 : 0.3}
            roughness={0.3}
            metalness={0.1}
          />
        </mesh>
      )}

      {/* Kafka Divider Rings */}
      {station.key === 'kafka' && (
        <group>
          {[-0.5, 0, 0.5].map((ry, i) => (
            <mesh key={i} position={[0, ry, 0]} rotation={[Math.PI / 2, 0, 0]}>
              <torusGeometry args={[0.79, 0.02, 8, 24]} />
              <meshStandardMaterial color={vesselColor} opacity={0.6} transparent />
            </mesh>
          ))}
        </group>
      )}

      {/* Liquid Inner Mesh (Source, Kafka) */}
      {(station.key === 'source' || station.key === 'kafka') && (
        <mesh ref={liquidRef} position={[0, -1.0, 0]}>
          <cylinderGeometry args={[0.72, 0.72, 2.0, 24]} />
          <meshStandardMaterial
            color={baseColor}
            transparent
            opacity={0.7}
            roughness={0.2}
          />
        </mesh>
      )}

      {/* Indeterminate Shimmer Band */}
      <mesh ref={shimmerRef} visible={false}>
        <cylinderGeometry args={[0.74, 0.74, 0.3, 24]} />
        <meshBasicMaterial color="#ffffff" transparent opacity={0.3} />
      </mesh>

      {/* Spark Rotating Gears and Energy Ring */}
      {station.key === 'spark' && (
        <group position={[0, 0, 0.62]}>
          {/* Faint track ring */}
          <mesh>
            <ringGeometry args={[0.7, 0.85, 48]} />
            <meshBasicMaterial color="#ffffff" transparent opacity={0.15} />
          </mesh>
          {/* Active arc */}
          {arcGeo && (
            <mesh ref={arcMeshRef} geometry={arcGeo}>
              <meshStandardMaterial
                color="#a855f7"
                emissive="#c084fc"
                emissiveIntensity={station.working ? 0.8 : 0.2}
                side={THREE.DoubleSide}
              />
            </mesh>
          )}
          {/* Counter-rotating gears */}
          <mesh ref={gear1Ref} position={[-0.4, 0.35, 0.02]}>
            <cylinderGeometry args={[0.22, 0.22, 0.04, 8]} />
            <meshStandardMaterial color="#cbd5e1" metalness={0.8} />
          </mesh>
          <mesh ref={gear2Ref} position={[0.4, 0.35, 0.02]}>
            <cylinderGeometry args={[0.22, 0.22, 0.04, 8]} />
            <meshStandardMaterial color="#cbd5e1" metalness={0.8} />
          </mesh>
          <mesh ref={gear3Ref} position={[0, -0.35, 0.02]}>
            <cylinderGeometry args={[0.22, 0.22, 0.04, 8]} />
            <meshStandardMaterial color="#cbd5e1" metalness={0.8} />
          </mesh>
        </group>
      )}

      {/* Floating HTML Labels */}
      <Html center distanceFactor={8} style={{ pointerEvents: 'none' }}>
        <div className="flex flex-col items-center justify-center text-center select-none whitespace-nowrap min-w-[130px] p-1.5 rounded-lg bg-slate-950/80 backdrop-blur-sm border border-slate-800 shadow-xl">
          <div className="text-xs font-bold text-slate-100">{station.label}</div>
          <div
            className={`text-[10px] font-medium uppercase tracking-wider ${
              isFaulted || isError
                ? 'text-rose-400 font-bold'
                : isDone
                ? 'text-emerald-400'
                : station.state === 'active'
                ? 'text-sky-400'
                : 'text-slate-400'
            }`}
          >
            {station.state}
          </div>
          {station.counter && (
            <div className="text-[10px] font-mono text-slate-300 mt-0.5">
              {station.counter}
            </div>
          )}
          {station.note && (
            <div
              className={`text-[9px] mt-0.5 max-w-[140px] truncate ${
                isFaulted || isError ? 'text-rose-400 font-semibold' : 'text-amber-400'
              }`}
            >
              {station.note}
            </div>
          )}
        </div>
      </Html>
    </group>
  );
}

export const PipelineScene3D: React.FC<PipelineScene3DProps> = ({
  model,
  quality,
  paused,
  onFps,
  onContextLost,
}) => {
  const [controlsRef, setControlsRef] = useState<{ reset: () => void } | null>(null);

  const [isVisible, setIsVisible] = useState(() =>
    typeof document !== 'undefined' ? document.visibilityState !== 'hidden' : true
  );

  useEffect(() => {
    if (typeof document === 'undefined') return;
    const handleVis = () => {
      setIsVisible(document.visibilityState !== 'hidden');
    };
    document.addEventListener('visibilitychange', handleVis);
    return () => document.removeEventListener('visibilitychange', handleVis);
  }, []);

  const isAnyWorking = model.stations.some((s) => s.working);
  const frameloop = !isVisible ? 'never' : paused ? 'demand' : isAnyWorking ? 'always' : 'demand';

  const resetView = useCallback(() => {
    if (controlsRef) {
      controlsRef.reset();
    }
  }, [controlsRef]);

  return (
    <div className="relative w-full h-full min-h-[280px]" data-testid="viz-canvas">
      {/* Reset view button */}
      <button
        type="button"
        onClick={resetView}
        className="absolute top-3 right-3 z-10 px-2.5 py-1 text-xs font-medium text-slate-200 bg-slate-900/80 hover:bg-slate-800 border border-slate-700 rounded shadow-md backdrop-blur-sm transition-colors focus:outline-none focus:ring-1 focus:ring-indigo-500"
      >
        Reset view
      </button>

      <Canvas
        dpr={1}
        frameloop={frameloop}
        camera={{ position: [0, 2.4, 9], fov: 40 }}
        gl={{
          antialias: false,
          powerPreference: 'low-power',
        }}
        onCreated={({ gl }) => {
          const dom = gl.domElement;
          const handleContextLost = (e: Event) => {
            e.preventDefault();
            onContextLost?.();
          };
          dom.addEventListener('webglcontextlost', handleContextLost, false);
        }}
      >
        <CameraController />
        <FpsTracker onFps={onFps} paused={paused} />

        <ambientLight intensity={0.8} />
        <directionalLight position={[5, 8, 5]} intensity={1.2} />
        <directionalLight position={[-5, 4, -3]} intensity={0.4} />

        <OrbitControls
          ref={setControlsRef}
          enablePan={false}
          minDistance={6}
          maxDistance={12}
          minPolarAngle={0.9}
          maxPolarAngle={1.6}
          minAzimuthAngle={-0.7}
          maxAzimuthAngle={0.7}
          enableDamping={!paused}
        />

        {/* 4 Stations */}
        {model.stations.map((station) => (
          <Station3D
            key={station.key}
            station={station}
            position={STATION_POSITIONS[station.key]}
            paused={paused}
          />
        ))}

        {/* Translucent Pipes & Instanced Particles */}
        {model.pipes.map((pipe) => {
          const fromKey = pipe.from;
          const toKey = pipe.to;
          const pFrom = STATION_POSITIONS[fromKey];
          const pTo = STATION_POSITIONS[toKey];
          const midX = (pFrom[0] + pTo[0]) / 2;
          const pipeLength = pTo[0] - pFrom[0] - 1.8;

          return (
            <group key={`pipe-${fromKey}-${toKey}`}>
              {/* Pipe Cylinder */}
              <mesh position={[midX, 0, 0]} rotation={[0, 0, Math.PI / 2]}>
                <cylinderGeometry args={[0.12, 0.12, pipeLength, 16]} />
                <meshStandardMaterial
                  color={pipe.blocked ? '#f59e0b' : '#38bdf8'}
                  transparent
                  opacity={0.3}
                  roughness={0.2}
                />
              </mesh>

              {/* Instanced Particles */}
              <PipeParticles
                pipe={pipe}
                quality={quality}
                paused={paused}
                fromPos={pFrom}
                toPos={pTo}
              />
            </group>
          );
        })}
      </Canvas>
    </div>
  );
};

export default PipelineScene3D;
