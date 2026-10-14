import '@testing-library/jest-dom';
import * as axeMatchers from 'vitest-axe/matchers';
import { expect } from 'vitest';
expect.extend(axeMatchers);

// jsdom does not implement scrollIntoView — mock it so LogViewer (and any
// component that calls element.scrollIntoView) doesn't throw in tests.
window.HTMLElement.prototype.scrollIntoView = () => undefined;

if (typeof window !== 'undefined' && !window.ResizeObserver) {
  window.ResizeObserver = class ResizeObserver {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
}

// Mock @react-three/fiber and @react-three/drei for jsdom tests
vi.mock('@react-three/fiber', () => ({
  Canvas: ({ children, ...props }: { children?: React.ReactNode; [key: string]: unknown }) => {
    return {
      $$typeof: Symbol.for('react.element'),
      type: 'div',
      key: null,
      ref: null,
      props: { 'data-testid': 'mock-canvas', ...props, children },
    };
  },
  useFrame: vi.fn(),
  useThree: () => ({
    camera: { position: { x: 0, y: 2.4, z: 9 }, updateProjectionMatrix: vi.fn() },
    size: { width: 1000, height: 300 },
    gl: { domElement: document.createElement('canvas') },
    invalidate: vi.fn(),
  }),
}));

vi.mock('@react-three/drei', () => ({
  OrbitControls: () => null,
  Html: ({ children }: { children?: React.ReactNode }) => {
    return {
      $$typeof: Symbol.for('react.element'),
      type: 'div',
      key: null,
      ref: null,
      props: { children },
    };
  },
  PerformanceMonitor: () => null,
}));

