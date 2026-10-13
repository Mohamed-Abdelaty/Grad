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
