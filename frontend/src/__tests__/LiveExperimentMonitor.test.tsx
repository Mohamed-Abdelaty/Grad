import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import LiveExperimentMonitor from '../components/experiments/LiveExperimentMonitor';

vi.mock('../api/experiments', () => ({
  getExperiment: vi.fn(),
  cancelExperiment: vi.fn(),
}));

describe('LiveExperimentMonitor', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders loading state initially before data arrives', () => {
    render(<LiveExperimentMonitor experimentId="123" onComplete={() => {}} />);
    expect(screen.getByText(/Loading monitor/i)).toBeInTheDocument();
  });
});
