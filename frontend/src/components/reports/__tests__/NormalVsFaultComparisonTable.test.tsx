import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { NormalVsFaultComparisonTable } from '../NormalVsFaultComparisonTable';
import { ComparisonTableRow } from '../../types/experiment';

describe('NormalVsFaultComparisonTable', () => {
  const sampleRows: ComparisonTableRow[] = [
    {
      metric: 'availability_percentage',
      label: 'Service Availability',
      unit: '%',
      normal: 100.0,
      during_fault: 95.0,
      delta: -5.0,
      pct_change: -5.0,
      baseline_applicable: true,
      measured_directly: true,
      definition: 'Percentage of probes that succeeded during the monitoring window.',
      aggregate: {
        mean: 95.0,
        std: 0.0,
        min: 95.0,
        max: 95.0,
        n_samples: 2,
      },
    },
    {
      metric: 'throughput_records_per_sec',
      label: 'Throughput',
      unit: 'records/s',
      normal: 1000.0,
      during_fault: 200.0,
      delta: -800.0,
      pct_change: -80.0,
      baseline_applicable: true,
      measured_directly: true,
      definition: 'Average rate of records processed per second during the fault window.',
      aggregate: null,
    },
    {
      metric: 'data_loss',
      label: 'Data Loss',
      unit: 'records',
      normal: 0,
      during_fault: 50,
      delta: 50.0,
      pct_change: null, // zero baseline division
      baseline_applicable: true,
      measured_directly: true,
      definition: 'Count of records lost between verified pipeline output and expected rows.',
      aggregate: null,
    },
    {
      metric: 'recovery_time_seconds',
      label: 'Recovery Time',
      unit: 'seconds',
      normal: null,
      during_fault: 12.4,
      delta: null,
      pct_change: null,
      baseline_applicable: false,
      measured_directly: true,
      definition: 'Elapsed duration from fault revert until service health check passed.',
      aggregate: {
        mean: 12.4,
        std: 1.2,
        min: 11.2,
        max: 13.6,
        n_samples: 2,
      },
    },
    {
      metric: 'duplicates',
      label: 'Duplicate Records',
      unit: 'records',
      normal: null,
      during_fault: null,
      delta: null,
      pct_change: null,
      baseline_applicable: false,
      measured_directly: false,
      definition: 'Count of unexpected duplicate records observed.',
      aggregate: null,
    },
  ];

  it('renders nothing when rows is empty or undefined', () => {
    const { container } = render(<NormalVsFaultComparisonTable rows={[]} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders comparison table with rows and proper labels', () => {
    render(<NormalVsFaultComparisonTable rows={sampleRows} repetitions={1} />);
    expect(screen.getByTestId('report-comparison-table')).toBeInTheDocument();
    expect(screen.getByText('Normal vs During-Fault Comparison')).toBeInTheDocument();

    // Check headers
    expect(screen.getByText('Metric')).toBeInTheDocument();
    expect(screen.getByText('Normal (Baseline)')).toBeInTheDocument();
    expect(screen.getByText('During Fault')).toBeInTheDocument();
    expect(screen.getByText('Delta (Impact)')).toBeInTheDocument();
    expect(screen.getByText('Definition')).toBeInTheDocument();

    // Check metric labels
    expect(screen.getByText('Service Availability')).toBeInTheDocument();
    expect(screen.getByText('Throughput')).toBeInTheDocument();
    expect(screen.getByText('Data Loss')).toBeInTheDocument();
    expect(screen.getByText('Recovery Time')).toBeInTheDocument();
    expect(screen.getByText('Duplicate Records')).toBeInTheDocument();
  });

  it('displays "n/a (by definition)" when baseline_applicable is false', () => {
    render(<NormalVsFaultComparisonTable rows={sampleRows} repetitions={1} />);
    const naByDefElements = screen.getAllByText('n/a (by definition)');
    expect(naByDefElements.length).toBeGreaterThan(0);
  });

  it('displays "not measured" for null values and never fake numbers', () => {
    render(<NormalVsFaultComparisonTable rows={sampleRows} repetitions={1} />);
    const notMeasuredElements = screen.getAllByText('not measured');
    expect(notMeasuredElements.length).toBeGreaterThan(0);
  });

  it('renders repetition statistics column and badge when repetitions > 1', () => {
    render(<NormalVsFaultComparisonTable rows={sampleRows} repetitions={2} />);
    expect(screen.getByText('Aggregated across 2 repetitions')).toBeInTheDocument();
    expect(screen.getByText('Repetition Statistics (Mean ± Std)')).toBeInTheDocument();
    expect(screen.getByText(/μ = 95.0 ± 0.0/)).toBeInTheDocument();
    expect(screen.getByText(/μ = 12.4 ± 1.2/)).toBeInTheDocument();
  });
});
