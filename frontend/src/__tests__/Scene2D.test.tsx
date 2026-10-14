import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Scene2D } from '../features/viz/Scene2D';
import { SceneModel } from '../features/viz/model';

function makeTestModel(overrides: Partial<SceneModel> = {}): SceneModel {
  return {
    stations: [
      {
        key: 'source',
        label: 'Source',
        state: 'active',
        level: 0.75,
        levelMeaning: 'Rows still to be read',
        counter: '750 / 1,000',
        working: true,
        note: null,
      },
      {
        key: 'kafka',
        label: 'Kafka',
        state: 'active',
        level: null,
        levelMeaning: 'Records published',
        counter: '400',
        working: true,
        note: null,
      },
      {
        key: 'spark',
        label: 'Spark',
        state: 'pending',
        level: null,
        levelMeaning: 'Tasks completed',
        counter: null,
        working: false,
        note: null,
      },
      {
        key: 'hdfs',
        label: 'HDFS',
        state: 'done',
        level: 1,
        levelMeaning: 'Output verified',
        counter: '3 files, 1 MB',
        working: false,
        note: null,
      },
    ],
    pipes: [
      { from: 'source', to: 'kafka', flow: 0.8, blocked: false },
      { from: 'kafka', to: 'spark', flow: 0, blocked: false },
      { from: 'spark', to: 'hdfs', flow: 0, blocked: false },
    ],
    completion: 'running',
    announce: 'Source processing started.',
    summary: ['Source: active.', 'Kafka: active.', 'Spark: pending.', 'HDFS: done.'],
    ...overrides,
  };
}

describe('Scene2D Component', () => {
  it('renders SVG canvas with data-testid="viz-canvas"', () => {
    const model = makeTestModel();
    render(<Scene2D model={model} />);
    expect(screen.getByTestId('viz-canvas')).toBeInTheDocument();
  });

  it('sets data-level attribute correctly on fill elements for numeric and null levels', () => {
    const model = makeTestModel();
    render(<Scene2D model={model} />);

    const sourceFill = screen.getByTestId('fill-source');
    expect(sourceFill.getAttribute('data-level')).toBe('0.75');

    const kafkaFill = screen.getByTestId('fill-kafka');
    expect(kafkaFill.getAttribute('data-level')).toBe('indeterminate');

    const hdfsFill = screen.getByTestId('fill-hdfs');
    expect(hdfsFill.getAttribute('data-level')).toBe('1');
  });

  it('renders shimmer rect when level is null and station is working', () => {
    const model = makeTestModel();
    render(<Scene2D model={model} />);

    // Kafka is working with level null -> shimmer should exist
    expect(screen.getByTestId('shimmer-kafka')).toBeInTheDocument();

    // Spark is NOT working -> shimmer should NOT exist
    expect(screen.queryByTestId('shimmer-spark')).not.toBeInTheDocument();
  });

  it('renders state text and labels for each station', () => {
    const model = makeTestModel();
    render(<Scene2D model={model} />);

    expect(screen.getByText('Source')).toBeInTheDocument();
    expect(screen.getByText('Kafka')).toBeInTheDocument();
    expect(screen.getByText('Spark')).toBeInTheDocument();
    expect(screen.getByText('HDFS')).toBeInTheDocument();

    expect(screen.getAllByText('active').length).toBe(2);
    expect(screen.getByText('pending')).toBeInTheDocument();
    expect(screen.getByText('done')).toBeInTheDocument();
  });

  it('renders blocked pipe marker when pipe is blocked', () => {
    const model = makeTestModel({
      pipes: [
        { from: 'source', to: 'kafka', flow: 0, blocked: true },
        { from: 'kafka', to: 'spark', flow: 0, blocked: false },
        { from: 'spark', to: 'hdfs', flow: 0, blocked: false },
      ],
    });
    render(<Scene2D model={model} />);

    expect(screen.getByTestId('pipe-blocked-source-kafka')).toBeInTheDocument();
    expect(screen.getByText('BLOCKED')).toBeInTheDocument();
  });

  it('renders STOPPED ribbon when station is faulted', () => {
    const model = makeTestModel({
      stations: [
        {
          key: 'source',
          label: 'Source',
          state: 'faulted',
          level: null,
          levelMeaning: 'Rows still to be read',
          counter: null,
          working: false,
          note: 'STOPPED (fault: container_stop)',
        },
        ...makeTestModel().stations.slice(1),
      ],
    });
    render(<Scene2D model={model} />);

    expect(screen.getByTestId('stopped-ribbon-source')).toBeInTheDocument();
    expect(screen.getByText('STOPPED')).toBeInTheDocument();
  });

  it('applies viz-paused container class and disables animation when paused', () => {
    const model = makeTestModel();
    const { container } = render(<Scene2D model={model} paused={true} />);

    expect(container.querySelector('.viz-paused')).toBeInTheDocument();

    const shimmer = screen.getByTestId('shimmer-kafka');
    expect(shimmer.getAttribute('class')).toBe('');
  });
});
