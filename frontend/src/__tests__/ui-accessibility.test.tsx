/**
 * Accessibility smoke tests for the components/ui/ library.
 *
 * Each test renders the component into a real DOM (jsdom), runs axe-core against
 * the rendered HTML, and asserts zero accessibility violations. This catches
 * missing ARIA roles, broken label associations, colour-contrast failures, and
 * keyboard focus issues.
 *
 * These run on every CI pass. They do not replace manual screen-reader testing
 * but ensure regressions are caught immediately.
 */
import React from 'react';
import { render } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { axe } from 'vitest-axe';
import { MemoryRouter } from 'react-router-dom';

import { Button } from '../components/ui/Button';
import { IconButton } from '../components/ui/IconButton';
import { Card, CardHeader, CardTitle } from '../components/ui/Card';
import { StatusPill } from '../components/ui/StatusPill';
import { StatCard } from '../components/ui/StatCard';
import { Skeleton, SkeletonCard } from '../components/ui/Skeleton';
import { EmptyState } from '../components/ui/EmptyState';
import { ErrorState } from '../components/ui/ErrorState';
import { Tabs, TabList, Tab, TabPanel } from '../components/ui/Tabs';
import { Modal } from '../components/ui/Modal';
import { ProgressBar } from '../components/ui/ProgressBar';
import { LogViewer } from '../components/ui/LogViewer';
import { FormField } from '../components/ui/FormField';
import { DataTable } from '../components/ui/DataTable';

const DummyIcon = () => (
  <svg aria-hidden="true" viewBox="0 0 24 24" width="16" height="16">
    <circle cx="12" cy="12" r="10" />
  </svg>
);

function wrap(ui: React.ReactElement) {
  const { container } = render(<MemoryRouter>{ui}</MemoryRouter>);
  return container;
}

// --------------------------------------------------------------------------
describe('Accessibility: Button', () => {
  it('has no violations — primary enabled', async () => {
    expect(await axe(wrap(<Button>Submit</Button>))).toHaveNoViolations();
  });
  it('has no violations — loading', async () => {
    expect(await axe(wrap(<Button loading>Saving…</Button>))).toHaveNoViolations();
  });
  it('has no violations — disabled', async () => {
    expect(await axe(wrap(<Button disabled>Disabled</Button>))).toHaveNoViolations();
  });
});

describe('Accessibility: IconButton', () => {
  it('has no violations', async () => {
    expect(await axe(wrap(
      <IconButton label="Close dialog"><DummyIcon /></IconButton>
    ))).toHaveNoViolations();
  });
});

describe('Accessibility: Card', () => {
  it('has no violations', async () => {
    expect(await axe(wrap(
      <Card>
        <CardHeader><CardTitle>Runs overview</CardTitle></CardHeader>
        <p>Content goes here.</p>
      </Card>
    ))).toHaveNoViolations();
  });
});

describe('Accessibility: StatusPill', () => {
  (['running', 'succeeded', 'failed', 'cancelled', 'pending'] as const).forEach((s) => {
    it(`has no violations — ${s}`, async () => {
      expect(await axe(wrap(<StatusPill status={s} />))).toHaveNoViolations();
    });
  });
});

describe('Accessibility: StatCard', () => {
  it('has no violations — with value', async () => {
    expect(await axe(wrap(
      <StatCard label="Total runs" value={42} sub="Last 7 days" />
    ))).toHaveNoViolations();
  });
  it('has no violations — null value (em-dash)', async () => {
    expect(await axe(wrap(<StatCard label="Total runs" value={null} />))).toHaveNoViolations();
  });
});

describe('Accessibility: Skeleton', () => {
  it('Skeleton has no violations', async () => {
    expect(await axe(wrap(<Skeleton className="h-6 w-32" />))).toHaveNoViolations();
  });
  it('SkeletonCard has no violations', async () => {
    expect(await axe(wrap(<SkeletonCard />))).toHaveNoViolations();
  });
});

describe('Accessibility: EmptyState', () => {
  it('has no violations', async () => {
    expect(await axe(wrap(
      <EmptyState title="Nothing here" description="Add something to get started." />
    ))).toHaveNoViolations();
  });
});

describe('Accessibility: ErrorState', () => {
  it('has no violations', async () => {
    expect(await axe(wrap(
      <ErrorState title="Something went wrong" onRetry={() => undefined} />
    ))).toHaveNoViolations();
  });
});

describe('Accessibility: Tabs', () => {
  it('has no violations — WAI-ARIA tabs pattern', async () => {
    expect(await axe(wrap(
      <Tabs defaultTab="a">
        <TabList>
          <Tab id="a" label="Tab A" />
          <Tab id="b" label="Tab B" />
        </TabList>
        <TabPanel id="a"><p>Panel A content</p></TabPanel>
        <TabPanel id="b"><p>Panel B content</p></TabPanel>
      </Tabs>
    ))).toHaveNoViolations();
  });
});

describe('Accessibility: Modal', () => {
  it('has no violations when open', async () => {
    expect(await axe(wrap(
      <Modal open onClose={() => undefined} title="Confirm action">
        <p>Are you sure you want to proceed?</p>
      </Modal>
    ))).toHaveNoViolations();
  });
});

describe('Accessibility: ProgressBar', () => {
  it('has no violations — determinate', async () => {
    expect(await axe(wrap(<ProgressBar value={60} label="Uploading" />))).toHaveNoViolations();
  });
  it('has no violations — indeterminate (null)', async () => {
    // null signals indeterminate mode to the component.
    expect(await axe(wrap(<ProgressBar value={null} label="Processing" />))).toHaveNoViolations();
  });
});

describe('Accessibility: LogViewer', () => {
  it('has no violations — with lines', async () => {
    expect(await axe(wrap(
      <LogViewer lines={['Starting…', 'Done.']} label="Execution log" />
    ))).toHaveNoViolations();
  });
  it('has no violations — empty', async () => {
    expect(await axe(wrap(<LogViewer lines={[]} label="Execution log" />))).toHaveNoViolations();
  });
});

describe('Accessibility: FormField', () => {
  it('has no violations — normal state', async () => {
    expect(await axe(wrap(
      <FormField label="Username" required><input type="text" /></FormField>
    ))).toHaveNoViolations();
  });
  it('has no violations — error state', async () => {
    expect(await axe(wrap(
      <FormField label="Email" error="Enter a valid email address.">
        <input type="email" />
      </FormField>
    ))).toHaveNoViolations();
  });
  it('has no violations — hint text', async () => {
    expect(await axe(wrap(
      <FormField label="API key" hint="Find this in your account settings.">
        <input type="text" />
      </FormField>
    ))).toHaveNoViolations();
  });
});

describe('Accessibility: DataTable', () => {
  interface Row { id: string; name: string; count: number }
  const columns = [
    { key: 'name', header: 'Name',  cell: (r: Row) => r.name  },
    { key: 'count', header: 'Count', cell: (r: Row) => r.count },
  ];
  const rows: Row[] = [
    { id: '1', name: 'Alpha', count: 10 },
    { id: '2', name: 'Beta',  count: 20 },
  ];

  it('has no violations — populated', async () => {
    expect(await axe(wrap(
      <DataTable<Row> columns={columns} rows={rows} rowKey={(r) => r.id} />
    ))).toHaveNoViolations();
  });
  it('has no violations — empty', async () => {
    expect(await axe(wrap(
      <DataTable<Row> columns={columns} rows={[]} rowKey={(r) => r.id} />
    ))).toHaveNoViolations();
  });
});
