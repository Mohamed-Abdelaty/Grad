import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import { axe } from 'vitest-axe';

import PipelineManagement from '../pages/PipelineManagement';
import * as pipelines from '../api/pipelines';
import { PipelineTemplate } from '../types/pipeline';

import { YamlEditorHandle, YamlEditorProps } from '../components/pipelines/YamlEditor';

// Mock CodeMirror YamlEditor for jsdom testing
const mockFocusLine = vi.fn();
vi.mock('../components/pipelines/YamlEditor', () => {
  return {
    default: React.forwardRef<YamlEditorHandle, YamlEditorProps>(({ value, onChange, placeholder }, ref) => {
      React.useImperativeHandle(ref, () => ({
        focusLine: mockFocusLine,
        focus: vi.fn(),
      }));
      return (
        <textarea
          data-testid="mock-yaml-editor"
          value={value}
          onChange={(e) => onChange?.(e.target.value)}
          placeholder={placeholder}
        />
      );
    }),
  };
});

vi.mock('../api/pipelines');

const mockTemplates: PipelineTemplate[] = [
  {
    id: 'sales-csv',
    name: 'Sales CSV Processing',
    description: 'Process batch sales transactions from CSV with deduplication',
    yaml: 'pipeline:\n  name: sales-csv\n  version: "1.0"\n',
  },
  {
    id: 'iot-json',
    name: 'IoT Sensor Stream',
    description: 'Process streaming temperature sensor readings from JSON',
    yaml: 'pipeline:\n  name: iot-json\n  version: "1.0"\n',
  },
  {
    id: 'network-logs',
    name: 'Network Log Analysis',
    description: 'Analyze network log traffic and error occurrences',
    yaml: 'pipeline:\n  name: network-logs\n  version: "1.0"\n',
  },
];

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/pipelines']}>
        <PipelineManagement />
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('PipelineManagement Component', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockFocusLine.mockReset();
    vi.mocked(pipelines.listTemplates).mockResolvedValue(mockTemplates);
    vi.mocked(pipelines.listRuns).mockResolvedValue({ items: [], total: 0 });
  });

  it('verifies toolbar logic: Run disabled until validated, disabled again after edit, ignores stale validation', async () => {
    renderPage();

    const runBtn = screen.getByTestId('btn-run');
    const validateBtn = screen.getByTestId('btn-validate');

    // Initially Run is disabled
    expect(runBtn).toBeDisabled();

    // Mock validation responses
    let resolveFirst: (val: pipelines.ValidationResult) => void;
    const firstValidationPromise = new Promise<pipelines.ValidationResult>((resolve) => {
      resolveFirst = resolve;
    });

    vi.mocked(pipelines.validatePipeline).mockReturnValueOnce(firstValidationPromise);

    // Type content in editor
    const editor = await screen.findByTestId('mock-yaml-editor');
    fireEvent.change(editor, { target: { value: 'pipeline:\n  name: test-1\n' } });

    // Click validate
    fireEvent.click(validateBtn);

    // Now type new content before the first validation resolves
    fireEvent.change(editor, { target: { value: 'pipeline:\n  name: test-2\n' } });

    // First validation resolves as valid, but it is stale!
    resolveFirst!({ valid: true, errors: [] });
    await waitFor(() => {
      // Because text changed, Run must still be disabled
      expect(runBtn).toBeDisabled();
    });

    // Now validate the current text
    vi.mocked(pipelines.validatePipeline).mockResolvedValueOnce({ valid: true, errors: [] });
    fireEvent.click(validateBtn);

    await waitFor(() => {
      expect(runBtn).not.toBeDisabled();
    });

    // Another edit immediately disables Run again
    fireEvent.change(editor, { target: { value: 'pipeline:\n  name: test-3\n' } });
    expect(runBtn).toBeDisabled();
  });

  it('renders four validation panel states with exact text', async () => {
    renderPage();

    // 1. Not validated (idle)
    const stateEl = screen.getByTestId('validation-state');
    expect(stateEl).toHaveTextContent('Not validated');

    // 2. Validating...
    let resolveValidation: (val: pipelines.ValidationResult) => void;
    vi.mocked(pipelines.validatePipeline).mockReturnValue(
      new Promise<pipelines.ValidationResult>((resolve) => {
        resolveValidation = resolve;
      })
    );

    const editor = await screen.findByTestId('mock-yaml-editor');
    fireEvent.change(editor, { target: { value: 'pipeline:\n  name: sample\n' } });

    const validateBtn = screen.getByTestId('btn-validate');
    fireEvent.click(validateBtn);

    expect(stateEl).toHaveTextContent('Validating...');

    // 3. Valid
    resolveValidation!({ valid: true, errors: [] });
    await waitFor(() => {
      expect(stateEl).toHaveTextContent('Valid');
      expect(screen.getByText('Ready to run')).toBeInTheDocument();
    });

    // 4. Invalid (N issues)
    vi.mocked(pipelines.validatePipeline).mockResolvedValueOnce({
      valid: false,
      errors: [
        { field: 'storage.path', message: 'Missing required storage path' },
        { field: 'source.type', message: 'Invalid source type' },
      ],
    });

    fireEvent.click(validateBtn);

    await waitFor(() => {
      expect(stateEl).toHaveTextContent('Invalid (2 issues)');
    });

    const issues = screen.getAllByTestId('issue-item');
    expect(issues.length).toBe(2);
    expect(issues[0]).toHaveTextContent('storage.path');
    expect(issues[0]).toHaveTextContent('Missing required storage path');

    // Click "Go to line" on the first issue
    const goToLineButtons = screen.getAllByRole('button', { name: /go to line/i });
    fireEvent.click(goToLineButtons[0]);
    expect(mockFocusLine).toHaveBeenCalled();
  });

  it('handles templates modal: lists three templates and asks confirmation when unsaved edits exist', async () => {
    renderPage();

    const templatesBtn = screen.getByTestId('btn-templates');
    fireEvent.click(templatesBtn);

    // Modal opens and lists all 3 templates
    expect(await screen.findByText('Sales CSV Processing')).toBeInTheDocument();
    expect(screen.getByText('IoT Sensor Stream')).toBeInTheDocument();
    expect(screen.getByText('Network Log Analysis')).toBeInTheDocument();

    // Select the first template when no edits exist -> loads directly without confirm dialog
    const templateCard = screen.getByText('Sales CSV Processing');
    fireEvent.click(templateCard);

    const editor = (await screen.findByTestId('mock-yaml-editor')) as HTMLTextAreaElement;
    expect(editor.value).toContain('sales-csv');

    // Now edit the content (mark as unsaved)
    fireEvent.change(editor, { target: { value: editor.value + '# user modification\n' } });

    // Open templates modal again and select IoT template
    fireEvent.click(templatesBtn);
    const iotCard = await screen.findByText('IoT Sensor Stream');
    fireEvent.click(iotCard);

    // Confirm dialog appears
    expect(await screen.findByText('Replace current content?')).toBeInTheDocument();

    // Click Confirm/Replace in dialog
    const replaceBtn = screen.getByRole('button', { name: 'Replace' });
    fireEvent.click(replaceBtn);

    await waitFor(() => {
      expect(editor.value).toContain('iot-json');
    });
  });

  it('has zero accessibility violations on page shell (with color-contrast disabled in jsdom)', async () => {
    const { container } = renderPage();
    await screen.findByTestId('btn-templates');

    const results = await axe(container, {
      rules: { 'color-contrast': { enabled: false } },
    });
    expect(results).toHaveNoViolations();
  });
});
