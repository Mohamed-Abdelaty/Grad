import React, { useState, useEffect, useRef } from 'react';
import { toast } from 'sonner';
import {
  AlertTriangle,
  ArrowLeft,
  ArrowRight,
  Check,
  Copy,
  Flame,
  Info,
  Play,
  ShieldAlert,
  Zap,
} from 'lucide-react';
import { ExperimentOptions, TargetOption, FaultOption } from '../../types/experiment';
import { createExperiment, runExperiment } from '../../api/experiments';
import {
  ExperimentFormState,
  buildExperimentYaml,
  describeExperiment,
} from '../../lib/reliability';

interface Props {
  options: ExperimentOptions;
  runningExperimentName?: string | null;
  onExperimentStarted: (experimentId: string) => void;
}

export function ExperimentWizard({
  options,
  runningExperimentName,
  onExperimentStarted,
}: Props) {
  const [currentStep, setCurrentStep] = useState<number>(1);
  const [selectedTarget, setSelectedTarget] = useState<string>('');
  const [selectedFault, setSelectedFault] = useState<string>('');
  const [duration, setDuration] = useState<number>(20);
  const [delayMs, setDelayMs] = useState<number>(500);
  const [expectedLoss, setExpectedLoss] = useState<number>(0);
  const [maxRecovery, setMaxRecovery] = useState<number>(30);
  const [name, setName] = useState<string>('');
  const [confirmed, setConfirmed] = useState<boolean>(false);

  const [copied, setCopied] = useState<boolean>(false);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  const [serverError, setServerError] = useState<string | null>(null);

  const targetsRef = useRef<HTMLDivElement>(null);

  // Initialize defaults once options are loaded
  useEffect(() => {
    if (options.targets.length > 0 && !selectedTarget) {
      setSelectedTarget(options.targets[0].id);
    }
    if (options.faults.length > 0 && !selectedFault) {
      setSelectedFault(options.faults[0].id);
    }
  }, [options, selectedTarget, selectedFault]);

  // Update default name when target/fault change
  useEffect(() => {
    if (selectedTarget && selectedFault) {
      setName(`${selectedTarget}-${selectedFault}-${Date.now().toString().slice(-6)}`);
    }
  }, [selectedTarget, selectedFault]);

  const limits = options.limits || { minDurationSeconds: 1, maxDurationSeconds: 300 };
  const currentFaultObj = options.faults.find((f) => f.id === selectedFault);
  const hasDelayParam = currentFaultObj?.params?.some((p) => p.name === 'delay_ms');

  const formState: ExperimentFormState = {
    name,
    targetId: selectedTarget,
    faultId: selectedFault,
    duration,
    params: hasDelayParam ? { delay_ms: delayMs } : {},
    expectedDataLoss: expectedLoss,
    maxRecoverySeconds: maxRecovery,
  };

  const yamlContent = buildExperimentYaml(formState);
  const reviewSentence = describeExperiment(formState, options);

  // Step validity checks
  const isNameValid = /^[a-zA-Z0-9_\-]+$/.test(name.trim());
  const isDurationValid = duration >= limits.minDurationSeconds && duration <= limits.maxDurationSeconds;
  const isLossValid = Number.isInteger(expectedLoss) && expectedLoss >= 0;
  const isRecoveryValid = Number.isInteger(maxRecovery) && maxRecovery >= 1;
  const isDelayValid = !hasDelayParam || (delayMs >= 1 && delayMs <= 10000);

  const isStep1Valid = Boolean(selectedTarget);
  const isStep2Valid = Boolean(selectedFault);
  const isStep3Valid = isNameValid && isDurationValid && isLossValid && isRecoveryValid && isDelayValid;
  const isStep4Valid = confirmed && !runningExperimentName && !isSubmitting;

  const canProceed =
    (currentStep === 1 && isStep1Valid) ||
    (currentStep === 2 && isStep2Valid) ||
    (currentStep === 3 && isStep3Valid);

  // Keyboard navigation for Step 1 target radiogroup
  const handleTargetKeyDown = (e: React.KeyboardEvent, index: number) => {
    if (e.key === 'ArrowDown' || e.key === 'ArrowRight') {
      e.preventDefault();
      const nextIdx = (index + 1) % options.targets.length;
      setSelectedTarget(options.targets[nextIdx].id);
    } else if (e.key === 'ArrowUp' || e.key === 'ArrowLeft') {
      e.preventDefault();
      const prevIdx = (index - 1 + options.targets.length) % options.targets.length;
      setSelectedTarget(options.targets[prevIdx].id);
    }
  };

  const handleCopyYaml = () => {
    navigator.clipboard.writeText(yamlContent);
    setCopied(true);
    toast.success('YAML configuration copied to clipboard');
    setTimeout(() => setCopied(false), 2000);
  };

  const handleStartExperiment = async () => {
    if (!isStep4Valid) return;
    setIsSubmitting(true);
    setServerError(null);

    try {
      const res = await createExperiment(yamlContent);
      await runExperiment(res.experiment_id);
      toast.success(`Experiment "${name}" initiated successfully`);
      onExperimentStarted(res.experiment_id);

      // Reset wizard
      setCurrentStep(1);
      setConfirmed(false);
      if (options.targets[0]) setSelectedTarget(options.targets[0].id);
      if (options.faults[0]) setSelectedFault(options.faults[0].id);
    } catch (err: any) {
      const msg = err.response?.data?.detail || err.message || 'Failed to start experiment';
      setServerError(String(msg));
      toast.error(`Experiment failed: ${msg}`);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div
      data-testid="exp-wizard"
      className="bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl flex flex-col gap-6"
    >
      {/* Header and Stepper Navigation */}
      <div className="flex flex-col gap-4 border-b border-slate-800 pb-5">
        <div className="flex items-center justify-between">
          <h2 className="text-xl font-semibold text-slate-100 flex items-center gap-2">
            <Flame className="w-5 h-5 text-amber-500" />
            Reliability Experiment Wizard
          </h2>
          <span className="text-xs text-slate-400 font-mono">Step {currentStep} of 4</span>
        </div>

        {/* Step Buttons */}
        <div className="grid grid-cols-4 gap-2">
          {[
            { n: 1, label: 'Target' },
            { n: 2, label: 'Fault' },
            { n: 3, label: 'Parameters' },
            { n: 4, label: 'Review' },
          ].map((s) => {
            const isActive = currentStep === s.n;
            const isCompleted = currentStep > s.n;
            return (
              <button
                key={s.n}
                type="button"
                data-testid={`exp-step-${s.n}`}
                onClick={() => {
                  if (s.n < currentStep || (s.n === 2 && isStep1Valid) || (s.n === 3 && isStep1Valid && isStep2Valid)) {
                    setCurrentStep(s.n);
                  }
                }}
                className={`py-2 px-3 rounded-lg text-xs font-medium transition-all text-left flex items-center gap-2 ${
                  isActive
                    ? 'bg-brand/20 border border-brand text-brand-light'
                    : isCompleted
                    ? 'bg-slate-800/80 border border-slate-700 text-slate-300 hover:bg-slate-800'
                    : 'bg-slate-900 border border-slate-800 text-slate-500 cursor-not-allowed'
                }`}
              >
                <span
                  className={`w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-bold shrink-0 ${
                    isActive
                      ? 'bg-brand text-white'
                      : isCompleted
                      ? 'bg-emerald-600 text-white'
                      : 'bg-slate-800 text-slate-500'
                  }`}
                >
                  {isCompleted ? <Check className="w-3 h-3" /> : s.n}
                </span>
                <span className="truncate">{s.label}</span>
              </button>
            );
          })}
        </div>
      </div>

      {/* Step 1: Target Selection */}
      {currentStep === 1 && (
        <div className="flex flex-col gap-4">
          <div>
            <h3 className="text-sm font-medium text-slate-200">1. Select Target Service</h3>
            <p className="text-xs text-slate-400 mt-0.5">
              Choose the pipeline or infrastructure component to inject fault into.
            </p>
          </div>

          <div
            ref={targetsRef}
            role="radiogroup"
            aria-label="Target Service Selection"
            className="grid grid-cols-1 md:grid-cols-2 gap-3"
          >
            {options.targets.map((t, idx) => {
              const isSelected = selectedTarget === t.id;
              const riskColor =
                t.risk === 'high'
                  ? 'bg-rose-950/60 border-rose-800/60 text-rose-400'
                  : t.risk === 'medium'
                  ? 'bg-amber-950/60 border-amber-800/60 text-amber-400'
                  : 'bg-emerald-950/60 border-emerald-800/60 text-emerald-400';

              return (
                <div
                  key={t.id}
                  data-testid={`target-${t.id}`}
                  role="radio"
                  aria-checked={isSelected}
                  tabIndex={isSelected ? 0 : -1}
                  onClick={() => setSelectedTarget(t.id)}
                  onKeyDown={(e) => handleTargetKeyDown(e, idx)}
                  className={`p-4 rounded-xl border cursor-pointer transition-all flex flex-col justify-between gap-3 focus:outline-none focus:ring-2 focus:ring-brand ${
                    isSelected
                      ? 'bg-brand/10 border-brand shadow-md'
                      : 'bg-slate-800/40 border-slate-700/60 hover:bg-slate-800/80 hover:border-slate-600'
                  }`}
                >
                  <div className="flex items-start justify-between gap-2">
                    <span className="font-semibold text-slate-100 text-sm">{t.label}</span>
                    <span
                      className={`px-2 py-0.5 rounded text-[10px] font-semibold uppercase tracking-wider border flex items-center gap-1 ${riskColor}`}
                    >
                      <ShieldAlert className="w-3 h-3" />
                      {t.risk}
                    </span>
                  </div>
                  <p className="text-xs text-slate-400 line-clamp-2">{t.description}</p>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Step 2: Fault Selection */}
      {currentStep === 2 && (
        <div className="flex flex-col gap-4">
          <div>
            <h3 className="text-sm font-medium text-slate-200">2. Select Fault Type</h3>
            <p className="text-xs text-slate-400 mt-0.5">
              Select the failure mode to simulate against the target service.
            </p>
          </div>

          <div role="radiogroup" aria-label="Fault Type Selection" className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {options.faults.map((f) => {
              const isSelected = selectedFault === f.id;
              return (
                <div
                  key={f.id}
                  data-testid={`fault-${f.id}`}
                  role="radio"
                  aria-checked={isSelected}
                  tabIndex={0}
                  onClick={() => setSelectedFault(f.id)}
                  onKeyDown={(e) => {
                    if (e.key === ' ' || e.key === 'Enter') {
                      e.preventDefault();
                      setSelectedFault(f.id);
                    }
                  }}
                  className={`p-4 rounded-xl border cursor-pointer transition-all flex flex-col justify-between gap-3 focus:outline-none focus:ring-2 focus:ring-brand ${
                    isSelected
                      ? 'bg-brand/10 border-brand shadow-md'
                      : 'bg-slate-800/40 border-slate-700/60 hover:bg-slate-800/80 hover:border-slate-600'
                  }`}
                >
                  <div className="flex items-center gap-2">
                    <Zap className="w-4 h-4 text-brand-light" />
                    <span className="font-semibold text-slate-100 text-sm">{f.label}</span>
                  </div>
                  <p className="text-xs text-slate-400">{f.description}</p>
                  <div className="bg-slate-900/80 p-2.5 rounded-lg border border-slate-800 text-[11px] text-slate-400 flex items-start gap-1.5">
                    <AlertTriangle className="w-3.5 h-3.5 text-amber-500 shrink-0 mt-0.5" />
                    <span>
                      <strong className="text-slate-300">Blast Radius:</strong> {f.blastRadius}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Step 3: Parameters Configuration */}
      {currentStep === 3 && (
        <div className="flex flex-col gap-5">
          <div>
            <h3 className="text-sm font-medium text-slate-200">3. Configure Parameters & Thresholds</h3>
            <p className="text-xs text-slate-400 mt-0.5">
              Set fault holding duration and recovery pass/fail criteria.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Experiment Name */}
            <div className="md:col-span-2 flex flex-col gap-1.5">
              <label htmlFor="exp-name" className="text-xs font-medium text-slate-300">
                Experiment Name
              </label>
              <input
                id="exp-name"
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                className={`bg-slate-800 border rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:ring-2 focus:ring-brand font-mono ${
                  !isNameValid ? 'border-rose-500' : 'border-slate-700'
                }`}
              />
              {!isNameValid && (
                <p className="text-[11px] text-rose-400">
                  Name must contain only letters, numbers, hyphens, and underscores.
                </p>
              )}
            </div>

            {/* Duration Slider and Number Input */}
            <div className="md:col-span-2 bg-slate-800/40 border border-slate-700/60 p-4 rounded-xl flex flex-col gap-3">
              <div className="flex items-center justify-between">
                <label htmlFor="exp-duration-num" className="text-xs font-semibold text-slate-200">
                  Fault Hold Duration (seconds)
                </label>
                <div className="flex items-center gap-2">
                  <input
                    id="exp-duration-num"
                    type="number"
                    min={limits.minDurationSeconds}
                    max={limits.maxDurationSeconds}
                    value={duration}
                    onChange={(e) => setDuration(Math.max(0, parseInt(e.target.value, 10) || 0))}
                    className="w-20 bg-slate-900 border border-slate-700 rounded-lg px-2.5 py-1 text-right text-sm font-mono text-slate-100 focus:outline-none focus:ring-2 focus:ring-brand"
                  />
                  <span className="text-xs text-slate-400">s</span>
                </div>
              </div>

              <input
                id="exp-duration-slider"
                type="range"
                aria-label="Fault Hold Duration Slider"
                min={limits.minDurationSeconds}
                max={limits.maxDurationSeconds}
                value={duration}
                onChange={(e) => setDuration(parseInt(e.target.value, 10))}
                className="w-full accent-brand cursor-pointer"
              />
              <div className="flex justify-between text-[11px] text-slate-400 font-mono">
                <span>Min: {limits.minDurationSeconds} s</span>
                <span>Max: {limits.maxDurationSeconds} s</span>
              </div>
              {!isDurationValid && (
                <p className="text-[11px] text-rose-400">
                  Duration must be between {limits.minDurationSeconds} and {limits.maxDurationSeconds} seconds.
                </p>
              )}
            </div>

            {/* Optional Delay Parameter for service_delay */}
            {hasDelayParam && (
              <div className="md:col-span-2 flex flex-col gap-1.5 bg-slate-800/40 border border-slate-700/60 p-4 rounded-xl">
                <label htmlFor="exp-delay" className="text-xs font-semibold text-slate-200">
                  Latency Injection (delay_ms)
                </label>
                <div className="flex items-center gap-2">
                  <input
                    id="exp-delay"
                    type="number"
                    min={1}
                    max={10000}
                    value={delayMs}
                    onChange={(e) => setDelayMs(parseInt(e.target.value, 10) || 0)}
                    className="w-32 bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-sm font-mono text-slate-100 focus:outline-none focus:ring-2 focus:ring-brand"
                  />
                  <span className="text-xs text-slate-400">ms (1 – 10,000)</span>
                </div>
                {!isDelayValid && (
                  <p className="text-[11px] text-rose-400">Latency delay must be between 1 and 10,000 ms.</p>
                )}
              </div>
            )}

            {/* Expected Data Loss */}
            <div className="flex flex-col gap-1.5 bg-slate-800/40 border border-slate-700/60 p-4 rounded-xl">
              <label htmlFor="exp-data-loss" className="text-xs font-semibold text-slate-200">
                Expected Data Loss Threshold
              </label>
              <div className="flex items-center gap-2">
                <input
                  id="exp-data-loss"
                  type="number"
                  min={0}
                  value={expectedLoss}
                  onChange={(e) => setExpectedLoss(parseInt(e.target.value, 10) || 0)}
                  className="w-28 bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-sm font-mono text-slate-100 focus:outline-none focus:ring-2 focus:ring-brand"
                />
                <span className="text-xs text-slate-400">records (0 = zero loss)</span>
              </div>
              {!isLossValid && (
                <p className="text-[11px] text-rose-400">Expected data loss must be 0 or a positive integer.</p>
              )}
            </div>

            {/* Maximum Recovery Time */}
            <div className="flex flex-col gap-1.5 bg-slate-800/40 border border-slate-700/60 p-4 rounded-xl">
              <label htmlFor="exp-max-recovery" className="text-xs font-semibold text-slate-200">
                Max Recovery Time Threshold
              </label>
              <div className="flex items-center gap-2">
                <input
                  id="exp-max-recovery"
                  type="number"
                  min={1}
                  value={maxRecovery}
                  onChange={(e) => setMaxRecovery(parseInt(e.target.value, 10) || 1)}
                  className="w-28 bg-slate-900 border border-slate-700 rounded-lg px-3 py-1.5 text-sm font-mono text-slate-100 focus:outline-none focus:ring-2 focus:ring-brand"
                />
                <span className="text-xs text-slate-400">seconds</span>
              </div>
              {!isRecoveryValid && (
                <p className="text-[11px] text-rose-400">Maximum recovery time must be at least 1 second.</p>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Step 4: Review and Confirmation */}
      {currentStep === 4 && (
        <div className="flex flex-col gap-5">
          <div>
            <h3 className="text-sm font-medium text-slate-200">4. Review & Launch</h3>
            <p className="text-xs text-slate-400 mt-0.5">
              Confirm experiment specification before executing disruption.
            </p>
          </div>

          {/* Description Sentence */}
          <div className="p-4 rounded-xl bg-brand/10 border border-brand/30 text-sm text-brand-light flex items-start gap-3">
            <Info className="w-5 h-5 shrink-0 mt-0.5 text-brand" />
            <p className="leading-relaxed">{reviewSentence}</p>
          </div>

          {/* Generated YAML */}
          <div className="flex flex-col gap-2">
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium text-slate-300">Generated Experiment YAML</span>
              <button
                type="button"
                onClick={handleCopyYaml}
                className="text-xs flex items-center gap-1.5 text-slate-400 hover:text-slate-200 px-2.5 py-1 rounded-lg bg-slate-800 border border-slate-700 transition-colors"
              >
                {copied ? <Check className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
                {copied ? 'Copied' : 'Copy YAML'}
              </button>
            </div>
            <pre className="bg-slate-950 border border-slate-800 rounded-xl p-4 text-xs font-mono text-slate-300 overflow-x-auto leading-relaxed">
              <code>{yamlContent}</code>
            </pre>
          </div>

          {/* Server Error Banner */}
          {serverError && (
            <div className="p-4 rounded-xl bg-rose-950/60 border border-rose-800 text-rose-300 text-xs flex items-start gap-3">
              <AlertTriangle className="w-5 h-5 shrink-0 text-rose-400" />
              <div>
                <p className="font-semibold text-rose-200 mb-1">Experiment Rejected</p>
                <p>{serverError}</p>
              </div>
            </div>
          )}

          {/* Active Run Warning */}
          {runningExperimentName && (
            <div className="p-4 rounded-xl bg-amber-950/60 border border-amber-800 text-amber-300 text-xs flex items-start gap-3">
              <AlertTriangle className="w-5 h-5 shrink-0 text-amber-400" />
              <p>
                Another experiment (<strong>{runningExperimentName}</strong>) is currently running. Only one experiment may run at a time to prevent compounding failures.
              </p>
            </div>
          )}

          {/* Confirmation Checkbox */}
          <label className="flex items-start gap-3 p-4 rounded-xl bg-slate-800/40 border border-slate-700/60 cursor-pointer select-none hover:bg-slate-800/60 transition-colors">
            <input
              type="checkbox"
              data-testid="exp-confirm"
              checked={confirmed}
              onChange={(e) => setConfirmed(e.target.checked)}
              className="mt-1 w-4 h-4 rounded border-slate-700 text-brand focus:ring-brand accent-brand cursor-pointer"
            />
            <span className="text-xs text-slate-200 leading-relaxed font-medium">
              I understand this will disrupt the selected service (<strong>{selectedTarget}</strong>) for {duration} seconds.
            </span>
          </label>
        </div>
      )}

      {/* Footer Navigation Buttons */}
      <div className="flex items-center justify-between border-t border-slate-800 pt-5 mt-2">
        <button
          type="button"
          onClick={() => setCurrentStep((prev) => Math.max(1, prev - 1))}
          disabled={currentStep === 1 || isSubmitting}
          className={`px-4 py-2 rounded-lg text-xs font-semibold flex items-center gap-2 transition-colors ${
            currentStep === 1 || isSubmitting
              ? 'opacity-40 cursor-not-allowed bg-slate-800 text-slate-500'
              : 'bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-white'
          }`}
        >
          <ArrowLeft className="w-3.5 h-3.5" />
          Back
        </button>

        {currentStep < 4 ? (
          <button
            type="button"
            onClick={() => setCurrentStep((prev) => Math.min(4, prev + 1))}
            disabled={!canProceed}
            className={`px-5 py-2 rounded-lg text-xs font-semibold flex items-center gap-2 transition-all ${
              canProceed
                ? 'bg-brand text-white hover:bg-brand-dark shadow-md'
                : 'opacity-40 cursor-not-allowed bg-slate-800 text-slate-500'
            }`}
          >
            Next
            <ArrowRight className="w-3.5 h-3.5" />
          </button>
        ) : (
          <button
            type="button"
            data-testid="btn-start-experiment"
            disabled={!isStep4Valid}
            onClick={handleStartExperiment}
            title={runningExperimentName ? `Experiment ${runningExperimentName} is already running` : undefined}
            className={`px-6 py-2.5 rounded-lg text-xs font-bold flex items-center gap-2 transition-all ${
              isStep4Valid
                ? 'bg-amber-600 hover:bg-amber-500 text-white shadow-lg shadow-amber-900/30'
                : 'opacity-40 cursor-not-allowed bg-slate-800 text-slate-500'
            }`}
          >
            {isSubmitting ? (
              <>Starting...</>
            ) : (
              <>
                <Play className="w-3.5 h-3.5 fill-current" />
                Start Experiment
              </>
            )}
          </button>
        )}
      </div>
    </div>
  );
}
