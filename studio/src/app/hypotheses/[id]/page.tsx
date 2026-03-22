"use client";

import { toast } from "sonner";
import { useEffect, useState, useCallback, useRef, use } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { StatusBadge } from "@/components/status-badge";
import type {
  Hypothesis,
  StatusResponse,
  RunResult,
} from "@/lib/api";
import {
  getHypothesis,
  getStatus,
  getEvents,
  getResults,
  startRun,
  stopRun,
  downloadBundle,
} from "@/lib/api";
import {
  PlayIcon,
  SquareIcon,
  DownloadIcon,
  Loader2Icon,
  ActivityIcon,
  ZapIcon,
  ShieldCheckIcon,
  AlertTriangleIcon,
  ClockIcon,
  ChevronDownIcon,
  ChevronRightIcon,
} from "lucide-react";
import { cn } from "@/lib/utils";

interface ParsedEvent {
  [key: string]: unknown;
}

function StatCard({
  label,
  value,
  icon: Icon,
  isRunning,
}: {
  label: string;
  value: string | number;
  icon: React.ComponentType<{ className?: string }>;
  isRunning: boolean;
}) {
  return (
    <div className="flex items-center gap-3 rounded-lg border border-zinc-800 bg-zinc-900 p-3">
      <div className="flex size-9 items-center justify-center rounded-md bg-zinc-800">
        <Icon className="size-4 text-zinc-400" />
      </div>
      <div>
        <p className="text-xs text-zinc-500">{label}</p>
        <p
          className={cn(
            "text-lg font-mono font-semibold text-zinc-100",
            isRunning && "animate-pulse-dot"
          )}
        >
          {value}
        </p>
      </div>
    </div>
  );
}

export default function HypothesisDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);

  const [hypothesis, setHypothesis] = useState<Hypothesis | null>(null);
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [events, setEvents] = useState<ParsedEvent[]>([]);
  const [results, setResults] = useState<RunResult[]>([]);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [expandedRun, setExpandedRun] = useState<string | null>(null);

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadData = useCallback(async () => {
    try {
      const [h, s, evText, r] = await Promise.all([
        getHypothesis(id),
        getStatus(id).catch(() => null),
        getEvents(id).catch(() => ""),
        getResults(id).catch(() => []),
      ]);
      setHypothesis(h);
      setStatus(s);
      setResults(r);
      const parsed: ParsedEvent[] = [];
      if (evText) {
        for (const line of evText.split("\n")) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          try {
            parsed.push(JSON.parse(trimmed));
          } catch {
            // skip malformed lines
          }
        }
      }
      setEvents(parsed);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      toast.error(msg);
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  useEffect(() => {
    if (hypothesis?.status === "running") {
      pollRef.current = setInterval(async () => {
        try {
          const s = await getStatus(id);
          setStatus(s);
          if (s.status !== "running") {
            loadData();
          }
        } catch {
          // ignore
        }
      }, 2000);
    } else if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [hypothesis?.status, id, loadData]);

  async function handleStartRun() {
    setSubmitting(true);
    try {
      await startRun(id);
      loadData();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      toast.error(msg);
    } finally {
      setSubmitting(false);
    }
  }

  async function handleStopRun() {
    try {
      await stopRun(id);
      loadData();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      toast.error(msg);
    }
  }

  async function handleDownload() {
    try {
      await downloadBundle(id);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      toast.error(msg);
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2Icon className="size-5 animate-spin text-zinc-500" />
      </div>
    );
  }

  if (!hypothesis) {
    return (
      <p className="text-sm text-zinc-500">Hypothesis not found.</p>
    );
  }

  const isRunning = hypothesis.status === "running";
  const progress = status?.progress;

  return (
    <div className="space-y-6">
      {/* Template Info Card */}
      <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-5">
        <div className="flex items-start justify-between gap-4">
          <div className="space-y-3">
            <div className="flex items-center gap-3">
              <h1 className="text-xl font-semibold tracking-tight text-zinc-100">
                {hypothesis.name}
              </h1>
              <StatusBadge status={hypothesis.status} />
            </div>
            <div className="flex flex-wrap gap-2">
              <Badge variant="secondary" className="text-xs font-mono">
                {hypothesis.generator}
              </Badge>
              {hypothesis.adapter && (
                <Badge variant="outline" className="text-xs font-mono">
                  {hypothesis.adapter}
                </Badge>
              )}
              <Badge variant="outline" className="text-xs font-mono gap-1 text-zinc-400 border-zinc-700">
                <ClockIcon className="size-3" />
                {hypothesis.duration || "forever"}
              </Badge>
              {hypothesis.tolerance && Object.keys(hypothesis.tolerance).length > 0 && (
                <Badge variant="outline" className="text-xs font-mono text-zinc-400 border-zinc-700">
                  tolerance: {JSON.stringify(hypothesis.tolerance)}
                </Badge>
              )}
            </div>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <Button variant="outline" onClick={handleDownload} size="sm">
              <DownloadIcon className="size-4 mr-1" />
              Bundle
            </Button>
            {isRunning ? (
              <Button variant="destructive" onClick={handleStopRun} size="sm">
                <SquareIcon className="size-4 mr-1" />
                Stop Run
              </Button>
            ) : (
              <Button onClick={handleStartRun} disabled={submitting} size="sm">
                <PlayIcon className="size-4 mr-1" />
                {submitting ? "Starting..." : "Start Run"}
              </Button>
            )}
          </div>
        </div>
      </div>

      {/* Live Status Card (when running) */}
      {isRunning && progress && (
        <div className="space-y-3">
          <h2 className="text-sm font-semibold text-zinc-400 uppercase tracking-wider">
            Live Status
          </h2>
          <div className="grid gap-3 grid-cols-2 md:grid-cols-3 lg:grid-cols-5">
            <StatCard
              label="Total Ops"
              value={progress.completed_ops}
              icon={ActivityIcon}
              isRunning={isRunning}
            />
            <StatCard
              label="Ops/sec"
              value={progress.ops_per_sec.toFixed(1)}
              icon={ZapIcon}
              isRunning={isRunning}
            />
            <StatCard
              label="Checkpoints"
              value={`${progress.passed_checkpoints}/${progress.total_checkpoints}`}
              icon={ShieldCheckIcon}
              isRunning={isRunning}
            />
            <StatCard
              label="Failures"
              value={progress.failed_response_ops}
              icon={AlertTriangleIcon}
              isRunning={isRunning}
            />
            <StatCard
              label="Elapsed Time"
              value={`${progress.elapsed_secs.toFixed(1)}s`}
              icon={ClockIcon}
              isRunning={isRunning}
            />
          </div>
        </div>
      )}

      {/* Run History */}
      <div className="space-y-3">
        <h2 className="text-sm font-semibold text-zinc-400 uppercase tracking-wider">
          Run History
        </h2>
        {results.length === 0 ? (
          <p className="py-4 text-sm text-zinc-500">No runs yet. Start one above.</p>
        ) : (
          <div className="rounded-lg border border-zinc-800 overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="border-zinc-800 hover:bg-transparent">
                  <TableHead className="text-zinc-400 w-8" />
                  <TableHead className="text-zinc-400">Run ID</TableHead>
                  <TableHead className="text-zinc-400">Status</TableHead>
                  <TableHead className="text-zinc-400">Total Ops</TableHead>
                  <TableHead className="text-zinc-400">Checkpoints</TableHead>
                  <TableHead className="text-zinc-400">Failures</TableHead>
                  <TableHead className="text-zinc-400">Started</TableHead>
                  <TableHead className="text-zinc-400">Finished</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {results.map((r) => {
                  const isExpanded = expandedRun === r.id;
                  const runEvents = events.filter(
                    (e) => (e.run_id as string) === r.run_id
                  );
                  return (
                    <>
                      <TableRow
                        key={r.id}
                        className={cn(
                          "border-zinc-800 cursor-pointer transition-colors hover:bg-zinc-900/50",
                          isExpanded && "bg-zinc-900/50"
                        )}
                        onClick={() =>
                          setExpandedRun(isExpanded ? null : r.id)
                        }
                      >
                        <TableCell className="w-8 px-2">
                          {isExpanded ? (
                            <ChevronDownIcon className="size-4 text-zinc-500" />
                          ) : (
                            <ChevronRightIcon className="size-4 text-zinc-500" />
                          )}
                        </TableCell>
                        <TableCell className="font-mono text-xs text-zinc-300">
                          {r.run_id.slice(0, 8)}
                        </TableCell>
                        <TableCell>
                          <Badge
                            variant="outline"
                            className={cn(
                              "text-xs",
                              r.stop_reason === "completed" || r.stop_reason === "duration_elapsed"
                                ? "bg-emerald-500/10 text-emerald-400 border-emerald-800"
                                : r.stop_reason === "failed" || r.stop_reason === "error"
                                  ? "bg-red-500/10 text-red-400 border-red-800"
                                  : "bg-amber-500/10 text-amber-400 border-amber-800"
                            )}
                          >
                            {r.stop_reason}
                          </Badge>
                        </TableCell>
                        <TableCell className="font-mono text-zinc-300">
                          {r.total_response_ops}
                        </TableCell>
                        <TableCell className="font-mono text-zinc-300">
                          <span className="text-emerald-400">
                            {r.passed_checkpoints}
                          </span>
                          /
                          <span className="text-red-400">
                            {r.failed_checkpoints}
                          </span>
                        </TableCell>
                        <TableCell className="font-mono text-zinc-300">
                          {r.failed_response_ops}
                        </TableCell>
                        <TableCell className="text-zinc-500 text-xs">
                          {r.started_at
                            ? new Date(r.started_at).toLocaleString()
                            : "-"}
                        </TableCell>
                        <TableCell className="text-zinc-500 text-xs">
                          {r.finished_at
                            ? new Date(r.finished_at).toLocaleString()
                            : "-"}
                        </TableCell>
                      </TableRow>
                      {isExpanded && (
                        <TableRow key={`${r.id}-events`} className="border-zinc-800">
                          <TableCell colSpan={8} className="p-0">
                            <div className="bg-zinc-950 border-t border-zinc-800 p-4 max-h-72 overflow-y-auto">
                              {runEvents.length === 0 ? (
                                <p className="text-xs text-zinc-500">
                                  No events found for this run.
                                </p>
                              ) : (
                                <div className="space-y-1">
                                  {runEvents.map((e, i) => (
                                    <div
                                      key={i}
                                      className="flex gap-3 text-xs font-mono"
                                    >
                                      <span className="text-zinc-600 shrink-0">
                                        {e.timestamp
                                          ? new Date(
                                              e.timestamp as string
                                            ).toLocaleTimeString()
                                          : "-"}
                                      </span>
                                      <span className="text-zinc-500 shrink-0">
                                        {(e.kind as string) ||
                                          (e.type as string) ||
                                          "event"}
                                      </span>
                                      <span className="text-zinc-400 truncate">
                                        {JSON.stringify(e)}
                                      </span>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                      )}
                    </>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        )}
      </div>
    </div>
  );
}
