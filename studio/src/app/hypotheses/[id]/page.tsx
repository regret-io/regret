"use client";

import { toast } from "sonner";
import { useEffect, useState, useCallback, useRef, use } from "react";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
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
          } catch (e: unknown) { const msg = e instanceof Error ? e.message : "Unknown error"; toast.error(msg);
            // skip malformed lines
          }
        }
      }
      setEvents(parsed);
    } catch (e: unknown) { const msg = e instanceof Error ? e.message : "Unknown error"; toast.error(msg);
      // API error
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
        } catch (e: unknown) { const msg = e instanceof Error ? e.message : "Unknown error"; toast.error(msg);
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
    } catch (e: unknown) { const msg = e instanceof Error ? e.message : "Unknown error"; toast.error(msg);
      // error
    } finally {
      setSubmitting(false);
    }
  }

  async function handleStopRun() {
    try {
      await stopRun(id);
      loadData();
    } catch (e: unknown) { const msg = e instanceof Error ? e.message : "Unknown error"; toast.error(msg);
      // error
    }
  }

  async function handleDownload() {
    try {
      await downloadBundle(id);
    } catch (e: unknown) { const msg = e instanceof Error ? e.message : "Unknown error"; toast.error(msg);
      // error
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
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold tracking-tight text-zinc-100">
            {hypothesis.name}
          </h1>
          <StatusBadge status={hypothesis.status} />
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" onClick={handleDownload}>
            <DownloadIcon className="size-4 mr-1" />
            Bundle
          </Button>
          {isRunning ? (
            <Button variant="destructive" onClick={handleStopRun}>
              <SquareIcon className="size-4 mr-1" />
              Stop Run
            </Button>
          ) : (
            <Button onClick={handleStartRun} disabled={submitting}>
              <PlayIcon className="size-4 mr-1" />
              {submitting ? "Starting..." : "Start Run"}
            </Button>
          )}
        </div>
      </div>

      {/* Stat cards row */}
      <div className="grid gap-3 grid-cols-2 md:grid-cols-3 lg:grid-cols-5">
        <StatCard
          label="Total Ops"
          value={progress?.completed_ops ?? 0}
          icon={ActivityIcon}
          isRunning={isRunning}
        />
        <StatCard
          label="Ops/sec"
          value={progress ? progress.ops_per_sec.toFixed(1) : "0.0"}
          icon={ZapIcon}
          isRunning={isRunning}
        />
        <StatCard
          label="Checkpoints"
          value={
            progress
              ? `${progress.passed_checkpoints}/${progress.total_checkpoints}`
              : "0/0"
          }
          icon={ShieldCheckIcon}
          isRunning={isRunning}
        />
        <StatCard
          label="Failures"
          value={progress?.failed_response_ops ?? 0}
          icon={AlertTriangleIcon}
          isRunning={isRunning}
        />
        <StatCard
          label="Elapsed Time"
          value={progress ? `${progress.elapsed_secs.toFixed(1)}s` : "0.0s"}
          icon={ClockIcon}
          isRunning={isRunning}
        />
      </div>

      {/* Tabs */}
      <Tabs defaultValue="events">
        <TabsList>
          <TabsTrigger value="events">Events</TabsTrigger>
          <TabsTrigger value="results">Results</TabsTrigger>
          <TabsTrigger value="config">Config</TabsTrigger>
        </TabsList>

        <TabsContent value="events">
          {events.length === 0 ? (
            <p className="py-4 text-sm text-zinc-500">No events yet.</p>
          ) : (
            <div className="rounded-lg border border-zinc-800 overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow className="border-zinc-800 hover:bg-transparent">
                    <TableHead className="text-zinc-400 w-44">Timestamp</TableHead>
                    <TableHead className="text-zinc-400 w-28">Type</TableHead>
                    <TableHead className="text-zinc-400">Details</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {events.map((e, i) => (
                    <TableRow key={i} className="border-zinc-800">
                      <TableCell className="text-zinc-500 font-mono text-xs">
                        {e.timestamp
                          ? new Date(e.timestamp as string).toLocaleString()
                          : "-"}
                      </TableCell>
                      <TableCell>
                        <code className="text-xs bg-zinc-800 text-zinc-300 px-1.5 py-0.5 rounded">
                          {(e.kind as string) || (e.type as string) || "event"}
                        </code>
                      </TableCell>
                      <TableCell>
                        <code className="text-xs text-zinc-400 max-w-[500px] truncate block">
                          {JSON.stringify(e)}
                        </code>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </TabsContent>

        <TabsContent value="results">
          {results.length === 0 ? (
            <p className="py-4 text-sm text-zinc-500">No results yet.</p>
          ) : (
            <div className="rounded-lg border border-zinc-800 overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow className="border-zinc-800 hover:bg-transparent">
                    <TableHead className="text-zinc-400">Run ID</TableHead>
                    <TableHead className="text-zinc-400">Checkpoints</TableHead>
                    <TableHead className="text-zinc-400">Failed Ops</TableHead>
                    <TableHead className="text-zinc-400">Stop Reason</TableHead>
                    <TableHead className="text-zinc-400">Started</TableHead>
                    <TableHead className="text-zinc-400">Finished</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {results.map((r) => (
                    <TableRow key={r.id} className="border-zinc-800">
                      <TableCell className="font-mono text-xs text-zinc-300">
                        {r.run_id.slice(0, 8)}
                      </TableCell>
                      <TableCell className="font-mono text-zinc-300">
                        {r.passed_checkpoints}/{r.total_checkpoints}
                      </TableCell>
                      <TableCell className="font-mono text-zinc-300">
                        {r.failed_response_ops}
                      </TableCell>
                      <TableCell className="text-zinc-400">
                        {r.stop_reason}
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
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </TabsContent>

        <TabsContent value="config">
          <div className="rounded-lg border border-zinc-800 bg-zinc-900 p-4">
            <pre className="text-xs font-mono text-zinc-300 whitespace-pre-wrap overflow-x-auto">
              {JSON.stringify(
                {
                  id: hypothesis.id,
                  name: hypothesis.name,
                  generator: hypothesis.generator,
                  adapter: hypothesis.adapter ?? null,
                  adapter_addr: hypothesis.adapter_addr ?? null,
                  duration: hypothesis.duration ?? null,
                  tolerance: hypothesis.tolerance ?? null,
                  status: hypothesis.status,
                  created_at: hypothesis.created_at,
                  last_run_at: hypothesis.last_run_at,
                },
                null,
                2
              )}
            </pre>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
