"use client";

import { toast } from "sonner";
import { useEffect, useState, useCallback, useRef, use } from "react";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { StatusBadge } from "@/components/status-badge";
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from "@/components/ui/table";
import type { Hypothesis, StatusResponse, RunResult } from "@/lib/api";
import { getHypothesis, getStatus, getEvents, getResults, stopRun, downloadBundle } from "@/lib/api";
import {
  ArrowLeftIcon, SquareIcon, DownloadIcon, ActivityIcon,
  ClockIcon, ZapIcon, ShieldCheckIcon, AlertTriangleIcon, LayersIcon,
} from "lucide-react";

function formatElapsed(secs: number): string {
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

interface EventItem {
  type: string;
  timestamp: string;
  [key: string]: unknown;
}

export default function RunDetailPage({
  params,
}: {
  params: Promise<{ hypothesisId: string; runId: string }>;
}) {
  const { hypothesisId, runId } = use(params);
  const [hypothesis, setHypothesis] = useState<Hypothesis | null>(null);
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [result, setResult] = useState<RunResult | null>(null);
  const [events, setEvents] = useState<EventItem[]>([]);
  const [tab, setTab] = useState<"stats" | "events">("stats");
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const isRunning = status?.status === "running" && status?.run_id === runId;

  const loadData = useCallback(async () => {
    try {
      const [h, s, evText, results] = await Promise.all([
        getHypothesis(hypothesisId),
        getStatus(hypothesisId),
        getEvents(hypothesisId).catch(() => ""),
        getResults(hypothesisId).catch(() => []),
      ]);
      setHypothesis(h);
      setStatus(s);

      // Find this run's result
      const thisResult = results.find((r) => r.run_id === runId);
      if (thisResult) setResult(thisResult);

      // Parse events for this run
      const parsed: EventItem[] = [];
      for (const line of evText.split("\n")) {
        if (!line.trim()) continue;
        try {
          const ev = JSON.parse(line);
          if (ev.run_id === runId || !ev.run_id) parsed.push(ev);
        } catch { /* skip */ }
      }
      setEvents(parsed);
    } catch (e: unknown) {
      toast.error(e instanceof Error ? e.message : "Failed to load");
    }
  }, [hypothesisId, runId]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  useEffect(() => {
    if (isRunning) {
      pollRef.current = setInterval(loadData, 2000);
    }
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [isRunning, loadData]);

  async function handleStop() {
    try {
      await stopRun(hypothesisId);
      toast.success("Run stopped");
      loadData();
    } catch (e: unknown) {
      toast.error(e instanceof Error ? e.message : "Failed to stop");
    }
  }

  async function handleDownload() {
    try {
      await downloadBundle(hypothesisId);
    } catch (e: unknown) {
      toast.error(e instanceof Error ? e.message : "Download failed");
    }
  }

  const progress = status?.run_id === runId ? status?.progress : null;
  const ops = progress?.completed_ops ?? result?.total_response_ops ?? 0;
  const opsPerSec = progress?.ops_per_sec ?? 0;
  const elapsed = progress?.elapsed_secs ?? 0;
  const checkPassed = progress?.passed_checkpoints ?? result?.passed_checkpoints ?? 0;
  const checkTotal = progress?.total_checkpoints ?? result?.total_checkpoints ?? 0;
  const checkFailed = progress?.failed_checkpoints ?? result?.failed_checkpoints ?? 0;
  const failures = progress?.failed_response_ops ?? result?.failed_response_ops ?? 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Link href="/runs">
          <Button variant="ghost" size="sm">
            <ArrowLeftIcon className="size-4 mr-1" /> Runs
          </Button>
        </Link>
        <div className="flex-1">
          <div className="flex items-center gap-3">
            <h1 className="text-xl font-semibold text-zinc-100">
              {hypothesis?.name ?? "..."}
            </h1>
            <StatusBadge status={isRunning ? "running" : (result?.stop_reason === "completed" ? "passed" : status?.status ?? "idle") as "idle" | "running" | "passed" | "failed" | "stopped"} />
            <span className="font-mono text-xs text-zinc-500">
              {runId.slice(-12)}
            </span>
          </div>
        </div>
        <div className="flex gap-2">
          {isRunning && (
            <Button variant="destructive" size="sm" onClick={handleStop}>
              <SquareIcon className="size-3 mr-1" /> Stop
            </Button>
          )}
          <Button variant="outline" size="sm" onClick={handleDownload}>
            <DownloadIcon className="size-3 mr-1" /> Bundle
          </Button>
        </div>
      </div>

      {/* Progress bar for running */}
      {isRunning && (
        <div className="h-1 rounded-full bg-zinc-800 overflow-hidden">
          <div className="h-full bg-blue-500 animate-pulse" style={{ width: "100%" }} />
        </div>
      )}

      {/* Stat cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        <StatCard icon={<LayersIcon className="size-4" />} label="Total Ops" value={ops.toLocaleString()} highlight={isRunning} />
        <StatCard icon={<ZapIcon className="size-4" />} label="Ops/sec" value={isRunning ? Math.round(opsPerSec).toString() : "-"} highlight={isRunning} color="text-blue-400" />
        <StatCard icon={<ClockIcon className="size-4" />} label="Elapsed" value={elapsed > 0 ? formatElapsed(elapsed) : "-"} highlight={isRunning} />
        <StatCard icon={<ShieldCheckIcon className="size-4" />} label="Checkpoints" value={`${checkPassed}/${checkTotal}`} color={checkFailed > 0 ? "text-red-400" : "text-emerald-400"} />
        <StatCard icon={<AlertTriangleIcon className="size-4" />} label="Failures" value={failures.toString()} color={failures > 0 ? "text-red-400" : "text-zinc-400"} />
      </div>

      {/* Tabs */}
      <div className="flex gap-1 border-b border-zinc-800">
        <button
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${tab === "stats" ? "border-blue-500 text-zinc-100" : "border-transparent text-zinc-500 hover:text-zinc-300"}`}
          onClick={() => setTab("stats")}
        >
          Details
        </button>
        <button
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${tab === "events" ? "border-blue-500 text-zinc-100" : "border-transparent text-zinc-500 hover:text-zinc-300"}`}
          onClick={() => setTab("events")}
        >
          Events ({events.length})
        </button>
      </div>

      {/* Tab content */}
      {tab === "stats" && (
        <div className="space-y-4">
          <div className="rounded-lg border border-zinc-800 bg-zinc-900 p-4 space-y-3">
            <h3 className="text-sm font-semibold text-zinc-300">Run Info</h3>
            <div className="grid grid-cols-2 gap-3 text-sm">
              <div><span className="text-zinc-500">Template:</span> <Link href={`/templates/${hypothesisId}`} className="text-zinc-200 hover:underline">{hypothesis?.name}</Link></div>
              <div><span className="text-zinc-500">Generator:</span> <span className="text-zinc-200 font-mono">{hypothesis?.generator}</span></div>
              <div><span className="text-zinc-500">Adapter:</span> <span className="text-zinc-200 font-mono">{hypothesis?.adapter || "none"}</span></div>
              <div><span className="text-zinc-500">Duration:</span> <span className="text-zinc-200 font-mono">{hypothesis?.duration || "forever"}</span></div>
              {result && <div><span className="text-zinc-500">Stop reason:</span> <span className="text-zinc-200">{result.stop_reason}</span></div>}
              {result?.finished_at && <div><span className="text-zinc-500">Finished:</span> <span className="text-zinc-200">{new Date(result.finished_at).toLocaleString()}</span></div>}
            </div>
          </div>
        </div>
      )}

      {tab === "events" && (
        <div className="rounded-lg border border-zinc-800 overflow-hidden max-h-[500px] overflow-y-auto">
          <Table>
            <TableHeader>
              <TableRow className="border-zinc-800 hover:bg-transparent">
                <TableHead className="text-zinc-400 w-[180px]">Timestamp</TableHead>
                <TableHead className="text-zinc-400 w-[160px]">Type</TableHead>
                <TableHead className="text-zinc-400">Details</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {events.length === 0 ? (
                <TableRow><TableCell colSpan={3} className="text-zinc-500 text-center py-8">No events</TableCell></TableRow>
              ) : (
                events.map((ev, i) => (
                  <TableRow key={i} className="border-zinc-800">
                    <TableCell className="font-mono text-xs text-zinc-500">
                      {ev.timestamp ? new Date(ev.timestamp).toLocaleTimeString() : "-"}
                    </TableCell>
                    <TableCell>
                      <EventBadge type={ev.type} />
                    </TableCell>
                    <TableCell className="font-mono text-xs text-zinc-400 max-w-[400px] truncate">
                      {eventDetails(ev)}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}

function StatCard({ icon, label, value, highlight, color }: {
  icon: React.ReactNode; label: string; value: string; highlight?: boolean; color?: string;
}) {
  return (
    <div className={`rounded-lg border border-zinc-800 bg-zinc-900 p-3 ${highlight ? "ring-1 ring-blue-500/30" : ""}`}>
      <div className="flex items-center gap-2 text-zinc-500 text-xs mb-1">
        {icon} {label}
      </div>
      <div className={`text-lg font-semibold font-mono ${color ?? "text-zinc-100"}`}>
        {value}
      </div>
    </div>
  );
}

function EventBadge({ type }: { type: string }) {
  const colors: Record<string, string> = {
    RunStarted: "bg-blue-500/10 text-blue-400 border-blue-800",
    RunCompleted: "bg-emerald-500/10 text-emerald-400 border-emerald-800",
    RunStopped: "bg-amber-500/10 text-amber-400 border-amber-800",
    BatchStarted: "bg-zinc-800 text-zinc-400 border-zinc-700",
    BatchCompleted: "bg-zinc-800 text-zinc-400 border-zinc-700",
    BatchFailed: "bg-red-500/10 text-red-400 border-red-800",
    CheckpointPassed: "bg-emerald-500/10 text-emerald-400 border-emerald-800",
    CheckpointFailed: "bg-red-500/10 text-red-400 border-red-800",
    ResponseFailed: "bg-red-500/10 text-red-400 border-red-800",
  };
  return (
    <Badge variant="outline" className={`text-xs font-mono ${colors[type] ?? "border-zinc-700 text-zinc-400"}`}>
      {type}
    </Badge>
  );
}

function eventDetails(ev: EventItem): string {
  const parts: string[] = [];
  if (ev.batch_id) parts.push(`batch=${ev.batch_id}`);
  if (ev.duration_ms !== undefined) parts.push(`${ev.duration_ms}ms`);
  if (ev.size !== undefined) parts.push(`ops=${ev.size}`);
  if (ev.keys_checked !== undefined) parts.push(`keys=${ev.keys_checked}`);
  if (ev.failures !== undefined) parts.push(`failures=${ev.failures}`);
  if (ev.pass_rate !== undefined) parts.push(`pass_rate=${ev.pass_rate}`);
  if (ev.stop_reason) parts.push(`reason=${ev.stop_reason}`);
  if (ev.op_id) parts.push(`op=${ev.op_id}`);
  if (ev.expected) parts.push(`expected=${ev.expected}`);
  if (ev.actual) parts.push(`actual=${ev.actual}`);
  if (ev.error) parts.push(`error=${ev.error}`);
  return parts.join(" | ") || "-";
}
