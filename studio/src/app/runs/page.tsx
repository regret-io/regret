"use client";

import { toast } from "sonner";
import { useEffect, useState, useCallback, useRef } from "react";
import Link from "next/link";
import { Badge } from "@/components/ui/badge";
import { StatusBadge } from "@/components/status-badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { Hypothesis, RunResult, StatusResponse, ProgressInfo } from "@/lib/api";
import { listHypotheses, getResults, getStatus } from "@/lib/api";
import { Loader2Icon } from "lucide-react";

function formatElapsed(secs: number): string {
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

interface AggregatedRun {
  hypothesis_id: string;
  hypothesis_name: string;
  run_id: string;
  result_id: string;
  stop_reason: string;
  total_response_ops: number;
  total_checkpoints: number;
  passed_checkpoints: number;
  failed_checkpoints: number;
  safety_violations: number;
  started_at: string | null;
  finished_at: string | null;
  // Live progress fields for active runs
  is_live: boolean;
  live_status?: string;
  progress?: ProgressInfo;
}

export default function RunsPage() {
  const [runs, setRuns] = useState<AggregatedRun[]>([]);
  const [loading, setLoading] = useState(true);
  const [hasActive, setHasActive] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadData = useCallback(async () => {
    try {
      const hypotheses = await listHypotheses();

      // Fetch results and status for all hypotheses in parallel
      const [resultsArr, statusArr] = await Promise.all([
        Promise.allSettled(hypotheses.map((h) => getResults(h.id))),
        Promise.allSettled(hypotheses.map((h) => getStatus(h.id))),
      ]);

      // Build status map by hypothesis id
      const statusMap = new Map<string, StatusResponse>();
      statusArr.forEach((result, idx) => {
        if (result.status === "fulfilled" && result.value) {
          statusMap.set(hypotheses[idx].id, result.value);
        }
      });

      const allRuns: AggregatedRun[] = [];

      // Add completed runs from results
      resultsArr.forEach((result, idx) => {
        if (result.status === "fulfilled") {
          for (const run of result.value) {
            allRuns.push({
              hypothesis_id: hypotheses[idx].id,
              hypothesis_name: hypotheses[idx].name,
              run_id: run.run_id,
              result_id: run.id,
              stop_reason: run.stop_reason,
              total_response_ops: run.total_response_ops,
              total_checkpoints: run.total_checkpoints,
              passed_checkpoints: run.passed_checkpoints,
              failed_checkpoints: run.failed_checkpoints,
              safety_violations: run.safety_violations,
              started_at: run.started_at,
              finished_at: run.finished_at,
              is_live: false,
            });
          }
        }
      });

      // Add or augment live running hypotheses
      let anyActive = false;
      for (const h of hypotheses) {
        const st = statusMap.get(h.id);
        if (st && st.status === "running" && st.run_id) {
          anyActive = true;
          // Check if this run already exists in results
          const existing = allRuns.find(
            (r) => r.run_id === st.run_id && r.hypothesis_id === h.id
          );
          if (existing) {
            existing.is_live = true;
            existing.live_status = st.status;
            existing.progress = st.progress;
          } else {
            allRuns.push({
              hypothesis_id: h.id,
              hypothesis_name: h.name,
              run_id: st.run_id,
              result_id: "",
              stop_reason: "",
              total_response_ops: st.progress?.completed_ops ?? 0,
              total_checkpoints: st.progress?.total_checkpoints ?? 0,
              passed_checkpoints: st.progress?.passed_checkpoints ?? 0,
              failed_checkpoints: st.progress?.failed_checkpoints ?? 0,
              safety_violations: st.progress?.safety_violations ?? 0,
              started_at: null,
              finished_at: null,
              is_live: true,
              live_status: st.status,
              progress: st.progress,
            });
          }
        }
      }
      setHasActive(anyActive);

      // Sort by most recent (live runs first, then by started_at desc)
      allRuns.sort((a, b) => {
        if (a.is_live && a.live_status === "running" && !(b.is_live && b.live_status === "running")) return -1;
        if (b.is_live && b.live_status === "running" && !(a.is_live && a.live_status === "running")) return 1;
        const aTime = a.started_at ? new Date(a.started_at).getTime() : Date.now();
        const bTime = b.started_at ? new Date(b.started_at).getTime() : Date.now();
        return bTime - aTime;
      });

      setRuns(allRuns);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      toast.error(msg);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // Auto-refresh every 3s if any run is active
  useEffect(() => {
    if (hasActive) {
      pollRef.current = setInterval(() => {
        loadData();
      }, 3000);
    } else if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [hasActive, loadData]);

  function statusBadge(run: AggregatedRun) {
    if (run.is_live && run.live_status === "running") {
      return <StatusBadge status="running" />;
    }
    if (!run.stop_reason) {
      return (
        <Badge variant="outline" className="text-xs text-zinc-500">
          unknown
        </Badge>
      );
    }
    if (run.stop_reason === "completed" || run.stop_reason === "duration_elapsed") {
      return (
        <Badge
          variant="secondary"
          className="bg-emerald-500/10 text-emerald-400 border-emerald-800 text-xs"
        >
          {run.stop_reason}
        </Badge>
      );
    }
    if (run.stop_reason === "stopped" || run.stop_reason === "user_stopped") {
      return (
        <Badge
          variant="secondary"
          className="bg-amber-500/10 text-amber-400 border-amber-800 text-xs"
        >
          {run.stop_reason}
        </Badge>
      );
    }
    if (run.stop_reason === "failed" || run.stop_reason === "error") {
      return (
        <Badge
          variant="secondary"
          className="bg-red-500/10 text-red-400 border-red-800 text-xs"
        >
          {run.stop_reason}
        </Badge>
      );
    }
    return (
      <Badge variant="outline" className="text-xs">
        {run.stop_reason}
      </Badge>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold tracking-tight text-zinc-100">
          Runs
        </h1>
        {hasActive && (
          <div className="flex items-center gap-2 text-xs text-zinc-500">
            <Loader2Icon className="size-3 animate-spin" />
            Auto-refreshing
          </div>
        )}
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2Icon className="size-5 animate-spin text-zinc-500" />
        </div>
      ) : runs.length === 0 ? (
        <p className="text-sm text-zinc-500">
          No runs yet. Start a run from a template.
        </p>
      ) : (
        <div className="rounded-lg border border-zinc-800 overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow className="border-zinc-800 hover:bg-transparent">
                <TableHead className="text-zinc-400 w-[160px]">Template</TableHead>
                <TableHead className="text-zinc-400 w-[80px]">Run ID</TableHead>
                <TableHead className="text-zinc-400 w-[90px]">Status</TableHead>
                <TableHead className="text-zinc-400 w-[90px] text-right">Ops</TableHead>
                <TableHead className="text-zinc-400 w-[70px] text-right">Ops/s</TableHead>
                <TableHead className="text-zinc-400 w-[80px] text-right">Elapsed</TableHead>
                <TableHead className="text-zinc-400 w-[100px] text-right">Checkpoints</TableHead>
                <TableHead className="text-zinc-400 w-[70px] text-right">Violations</TableHead>
                <TableHead className="text-zinc-400 w-[120px]">Started</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runs.map((r) => {
                const isRunning = r.is_live && r.live_status === "running";
                return (
                <TableRow
                  key={`${r.hypothesis_id}-${r.run_id}`}
                  className={`border-zinc-800 cursor-pointer hover:bg-zinc-800/50 ${isRunning ? "bg-blue-500/5 border-l-2 border-l-blue-500" : ""}`}
                  onClick={() => window.location.href = `/runs/${r.hypothesis_id}/${r.run_id}`}
                >
                  <TableCell>
                    <Link
                      href={`/templates/${r.hypothesis_id}`}
                      className="text-zinc-100 hover:text-zinc-50 underline-offset-4 hover:underline font-medium text-sm"
                    >
                      {r.hypothesis_name}
                    </Link>
                  </TableCell>
                  <TableCell className="font-mono text-xs text-zinc-400">
                    {r.run_id.slice(-8)}
                  </TableCell>
                  <TableCell>{statusBadge(r)}</TableCell>
                  <TableCell className="font-mono text-zinc-300 text-sm text-right">
                    {r.is_live && r.progress
                      ? r.progress.completed_ops.toLocaleString()
                      : r.total_response_ops.toLocaleString()}
                  </TableCell>
                  <TableCell className="font-mono text-sm text-right">
                    {r.is_live && r.progress ? (
                      <span className="text-blue-400">{Math.round(r.progress.ops_per_sec)}</span>
                    ) : (
                      <span className="text-zinc-500">-</span>
                    )}
                  </TableCell>
                  <TableCell className="font-mono text-sm text-right">
                    {r.is_live && r.progress ? (
                      <span className="text-zinc-300">{formatElapsed(r.progress.elapsed_secs)}</span>
                    ) : (
                      <span className="text-zinc-500">-</span>
                    )}
                  </TableCell>
                  <TableCell className="font-mono text-zinc-300 text-sm text-right">
                    {r.is_live && r.progress
                      ? `${r.progress.passed_checkpoints}/${r.progress.total_checkpoints}`
                      : `${r.passed_checkpoints}/${r.total_checkpoints}`}
                  </TableCell>
                  <TableCell className="font-mono text-zinc-300 text-sm text-right">
                    {r.is_live && r.progress
                      ? r.progress.safety_violations
                      : r.safety_violations}
                  </TableCell>
                  <TableCell className="text-zinc-500 text-xs">
                    {r.started_at
                      ? new Date(r.started_at).toLocaleString()
                      : r.is_live
                        ? "now"
                        : "-"}
                  </TableCell>
                </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
