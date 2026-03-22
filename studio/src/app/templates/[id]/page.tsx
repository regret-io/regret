"use client";

import { toast } from "sonner";
import { useEffect, useState, useCallback, use } from "react";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  DialogClose,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { StatusBadge } from "@/components/status-badge";
import type { Hypothesis, Generator, Adapter, RunResult, StatusResponse } from "@/lib/api";
import {
  getHypothesis,
  startRun,
  deleteHypothesis,
  getResults,
  getStatus,
  stopRun,
  updateHypothesis,
  listGenerators,
  listAdapters,
} from "@/lib/api";
import {
  PlayIcon,
  Loader2Icon,
  ClockIcon,
  PencilIcon,
  Trash2Icon,
} from "lucide-react";
import { cn } from "@/lib/utils";

const ADAPTER_NONE = "__none__";

export default function TemplateDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);
  const router = useRouter();

  const [hypothesis, setHypothesis] = useState<Hypothesis | null>(null);
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [results, setResults] = useState<RunResult[]>([]);
  const [generators, setGenerators] = useState<Generator[]>([]);
  const [adapters, setAdapters] = useState<Adapter[]>([]);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);

  // Edit dialog state
  const [editOpen, setEditOpen] = useState(false);
  const [editName, setEditName] = useState("");
  const [editGenerator, setEditGenerator] = useState("");
  const [editAdapter, setEditAdapter] = useState(ADAPTER_NONE);
  const [editAdapterAddr, setEditAdapterAddr] = useState("");
  const [editDuration, setEditDuration] = useState("");
  const [editToleranceJson, setEditToleranceJson] = useState("");
  const [editSubmitting, setEditSubmitting] = useState(false);

  // Delete dialog state
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);

  const loadData = useCallback(async () => {
    try {
      const [h, s, r, g, a] = await Promise.all([
        getHypothesis(id),
        getStatus(id).catch(() => null),
        getResults(id).catch(() => []),
        listGenerators(),
        listAdapters(),
      ]);
      setHypothesis(h);
      setStatus(s);
      setResults(r);
      setGenerators(g);
      setAdapters(a);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      toast.error(msg);
    } finally {
      setLoading(false);
    }
  }, [id]);

  const isRunning = status?.status === "running";

  // Auto-poll when running
  useEffect(() => {
    if (!isRunning) return;
    const interval = setInterval(loadData, 2000);
    return () => clearInterval(interval);
  }, [isRunning, loadData]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  function openEditDialog() {
    if (!hypothesis) return;
    setEditName(hypothesis.name);
    setEditGenerator(hypothesis.generator);
    setEditAdapter(hypothesis.adapter || ADAPTER_NONE);
    setEditAdapterAddr(hypothesis.adapter_addr || "");
    setEditDuration(hypothesis.duration || "");
    setEditToleranceJson(
      hypothesis.tolerance && Object.keys(hypothesis.tolerance).length > 0
        ? JSON.stringify(hypothesis.tolerance, null, 2)
        : ""
    );
    setEditOpen(true);
  }

  async function handleEdit() {
    setEditSubmitting(true);
    try {
      let tolerance: Record<string, unknown> | undefined;
      if (editToleranceJson.trim()) {
        try {
          tolerance = JSON.parse(editToleranceJson);
        } catch {
          toast.error("Invalid JSON for tolerance");
          setEditSubmitting(false);
          return;
        }
      }
      await updateHypothesis(id, {
        name: editName,
        generator: editGenerator,
        adapter: editAdapter !== ADAPTER_NONE ? editAdapter : undefined,
        adapter_addr: editAdapterAddr || undefined,
        duration: editDuration || undefined,
        tolerance,
      });
      toast.success("Template updated");
      setEditOpen(false);
      loadData();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      toast.error(msg);
    } finally {
      setEditSubmitting(false);
    }
  }

  async function handleStartRun() {
    setSubmitting(true);
    try {
      await startRun(id);
      toast.success("Run started");
      router.push("/runs");
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      toast.error(msg);
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    setDeleting(true);
    try {
      await deleteHypothesis(id);
      toast.success("Template deleted");
      router.push("/templates");
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      toast.error(msg);
    } finally {
      setDeleting(false);
    }
  }

  function stopReasonBadge(reason: string) {
    if (reason === "completed" || reason === "duration_elapsed") {
      return (
        <Badge
          variant="secondary"
          className="bg-emerald-500/10 text-emerald-400 border-emerald-800 text-xs"
        >
          {reason}
        </Badge>
      );
    }
    if (reason === "stopped" || reason === "user_stopped") {
      return (
        <Badge
          variant="secondary"
          className="bg-amber-500/10 text-amber-400 border-amber-800 text-xs"
        >
          {reason}
        </Badge>
      );
    }
    if (reason === "failed" || reason === "error") {
      return (
        <Badge
          variant="secondary"
          className="bg-red-500/10 text-red-400 border-red-800 text-xs"
        >
          {reason}
        </Badge>
      );
    }
    return (
      <Badge variant="outline" className="text-xs">
        {reason}
      </Badge>
    );
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2Icon className="size-5 animate-spin text-zinc-500" />
      </div>
    );
  }

  if (!hypothesis) {
    return <p className="text-sm text-zinc-500">Template not found.</p>;
  }

  return (
    <div className="space-y-6">
      {/* Template Info Card */}
      <div className="rounded-xl border border-zinc-800 bg-zinc-900 p-5">
        <div className="flex items-start justify-between gap-4">
          <div className="space-y-3">
            <h1 className="text-xl font-semibold tracking-tight text-zinc-100">
              {hypothesis.name}
            </h1>
            <div className="flex flex-wrap gap-2">
              <Badge variant="secondary" className="text-xs font-mono">
                {hypothesis.generator}
              </Badge>
              {hypothesis.adapter && (
                <Badge variant="outline" className="text-xs font-mono">
                  {hypothesis.adapter}
                </Badge>
              )}
              {hypothesis.adapter_addr && (
                <Badge
                  variant="outline"
                  className="text-xs font-mono text-zinc-400 border-zinc-700"
                >
                  {hypothesis.adapter_addr}
                </Badge>
              )}
              <Badge
                variant="outline"
                className="text-xs font-mono gap-1 text-zinc-400 border-zinc-700"
              >
                <ClockIcon className="size-3" />
                {hypothesis.duration || "forever"}
              </Badge>
              {hypothesis.tolerance &&
                Object.keys(hypothesis.tolerance).length > 0 && (
                  <Badge
                    variant="outline"
                    className="text-xs font-mono text-zinc-400 border-zinc-700"
                  >
                    tolerance: {JSON.stringify(hypothesis.tolerance)}
                  </Badge>
                )}
            </div>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <Button variant="outline" size="sm" onClick={openEditDialog}>
              <PencilIcon className="size-4 mr-1" />
              Edit
            </Button>
            <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
              <DialogTrigger render={<Button variant="destructive" size="sm" />}>
                <Trash2Icon className="size-4 mr-1" />
                Delete
              </DialogTrigger>
              <DialogContent className="sm:max-w-sm bg-zinc-900 border-zinc-800">
                <DialogHeader>
                  <DialogTitle>Delete Template</DialogTitle>
                </DialogHeader>
                <p className="text-sm text-zinc-400">
                  Are you sure you want to delete{" "}
                  <span className="font-semibold text-zinc-200">
                    {hypothesis.name}
                  </span>
                  ? This action cannot be undone.
                </p>
                <DialogFooter>
                  <DialogClose render={<Button variant="outline" />}>
                    Cancel
                  </DialogClose>
                  <Button
                    variant="destructive"
                    onClick={handleDelete}
                    disabled={deleting}
                  >
                    {deleting ? "Deleting..." : "Delete"}
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
            <Button onClick={handleStartRun} disabled={submitting} size="sm">
              <PlayIcon className="size-4 mr-1" />
              {submitting ? "Starting..." : "Start Run"}
            </Button>
          </div>
        </div>
      </div>

      {/* Edit Dialog */}
      <Dialog open={editOpen} onOpenChange={setEditOpen}>
        <DialogContent className="sm:max-w-md bg-zinc-900 border-zinc-800">
          <DialogHeader>
            <DialogTitle>Edit Template</DialogTitle>
          </DialogHeader>
          <div className="grid gap-4 py-2">
            <div className="grid gap-2">
              <Label htmlFor="edit-name">Name</Label>
              <Input
                id="edit-name"
                value={editName}
                onChange={(e) => setEditName(e.target.value)}
              />
            </div>
            <div className="grid gap-2">
              <Label>Generator</Label>
              <Select
                value={editGenerator}
                onValueChange={(v) => setEditGenerator(v ?? "")}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select generator" />
                </SelectTrigger>
                <SelectContent>
                  {generators.map((g) => (
                    <SelectItem key={g.name} value={g.name}>
                      {g.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label>Adapter (optional)</Label>
              <Select
                value={editAdapter}
                onValueChange={(v) => setEditAdapter(v ?? ADAPTER_NONE)}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="None (reference only)" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={ADAPTER_NONE}>
                    None (reference only)
                  </SelectItem>
                  {adapters.map((a) => (
                    <SelectItem key={a.id} value={a.name}>
                      {a.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="edit-adapter-addr">
                Adapter Address (optional)
              </Label>
              <Input
                id="edit-adapter-addr"
                value={editAdapterAddr}
                onChange={(e) => setEditAdapterAddr(e.target.value)}
                placeholder="http://localhost:9090"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="edit-duration">Duration</Label>
              <Input
                id="edit-duration"
                value={editDuration}
                onChange={(e) => setEditDuration(e.target.value)}
                placeholder="30s, 5m, 1h, or empty for forever"
              />
            </div>
            <div className="grid gap-2">
              <Label htmlFor="edit-tolerance">
                Tolerance (JSON, optional)
              </Label>
              <Textarea
                id="edit-tolerance"
                value={editToleranceJson}
                onChange={(e) => setEditToleranceJson(e.target.value)}
                rows={4}
                className="font-mono text-xs"
                placeholder="{}"
              />
            </div>
          </div>
          <DialogFooter>
            <DialogClose render={<Button variant="outline" />}>
              Cancel
            </DialogClose>
            <Button
              onClick={handleEdit}
              disabled={!editName || !editGenerator || editSubmitting}
            >
              {editSubmitting ? "Saving..." : "Save"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Run History */}
      <div className="space-y-3">
        <h2 className="text-sm font-semibold text-zinc-400 uppercase tracking-wider">
          Run History
        </h2>
        {results.length === 0 && !isRunning ? (
          <p className="py-4 text-sm text-zinc-500">
            No runs yet. Start one above.
          </p>
        ) : (
          <div className="rounded-lg border border-zinc-800 overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="border-zinc-800 hover:bg-transparent">
                  <TableHead className="text-zinc-400">Run ID</TableHead>
                  <TableHead className="text-zinc-400">Status</TableHead>
                  <TableHead className="text-zinc-400">Total Ops</TableHead>
                  <TableHead className="text-zinc-400">Checkpoints</TableHead>
                  <TableHead className="text-zinc-400">Violations</TableHead>
                  <TableHead className="text-zinc-400">Started</TableHead>
                  <TableHead className="text-zinc-400">Finished</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {/* Active run (live) */}
                {isRunning && status?.run_id && (
                  <TableRow
                    className="border-zinc-800 bg-blue-500/5 border-l-2 border-l-blue-500 cursor-pointer"
                    onClick={() => router.push(`/runs/${id}/${status.run_id}`)}
                  >
                    <TableCell className="font-mono text-xs text-blue-400">
                      {status.run_id.slice(-8)}
                    </TableCell>
                    <TableCell>
                      <StatusBadge status="running" />
                    </TableCell>
                    <TableCell className="font-mono text-zinc-300 text-sm">
                      {status.progress?.completed_ops.toLocaleString() ?? 0}
                    </TableCell>
                    <TableCell className="font-mono text-emerald-400 text-sm">
                      {status.progress?.passed_checkpoints ?? 0}/{status.progress?.total_checkpoints ?? 0}
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {(status.progress?.safety_violations ?? 0) > 0
                        ? <span className="text-red-400">{status.progress?.safety_violations}</span>
                        : <span className="text-zinc-400">0</span>}
                    </TableCell>
                    <TableCell className="text-zinc-500 text-xs">now</TableCell>
                    <TableCell className="text-blue-400 text-xs font-mono">
                      {status.progress?.ops_per_sec ? `${Math.round(status.progress.ops_per_sec)} ops/s` : "-"}
                    </TableCell>
                  </TableRow>
                )}
                {/* Completed runs */}
                {results.map((r) => (
                  <TableRow
                    key={r.id}
                    className="border-zinc-800 cursor-pointer hover:bg-zinc-800/50"
                    onClick={() => router.push(`/runs/${id}/${r.run_id}`)}
                  >
                    <TableCell className="font-mono text-xs text-zinc-300">
                      {r.run_id.slice(-8)}
                    </TableCell>
                    <TableCell>{stopReasonBadge(r.stop_reason)}</TableCell>
                    <TableCell className="font-mono text-zinc-300">
                      {r.total_response_ops}
                    </TableCell>
                    <TableCell className="font-mono text-zinc-300">
                      <span className="text-emerald-400">
                        {r.passed_checkpoints}
                      </span>
                      /
                      <span className={cn(r.failed_checkpoints > 0 ? "text-red-400" : "text-zinc-400")}>
                        {r.failed_checkpoints}
                      </span>
                    </TableCell>
                    <TableCell className="font-mono text-zinc-300">
                      {r.safety_violations}
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
      </div>
    </div>
  );
}
