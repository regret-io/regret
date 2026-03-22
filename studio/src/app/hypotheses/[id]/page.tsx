"use client";

import { useEffect, useState, useCallback, useRef, use } from "react";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
  RunStatus,
  HypothesisEvent,
  Adapter,
} from "@/lib/api";
import {
  getHypothesis,
  getStatus,
  getEvents,
  startRun,
  stopRun,
  downloadBundle,
  listAdapters,
} from "@/lib/api";
import {
  PlayIcon,
  SquareIcon,
  DownloadIcon,
  Loader2Icon,
} from "lucide-react";

export default function HypothesisDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);

  const [hypothesis, setHypothesis] = useState<Hypothesis | null>(null);
  const [status, setStatus] = useState<RunStatus | null>(null);
  const [events, setEvents] = useState<HypothesisEvent[]>([]);
  const [adapters, setAdapters] = useState<Adapter[]>([]);
  const [loading, setLoading] = useState(true);
  const [runDialogOpen, setRunDialogOpen] = useState(false);

  // Run form
  const [adapter, setAdapter] = useState("");
  const [batchSize, setBatchSize] = useState("100");
  const [checkpointEvery, setCheckpointEvery] = useState("10");
  const [duration, setDuration] = useState("60s");
  const [submitting, setSubmitting] = useState(false);

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadData = useCallback(async () => {
    try {
      const [h, s, e, a] = await Promise.all([
        getHypothesis(id),
        getStatus(id).catch(() => null),
        getEvents(id).catch(() => []),
        listAdapters().catch(() => []),
      ]);
      setHypothesis(h);
      setStatus(s);
      setEvents(e);
      setAdapters(a);
    } catch {
      // API error
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // Poll when running
  useEffect(() => {
    if (hypothesis?.status === "running") {
      pollRef.current = setInterval(async () => {
        try {
          const s = await getStatus(id);
          setStatus(s);
          if (s.status !== "running") {
            // Reload full data when run finishes
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
      await startRun(id, {
        adapter,
        batch_size: Number(batchSize),
        checkpoint_every: Number(checkpointEvery),
        duration,
      });
      setRunDialogOpen(false);
      loadData();
    } catch {
      // error
    } finally {
      setSubmitting(false);
    }
  }

  async function handleStopRun() {
    try {
      await stopRun(id);
      loadData();
    } catch {
      // error
    }
  }

  async function handleDownload() {
    try {
      const blob = await downloadBundle(id);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${hypothesis?.name || id}-bundle.tar.gz`;
      a.click();
      URL.revokeObjectURL(url);
    } catch {
      // error
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2Icon className="size-5 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!hypothesis) {
    return (
      <p className="text-sm text-muted-foreground">Hypothesis not found.</p>
    );
  }

  const isRunning = hypothesis.status === "running";

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">
          {hypothesis.name}
        </h1>
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
            <Dialog open={runDialogOpen} onOpenChange={setRunDialogOpen}>
              <DialogTrigger
                render={
                  <Button>
                    <PlayIcon className="size-4 mr-1" />
                    Start Run
                  </Button>
                }
              />
              <DialogContent className="sm:max-w-md">
                <DialogHeader>
                  <DialogTitle>Start Run</DialogTitle>
                </DialogHeader>
                <div className="grid gap-4 py-2">
                  <div className="grid gap-2">
                    <Label>Adapter</Label>
                    <Select value={adapter} onValueChange={(v) => setAdapter(v ?? "")}>
                      <SelectTrigger className="w-full">
                        <SelectValue placeholder="Select adapter" />
                      </SelectTrigger>
                      <SelectContent>
                        {adapters.map((a) => (
                          <SelectItem key={a.id} value={a.name}>
                            {a.name}
                          </SelectItem>
                        ))}
                        {adapters.length === 0 && (
                          <SelectItem value="_none" disabled>
                            No adapters available
                          </SelectItem>
                        )}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="grid gap-2">
                      <Label htmlFor="batch_size">Batch Size</Label>
                      <Input
                        id="batch_size"
                        type="number"
                        value={batchSize}
                        onChange={(e) => setBatchSize(e.target.value)}
                      />
                    </div>
                    <div className="grid gap-2">
                      <Label htmlFor="checkpoint">Checkpoint Every</Label>
                      <Input
                        id="checkpoint"
                        type="number"
                        value={checkpointEvery}
                        onChange={(e) => setCheckpointEvery(e.target.value)}
                      />
                    </div>
                  </div>
                  <div className="grid gap-2">
                    <Label htmlFor="duration">Duration</Label>
                    <Input
                      id="duration"
                      value={duration}
                      onChange={(e) => setDuration(e.target.value)}
                      placeholder="60s"
                    />
                  </div>
                </div>
                <DialogFooter>
                  <DialogClose render={<Button variant="outline" />}>
                    Cancel
                  </DialogClose>
                  <Button
                    onClick={handleStartRun}
                    disabled={!adapter || submitting}
                  >
                    {submitting ? "Starting..." : "Start"}
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          )}
        </div>
      </div>

      {/* Info card */}
      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Details</CardTitle>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
              <dt className="text-muted-foreground">Status</dt>
              <dd>
                <StatusBadge status={hypothesis.status} />
              </dd>
              <dt className="text-muted-foreground">Generator</dt>
              <dd>{hypothesis.generator}</dd>
              <dt className="text-muted-foreground">Created</dt>
              <dd>{new Date(hypothesis.created).toLocaleString()}</dd>
              <dt className="text-muted-foreground">Last Run</dt>
              <dd>
                {hypothesis.last_run
                  ? new Date(hypothesis.last_run).toLocaleString()
                  : "-"}
              </dd>
            </dl>
          </CardContent>
        </Card>

        {/* Status card */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              Run Status
              {isRunning && (
                <Loader2Icon className="size-3.5 animate-spin text-blue-500" />
              )}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {status ? (
              <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
                <dt className="text-muted-foreground">Ops</dt>
                <dd className="font-mono">{status.ops}</dd>
                <dt className="text-muted-foreground">Batches</dt>
                <dd className="font-mono">{status.batches}</dd>
                <dt className="text-muted-foreground">Checkpoints</dt>
                <dd className="font-mono">{status.checkpoints}</dd>
                <dt className="text-muted-foreground">Failures</dt>
                <dd className="font-mono">{status.failures}</dd>
              </dl>
            ) : (
              <p className="text-sm text-muted-foreground">
                No run data available.
              </p>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Events tab */}
      <Tabs defaultValue="events">
        <TabsList>
          <TabsTrigger value="events">Events</TabsTrigger>
        </TabsList>
        <TabsContent value="events">
          {events.length === 0 ? (
            <p className="py-4 text-sm text-muted-foreground">
              No events yet.
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Time</TableHead>
                  <TableHead>Kind</TableHead>
                  <TableHead>Message</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {events.map((e) => (
                  <TableRow key={e.id}>
                    <TableCell className="text-muted-foreground">
                      {new Date(e.timestamp).toLocaleString()}
                    </TableCell>
                    <TableCell>
                      <code className="text-xs bg-muted px-1 py-0.5 rounded">
                        {e.kind}
                      </code>
                    </TableCell>
                    <TableCell>{e.message}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
