"use client";

import { toast } from "sonner";
import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
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
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { StatusBadge } from "@/components/status-badge";
import { Badge } from "@/components/ui/badge";
import type { Hypothesis, Generator, Adapter } from "@/lib/api";
import {
  listHypotheses,
  createHypothesis,
  startRun,
  stopRun,
  listGenerators,
  listAdapters,
} from "@/lib/api";
import {
  PlusIcon,
  PlayIcon,
  SquareIcon,
  ArrowRightIcon,
  Loader2Icon,
} from "lucide-react";

const ADAPTER_NONE = "__none__";

export default function HypothesesPage() {
  const [hypotheses, setHypotheses] = useState<Hypothesis[]>([]);
  const [generators, setGenerators] = useState<Generator[]>([]);
  const [adapters, setAdapters] = useState<Adapter[]>([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);

  const [name, setName] = useState("");
  const [generator, setGenerator] = useState("");
  const [adapter, setAdapter] = useState(ADAPTER_NONE);
  const [adapterAddr, setAdapterAddr] = useState("");
  const [duration, setDuration] = useState("");
  const [toleranceJson, setToleranceJson] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [actionLoading, setActionLoading] = useState<string | null>(null);

  const load = useCallback(() => {
    Promise.all([listHypotheses(), listGenerators(), listAdapters()])
      .then(([h, g, a]) => {
        setHypotheses(h);
        setGenerators(g);
        setAdapters(a);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  async function handleCreate() {
    setSubmitting(true);
    try {
      let tolerance: Record<string, unknown> | undefined;
      if (toleranceJson.trim()) {
        try {
          tolerance = JSON.parse(toleranceJson);
        } catch (e: unknown) { const msg = e instanceof Error ? e.message : "Unknown error"; toast.error(msg);
          // keep undefined
        }
      }
      await createHypothesis({
        name,
        generator,
        adapter: adapter !== ADAPTER_NONE ? adapter : undefined,
        adapter_addr: adapterAddr || undefined,
        duration: duration || undefined,
        tolerance,
      });
      setName("");
      setGenerator("");
      setAdapter(ADAPTER_NONE);
      setAdapterAddr("");
      setDuration("");
      setToleranceJson("");
      setDialogOpen(false);
      load();
    } catch (e: unknown) { const msg = e instanceof Error ? e.message : "Unknown error"; toast.error(msg);
      // error handling
    } finally {
      setSubmitting(false);
    }
  }

  async function handleStartStop(h: Hypothesis) {
    setActionLoading(h.id);
    try {
      if (h.status === "running") {
        await stopRun(h.id);
      } else {
        await startRun(h.id);
      }
      load();
    } catch (e: unknown) { const msg = e instanceof Error ? e.message : "Unknown error"; toast.error(msg);
      // error
    } finally {
      setActionLoading(null);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold tracking-tight text-zinc-100">
          Hypotheses
        </h1>
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger render={<Button />}>
            <PlusIcon className="size-4 mr-1" />
            New Hypothesis
          </DialogTrigger>
          <DialogContent className="sm:max-w-md bg-zinc-900 border-zinc-800">
            <DialogHeader>
              <DialogTitle>New Hypothesis</DialogTitle>
            </DialogHeader>
            <div className="grid gap-4 py-2">
              <div className="grid gap-2">
                <Label htmlFor="name">Name</Label>
                <Input
                  id="name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="my-hypothesis"
                />
              </div>
              <div className="grid gap-2">
                <Label>Generator</Label>
                <Select value={generator} onValueChange={(v) => setGenerator(v ?? "")}>
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder="Select generator" />
                  </SelectTrigger>
                  <SelectContent>
                    {generators.map((g) => (
                      <SelectItem key={g.name} value={g.name}>
                        {g.name}
                      </SelectItem>
                    ))}
                    {generators.length === 0 && (
                      <SelectItem value="_none" disabled>
                        No generators available
                      </SelectItem>
                    )}
                  </SelectContent>
                </Select>
              </div>
              <div className="grid gap-2">
                <Label>Adapter (optional)</Label>
                <Select value={adapter} onValueChange={(v) => setAdapter(v ?? ADAPTER_NONE)}>
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
                <Label htmlFor="adapter_addr">Adapter Address (optional)</Label>
                <Input
                  id="adapter_addr"
                  value={adapterAddr}
                  onChange={(e) => setAdapterAddr(e.target.value)}
                  placeholder="http://localhost:9090"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="duration">Duration</Label>
                <Input
                  id="duration"
                  value={duration}
                  onChange={(e) => setDuration(e.target.value)}
                  placeholder="30s, 5m, 1h, or empty for forever"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="tolerance">Tolerance (JSON, optional)</Label>
                <Textarea
                  id="tolerance"
                  value={toleranceJson}
                  onChange={(e) => setToleranceJson(e.target.value)}
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
                onClick={handleCreate}
                disabled={!name || !generator || submitting}
              >
                {submitting ? "Creating..." : "Create"}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2Icon className="size-5 animate-spin text-zinc-500" />
        </div>
      ) : hypotheses.length === 0 ? (
        <p className="text-sm text-zinc-500">
          No hypotheses yet. Create one to get started.
        </p>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {hypotheses.map((h) => {
            const isRunning = h.status === "running";
            const isLoading = actionLoading === h.id;
            return (
              <div
                key={h.id}
                className="flex flex-col rounded-xl border border-zinc-800 bg-zinc-900 p-4 gap-3"
              >
                <div className="flex items-start justify-between gap-2">
                  <h3 className="font-semibold text-sm text-zinc-100 truncate">
                    {h.name}
                  </h3>
                  <StatusBadge status={h.status} />
                </div>

                <div className="flex flex-wrap gap-1.5">
                  <Badge variant="secondary" className="text-xs font-mono">
                    {h.generator}
                  </Badge>
                  {h.adapter && (
                    <Badge variant="outline" className="text-xs font-mono">
                      {h.adapter}
                    </Badge>
                  )}
                </div>

                <div className="text-xs text-zinc-500 font-mono">
                  {h.duration || "forever"}
                </div>

                <div className="flex items-center gap-2 mt-auto pt-2 border-t border-zinc-800">
                  <Button
                    size="sm"
                    variant={isRunning ? "destructive" : "default"}
                    onClick={() => handleStartStop(h)}
                    disabled={isLoading}
                    className="flex-1"
                  >
                    {isLoading ? (
                      <Loader2Icon className="size-3.5 animate-spin" />
                    ) : isRunning ? (
                      <>
                        <SquareIcon className="size-3 mr-1" />
                        Stop
                      </>
                    ) : (
                      <>
                        <PlayIcon className="size-3 mr-1" />
                        Start
                      </>
                    )}
                  </Button>
                  <Link href={`/hypotheses/${h.id}`}>
                    <Button size="sm" variant="outline">
                      <ArrowRightIcon className="size-3.5" />
                    </Button>
                  </Link>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
