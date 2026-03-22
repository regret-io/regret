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
import { Badge } from "@/components/ui/badge";
import type { Hypothesis, Generator, Adapter } from "@/lib/api";
import {
  listHypotheses,
  createHypothesis,
  listGenerators,
  listAdapters,
} from "@/lib/api";
import { PlusIcon, ClockIcon, Loader2Icon } from "lucide-react";

const ADAPTER_NONE = "__none__";

export default function TemplatesPage() {
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

  const load = useCallback(async () => {
    try {
      const [h, g, a] = await Promise.all([
        listHypotheses(),
        listGenerators(),
        listAdapters(),
      ]);
      setHypotheses(h);
      setGenerators(g);
      setAdapters(a);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      toast.error(msg);
    } finally {
      setLoading(false);
    }
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
        } catch {
          toast.error("Invalid JSON for tolerance");
          setSubmitting(false);
          return;
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
      toast.success("Template created");
      setName("");
      setGenerator("");
      setAdapter(ADAPTER_NONE);
      setAdapterAddr("");
      setDuration("");
      setToleranceJson("");
      setDialogOpen(false);
      load();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      toast.error(msg);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold tracking-tight text-zinc-100">
          Templates
        </h1>
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger render={<Button />}>
            <PlusIcon className="size-4 mr-1" />
            New Template
          </DialogTrigger>
          <DialogContent className="sm:max-w-md bg-zinc-900 border-zinc-800">
            <DialogHeader>
              <DialogTitle>New Hypothesis Template</DialogTitle>
            </DialogHeader>
            <div className="grid gap-4 py-2">
              <div className="grid gap-2">
                <Label htmlFor="tpl-name">Name</Label>
                <Input
                  id="tpl-name"
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
                <Label htmlFor="tpl-adapter-addr">Adapter Address (optional)</Label>
                <Input
                  id="tpl-adapter-addr"
                  value={adapterAddr}
                  onChange={(e) => setAdapterAddr(e.target.value)}
                  placeholder="http://localhost:9090"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="tpl-duration">Duration</Label>
                <Input
                  id="tpl-duration"
                  value={duration}
                  onChange={(e) => setDuration(e.target.value)}
                  placeholder="30s, 5m, 1h, or empty for forever"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="tpl-tolerance">Tolerance (JSON, optional)</Label>
                <Textarea
                  id="tpl-tolerance"
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
          No hypothesis templates yet. Create one to get started.
        </p>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {hypotheses.map((h) => (
            <Link key={h.id} href={`/templates/${h.id}`} className="group">
              <div className="flex flex-col rounded-xl border border-zinc-800 bg-zinc-900 p-4 gap-3 transition-colors group-hover:border-zinc-700 group-hover:bg-zinc-900/80">
                <div className="flex items-start justify-between gap-2">
                  <h3 className="font-semibold text-sm text-zinc-100 truncate">
                    {h.name}
                  </h3>
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

                <div className="flex items-center gap-1.5 text-xs text-zinc-500 font-mono">
                  <ClockIcon className="size-3" />
                  {h.duration || "forever"}
                </div>
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}
