"use client";

import { useEffect, useState, useCallback } from "react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
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
import type { Generator } from "@/lib/api";
import { listGenerators, createGenerator } from "@/lib/api";
import { PlusIcon, Loader2Icon } from "lucide-react";

const OPERATION_TYPES = [
  "put",
  "get",
  "delete",
  "delete_range",
  "list",
  "range_scan",
  "cas",
  "ephemeral_put",
  "indexed_put",
  "indexed_get",
  "indexed_list",
  "indexed_range_scan",
  "sequence_put",
] as const;

export default function GeneratorsPage() {
  const [generators, setGenerators] = useState<Generator[]>([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [rate, setRate] = useState("1000");
  const [weights, setWeights] = useState<Record<string, number>>({});
  const [submitting, setSubmitting] = useState(false);

  const load = useCallback(() => {
    listGenerators()
      .then(setGenerators)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  function setWeight(op: string, value: number) {
    setWeights((prev) => {
      const next = { ...prev };
      if (value === 0) {
        delete next[op];
      } else {
        next[op] = value;
      }
      return next;
    });
  }

  async function handleCreate() {
    setSubmitting(true);
    try {
      await createGenerator({ name, description, rate: Number(rate), workload: weights });
      setName("");
      setDescription("");
      setRate("1000");
      setWeights({});
      setDialogOpen(false);
      load();
    } catch {
      // error
    } finally {
      setSubmitting(false);
    }
  }

  function formatWorkload(workload: Record<string, number>): string {
    const entries = Object.entries(workload).filter(([, v]) => v > 0);
    if (entries.length === 0) return "-";
    return entries.map(([k, v]) => `${k}:${v}`).join(", ");
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold tracking-tight text-zinc-100">
          Generators
        </h1>
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger render={<Button />}>
            <PlusIcon className="size-4 mr-1" />
            New Generator
          </DialogTrigger>
          <DialogContent className="sm:max-w-lg bg-zinc-900 border-zinc-800">
            <DialogHeader>
              <DialogTitle>New Generator</DialogTitle>
            </DialogHeader>
            <div className="grid gap-4 py-2">
              <div className="grid gap-2">
                <Label htmlFor="gen-name">Name</Label>
                <Input
                  id="gen-name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="kv-workload"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="gen-desc">Description</Label>
                <Input
                  id="gen-desc"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="Key-value workload generator"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="gen-rate">Rate (ops/s)</Label>
                <Input
                  id="gen-rate"
                  type="number"
                  value={rate}
                  onChange={(e) => setRate(e.target.value)}
                />
              </div>
              <div className="grid gap-2">
                <Label>Workload Weights</Label>
                <div className="grid grid-cols-2 gap-2 max-h-64 overflow-y-auto">
                  {OPERATION_TYPES.map((op) => (
                    <div key={op} className="flex items-center gap-2">
                      <Label className="text-xs font-mono w-28 shrink-0 text-zinc-400">
                        {op}
                      </Label>
                      <Input
                        type="number"
                        min={0}
                        max={100}
                        value={weights[op] ?? 0}
                        onChange={(e) => setWeight(op, Number(e.target.value))}
                        className="h-8 text-xs"
                      />
                    </div>
                  ))}
                </div>
                {Object.keys(weights).length > 0 && (
                  <p className="text-xs text-zinc-500">
                    Active: {Object.entries(weights).filter(([, v]) => v > 0).map(([k, v]) => `${k}=${v}`).join(", ")}
                  </p>
                )}
              </div>
            </div>
            <DialogFooter>
              <DialogClose render={<Button variant="outline" />}>
                Cancel
              </DialogClose>
              <Button onClick={handleCreate} disabled={!name || submitting}>
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
      ) : generators.length === 0 ? (
        <p className="text-sm text-zinc-500">
          No generators yet. Create one to get started.
        </p>
      ) : (
        <div className="rounded-lg border border-zinc-800 overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow className="border-zinc-800 hover:bg-transparent">
                <TableHead className="text-zinc-400">Name</TableHead>
                <TableHead className="text-zinc-400">Description</TableHead>
                <TableHead className="text-zinc-400">Rate</TableHead>
                <TableHead className="text-zinc-400">Workload</TableHead>
                <TableHead className="text-zinc-400">Type</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {generators.map((g) => (
                <TableRow key={g.name} className="border-zinc-800">
                  <TableCell className="font-medium text-zinc-100">
                    {g.name}
                  </TableCell>
                  <TableCell className="text-zinc-400">
                    {g.description}
                  </TableCell>
                  <TableCell className="font-mono text-zinc-300">
                    {g.rate} ops/s
                  </TableCell>
                  <TableCell>
                    <code className="text-xs bg-zinc-800 text-zinc-300 px-1.5 py-0.5 rounded max-w-[200px] truncate block">
                      {formatWorkload(g.workload)}
                    </code>
                  </TableCell>
                  <TableCell>
                    {g.builtin ? (
                      <Badge variant="secondary" className="bg-zinc-800 text-zinc-300 border-zinc-700">
                        builtin
                      </Badge>
                    ) : (
                      <Badge variant="outline" className="border-zinc-700 text-zinc-400">
                        custom
                      </Badge>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}
