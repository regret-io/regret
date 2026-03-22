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
import { PlusIcon } from "lucide-react";

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
    return entries.map(([k, v]) => `${k}: ${v}`).join(", ");
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">Generators</h1>
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger render={<Button />}>
            <PlusIcon className="size-4 mr-1" />
            New Generator
          </DialogTrigger>
          <DialogContent className="sm:max-w-lg">
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
                      <Label className="text-xs font-mono w-28 shrink-0">{op}</Label>
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
                  <p className="text-xs text-muted-foreground">
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
        <p className="text-sm text-muted-foreground">Loading...</p>
      ) : generators.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          No generators yet. Create one to get started.
        </p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Description</TableHead>
              <TableHead>Rate</TableHead>
              <TableHead>Workload</TableHead>
              <TableHead>Type</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {generators.map((g) => (
              <TableRow key={g.name}>
                <TableCell className="font-medium">{g.name}</TableCell>
                <TableCell className="text-muted-foreground">
                  {g.description}
                </TableCell>
                <TableCell className="font-mono">
                  {g.rate} ops/s
                </TableCell>
                <TableCell>
                  <code className="text-xs bg-muted px-1.5 py-0.5 rounded max-w-[200px] truncate block">
                    {formatWorkload(g.workload)}
                  </code>
                </TableCell>
                <TableCell>
                  {g.builtin ? (
                    <Badge variant="secondary">builtin</Badge>
                  ) : (
                    <Badge variant="outline">custom</Badge>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}
