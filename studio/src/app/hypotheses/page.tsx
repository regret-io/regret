"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
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
import type { Hypothesis, Generator } from "@/lib/api";
import {
  listHypotheses,
  createHypothesis,
  listGenerators,
} from "@/lib/api";
import { PlusIcon } from "lucide-react";

export default function HypothesesPage() {
  const [hypotheses, setHypotheses] = useState<Hypothesis[]>([]);
  const [generators, setGenerators] = useState<Generator[]>([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);

  const [name, setName] = useState("");
  const [generator, setGenerator] = useState("");
  const [toleranceJson, setToleranceJson] = useState("{}");
  const [submitting, setSubmitting] = useState(false);

  const load = useCallback(() => {
    Promise.all([listHypotheses(), listGenerators()])
      .then(([h, g]) => {
        setHypotheses(h);
        setGenerators(g);
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
      let tolerance: Record<string, unknown> = {};
      try {
        tolerance = JSON.parse(toleranceJson);
      } catch {
        // keep default
      }
      await createHypothesis({ name, generator, tolerance });
      setName("");
      setGenerator("");
      setToleranceJson("{}");
      setDialogOpen(false);
      load();
    } catch {
      // error handling
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">Hypotheses</h1>
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger render={<Button />}>
            <PlusIcon className="size-4 mr-1" />
            New Hypothesis
          </DialogTrigger>
          <DialogContent className="sm:max-w-md">
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
                <Label htmlFor="tolerance">Tolerance (JSON)</Label>
                <Textarea
                  id="tolerance"
                  value={toleranceJson}
                  onChange={(e) => setToleranceJson(e.target.value)}
                  rows={4}
                  className="font-mono text-xs"
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
        <p className="text-sm text-muted-foreground">Loading...</p>
      ) : hypotheses.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          No hypotheses yet. Create one to get started.
        </p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Generator</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Created</TableHead>
              <TableHead>Last Run</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {hypotheses.map((h) => (
              <TableRow key={h.id}>
                <TableCell>
                  <Link
                    href={`/hypotheses/${h.id}`}
                    className="font-medium hover:underline"
                  >
                    {h.name}
                  </Link>
                </TableCell>
                <TableCell className="text-muted-foreground">
                  {h.generator}
                </TableCell>
                <TableCell>
                  <StatusBadge status={h.status} />
                </TableCell>
                <TableCell className="text-muted-foreground">
                  {new Date(h.created_at).toLocaleDateString()}
                </TableCell>
                <TableCell className="text-muted-foreground">
                  {h.last_run_at
                    ? new Date(h.last_run_at).toLocaleDateString()
                    : "-"}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}
