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
import { Textarea } from "@/components/ui/textarea";
import type { Generator } from "@/lib/api";
import { listGenerators, createGenerator } from "@/lib/api";
import { PlusIcon } from "lucide-react";

export default function GeneratorsPage() {
  const [generators, setGenerators] = useState<Generator[]>([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [rate, setRate] = useState("1000");
  const [workloadJson, setWorkloadJson] = useState("{}");
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

  async function handleCreate() {
    setSubmitting(true);
    try {
      let workload: Record<string, unknown> = {};
      try {
        workload = JSON.parse(workloadJson);
      } catch {
        // keep default
      }
      await createGenerator({ name, description, rate: Number(rate), workload });
      setName("");
      setDescription("");
      setRate("1000");
      setWorkloadJson("{}");
      setDialogOpen(false);
      load();
    } catch {
      // error
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">Generators</h1>
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger
            render={
              <Button>
                <PlusIcon className="size-4 mr-1" />
                New Generator
              </Button>
            }
          />
          <DialogContent className="sm:max-w-md">
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
                <Label htmlFor="gen-workload">Workload (JSON)</Label>
                <Textarea
                  id="gen-workload"
                  value={workloadJson}
                  onChange={(e) => setWorkloadJson(e.target.value)}
                  rows={4}
                  className="font-mono text-xs"
                />
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
              <TableRow key={g.id}>
                <TableCell className="font-medium">{g.name}</TableCell>
                <TableCell className="text-muted-foreground">
                  {g.description}
                </TableCell>
                <TableCell className="font-mono">
                  {g.rate} ops/s
                </TableCell>
                <TableCell>
                  <code className="text-xs bg-muted px-1.5 py-0.5 rounded max-w-[200px] truncate block">
                    {JSON.stringify(g.workload)}
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
