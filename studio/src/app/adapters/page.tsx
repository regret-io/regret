"use client";

import { toast } from "sonner";
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
import type { Adapter } from "@/lib/api";
import { listAdapters, createAdapter } from "@/lib/api";
import { PlusIcon, Loader2Icon } from "lucide-react";

export default function AdaptersPage() {
  const [adapters, setAdapters] = useState<Adapter[]>([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);

  const [name, setName] = useState("");
  const [image, setImage] = useState("");
  const [envJson, setEnvJson] = useState("{}");
  const [submitting, setSubmitting] = useState(false);

  const load = useCallback(() => {
    listAdapters()
      .then(setAdapters)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  async function handleCreate() {
    setSubmitting(true);
    try {
      let env: Record<string, string> = {};
      try {
        env = JSON.parse(envJson);
      } catch (e: unknown) { const msg = e instanceof Error ? e.message : "Unknown error"; toast.error(msg);
        // keep default
      }
      await createAdapter({ name, image, env });
      setName("");
      setImage("");
      setEnvJson("{}");
      setDialogOpen(false);
      load();
    } catch (e: unknown) { const msg = e instanceof Error ? e.message : "Unknown error"; toast.error(msg);
      // error
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold tracking-tight text-zinc-100">
          Adapters
        </h1>
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogTrigger render={<Button />}>
            <PlusIcon className="size-4 mr-1" />
            New Adapter
          </DialogTrigger>
          <DialogContent className="sm:max-w-md bg-zinc-900 border-zinc-800">
            <DialogHeader>
              <DialogTitle>New Adapter</DialogTitle>
            </DialogHeader>
            <div className="grid gap-4 py-2">
              <div className="grid gap-2">
                <Label htmlFor="adapter-name">Name</Label>
                <Input
                  id="adapter-name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="redis-adapter"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="adapter-image">Image</Label>
                <Input
                  id="adapter-image"
                  value={image}
                  onChange={(e) => setImage(e.target.value)}
                  placeholder="ghcr.io/org/adapter:latest"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="adapter-env">Environment (JSON)</Label>
                <Textarea
                  id="adapter-env"
                  value={envJson}
                  onChange={(e) => setEnvJson(e.target.value)}
                  rows={4}
                  className="font-mono text-xs"
                  placeholder='{"REDIS_URL": "redis://localhost:6379"}'
                />
              </div>
            </div>
            <DialogFooter>
              <DialogClose render={<Button variant="outline" />}>
                Cancel
              </DialogClose>
              <Button
                onClick={handleCreate}
                disabled={!name || !image || submitting}
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
      ) : adapters.length === 0 ? (
        <p className="text-sm text-zinc-500">
          No adapters yet. Create one to get started.
        </p>
      ) : (
        <div className="rounded-lg border border-zinc-800 overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow className="border-zinc-800 hover:bg-transparent">
                <TableHead className="text-zinc-400">Name</TableHead>
                <TableHead className="text-zinc-400">Image</TableHead>
                <TableHead className="text-zinc-400">Env</TableHead>
                <TableHead className="text-zinc-400">Created</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {adapters.map((a) => (
                <TableRow key={a.id} className="border-zinc-800">
                  <TableCell className="font-medium text-zinc-100">
                    {a.name}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-zinc-400">
                    {a.image}
                  </TableCell>
                  <TableCell>
                    <code className="text-xs bg-zinc-800 text-zinc-300 px-1.5 py-0.5 rounded max-w-[200px] truncate block">
                      {JSON.stringify(a.env)}
                    </code>
                  </TableCell>
                  <TableCell className="text-zinc-500">
                    {new Date(a.created_at).toLocaleDateString()}
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
