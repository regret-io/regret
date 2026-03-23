"use client";

import { toast } from "sonner";
import { useEffect, useState, useCallback } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogClose,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import type { ChaosScenario } from "@/lib/api";
import {
  listChaosScenarios,
  createChaosScenario,
  updateChaosScenario,
  deleteChaosScenario,
  startChaosInjection,
} from "@/lib/api";
import { PlusIcon, PlayIcon, TrashIcon, Loader2Icon, ZapIcon, PencilIcon } from "lucide-react";

export default function ChaosScenariosPage() {
  const [scenarios, setScenarios] = useState<ChaosScenario[]>([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editScenario, setEditScenario] = useState<ChaosScenario | null>(null);

  const [name, setName] = useState("");
  const [namespace, setNamespace] = useState("regret-system");
  const [actionsJson, setActionsJson] = useState(`[
  {
    "type": "pod_kill",
    "selector": {
      "match_labels": { "app.kubernetes.io/name": "oxia" },
      "mode": "one"
    },
    "interval": "60s"
  }
]`);
  const [submitting, setSubmitting] = useState(false);

  const load = useCallback(async () => {
    try {
      const data = await listChaosScenarios();
      setScenarios(data);
    } catch (e: unknown) {
      toast.error("Failed to load scenarios");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const defaultActions = `[
  {
    "type": "pod_kill",
    "selector": {
      "match_labels": { "app.kubernetes.io/name": "oxia" },
      "mode": "one"
    },
    "interval": "60s"
  }
]`;

  function openEdit(s: ChaosScenario) {
    setEditScenario(s);
    setName(s.name);
    setNamespace(s.namespace);
    setActionsJson(JSON.stringify(s.actions, null, 2));
    setDialogOpen(true);
  }

  function openCreate() {
    setEditScenario(null);
    setName("");
    setNamespace("regret-system");
    setActionsJson(defaultActions);
    setDialogOpen(true);
  }

  async function handleSave() {
    try {
      const actions = JSON.parse(actionsJson);
      setSubmitting(true);
      if (editScenario) {
        await updateChaosScenario(editScenario.id, { name, namespace, actions });
        toast.success("Scenario updated");
      } else {
        await createChaosScenario({ name, namespace, actions });
        toast.success("Scenario created");
      }
      setDialogOpen(false);
      setEditScenario(null);
      load();
    } catch (e: unknown) {
      toast.error(e instanceof Error ? e.message : "Failed to save scenario");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete(id: string) {
    try {
      await deleteChaosScenario(id);
      toast.success("Scenario deleted");
      load();
    } catch {
      toast.error("Failed to delete scenario");
    }
  }

  async function handleInject(id: string) {
    try {
      const res = await startChaosInjection(id);
      toast.success(`Injection started: ${res.injection_id}`);
    } catch (e: unknown) {
      toast.error(e instanceof Error ? e.message : "Failed to start injection");
    }
  }

  const actionTypeColors: Record<string, string> = {
    pod_kill: "bg-red-900/40 text-red-300",
    pod_restart: "bg-orange-900/40 text-orange-300",
    network_partition: "bg-purple-900/40 text-purple-300",
    network_delay: "bg-yellow-900/40 text-yellow-300",
    network_loss: "bg-amber-900/40 text-amber-300",
    custom: "bg-zinc-700/40 text-zinc-300",
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold text-zinc-100">Chaos Scenarios</h1>
          <p className="text-sm text-zinc-400 mt-1">
            Define reusable chaos injection templates
          </p>
        </div>
        <Button size="sm" onClick={openCreate}>
          <PlusIcon className="size-4 mr-1.5" />
          New Scenario
        </Button>
        <Dialog open={dialogOpen} onOpenChange={(open) => { setDialogOpen(open); if (!open) setEditScenario(null); }}>
          <DialogContent className="max-w-lg bg-zinc-900 border-zinc-800">
            <DialogHeader>
              <DialogTitle>{editScenario ? "Edit Scenario" : "Create Chaos Scenario"}</DialogTitle>
            </DialogHeader>
            <div className="space-y-4 py-2">
              <div className="space-y-1.5">
                <Label>Name</Label>
                <Input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="oxia-pod-kill"
                />
              </div>
              <div className="space-y-1.5">
                <Label>Namespace</Label>
                <Input
                  value={namespace}
                  onChange={(e) => setNamespace(e.target.value)}
                  placeholder="regret-system"
                />
              </div>
              <div className="space-y-1.5">
                <Label>Actions (JSON)</Label>
                <Textarea
                  value={actionsJson}
                  onChange={(e) => setActionsJson(e.target.value)}
                  rows={12}
                  className="font-mono text-xs"
                />
              </div>
            </div>
            <DialogFooter>
              <DialogClose render={<Button variant="outline" size="sm" />}>
                Cancel
              </DialogClose>
              <Button
                size="sm"
                onClick={handleSave}
                disabled={!name || submitting}
              >
                {submitting && <Loader2Icon className="size-4 mr-1.5 animate-spin" />}
                {editScenario ? "Save" : "Create"}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      {loading ? (
        <div className="flex items-center gap-2 text-zinc-400 text-sm py-12 justify-center">
          <Loader2Icon className="size-4 animate-spin" /> Loading...
        </div>
      ) : scenarios.length === 0 ? (
        <div className="text-zinc-500 text-sm py-12 text-center">
          No chaos scenarios yet. Create one to get started.
        </div>
      ) : (
        <div className="space-y-3">
          {scenarios.map((s) => (
            <div
              key={s.id}
              className="rounded-lg border border-zinc-800 bg-zinc-900 p-4"
            >
              <div className="flex items-start justify-between">
                <div>
                  <div className="flex items-center gap-2">
                    <ZapIcon className="size-4 text-yellow-400" />
                    <h3 className="font-medium text-zinc-100">{s.name}</h3>
                    <span className="text-xs text-zinc-500">{s.namespace}</span>
                  </div>
                  <div className="flex flex-wrap gap-1.5 mt-2">
                    {s.actions.map((a, i) => (
                      <Badge
                        key={i}
                        className={actionTypeColors[a.type] || "bg-zinc-700/40 text-zinc-300"}
                      >
                        {a.type}
                        {a.interval && ` every ${a.interval}`}
                        {a.at && ` at ${a.at}`}
                        {a.duration && ` for ${a.duration}`}
                        {a.selector?.mode && a.selector.mode !== "one" && ` (${a.selector.mode})`}
                      </Badge>
                    ))}
                  </div>
                  <div className="text-xs text-zinc-500 mt-2">
                    {s.id} &middot; {new Date(s.created_at).toLocaleDateString()}
                  </div>
                </div>
                <div className="flex gap-1.5">
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => handleInject(s.id)}
                    title="Start injection"
                  >
                    <PlayIcon className="size-4 text-green-400" />
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => openEdit(s)}
                    title="Edit"
                  >
                    <PencilIcon className="size-4 text-zinc-400" />
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => handleDelete(s.id)}
                    title="Delete"
                  >
                    <TrashIcon className="size-4 text-zinc-500" />
                  </Button>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
