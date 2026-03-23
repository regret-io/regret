"use client";

import { toast } from "sonner";
import { useEffect, useState, useCallback } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import type { ChaosInjection } from "@/lib/api";
import {
  listChaosInjections,
  stopChaosInjection,
  deleteChaosInjection,
} from "@/lib/api";
import {
  Loader2Icon,
  SquareIcon,
  TrashIcon,
  ActivityIcon,
} from "lucide-react";

export default function ChaosInjectionsPage() {
  const [injections, setInjections] = useState<ChaosInjection[]>([]);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try {
      const data = await listChaosInjections();
      setInjections(data);
    } catch {
      toast.error("Failed to load injections");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
    const interval = setInterval(load, 3000);
    return () => clearInterval(interval);
  }, [load]);

  async function handleStop(id: string) {
    try {
      await stopChaosInjection(id);
      toast.success("Injection stopped");
      load();
    } catch (e: unknown) {
      toast.error(e instanceof Error ? e.message : "Failed to stop injection");
    }
  }

  async function handleDelete(id: string) {
    try {
      await deleteChaosInjection(id);
      toast.success("Injection deleted");
      load();
    } catch {
      toast.error("Failed to delete injection");
    }
  }

  const statusColors: Record<string, string> = {
    running: "bg-green-900/40 text-green-300",
    stopped: "bg-zinc-700/40 text-zinc-300",
    finished: "bg-blue-900/40 text-blue-300",
    error: "bg-red-900/40 text-red-300",
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-zinc-100">
          Chaos Injections
        </h1>
        <p className="text-sm text-zinc-400 mt-1">
          Active and past chaos injection runs
        </p>
      </div>

      {loading ? (
        <div className="flex items-center gap-2 text-zinc-400 text-sm py-12 justify-center">
          <Loader2Icon className="size-4 animate-spin" /> Loading...
        </div>
      ) : injections.length === 0 ? (
        <div className="text-zinc-500 text-sm py-12 text-center">
          No chaos injections yet. Start one from a scenario.
        </div>
      ) : (
        <div className="space-y-3">
          {injections.map((inj) => (
            <div
              key={inj.id}
              className={`rounded-lg border p-4 ${
                inj.status === "running"
                  ? "border-green-800/50 bg-green-950/20"
                  : "border-zinc-800 bg-zinc-900"
              }`}
            >
              <div className="flex items-start justify-between">
                <div>
                  <div className="flex items-center gap-2">
                    <ActivityIcon className="size-4 text-zinc-400" />
                    <h3 className="font-medium text-zinc-100">
                      {inj.scenario_name}
                    </h3>
                    <Badge
                      className={
                        statusColors[inj.status] ||
                        "bg-zinc-700/40 text-zinc-300"
                      }
                    >
                      {inj.status}
                    </Badge>
                  </div>
                  <div className="text-xs text-zinc-500 mt-1.5 space-x-3">
                    <span>{inj.id}</span>
                    <span>
                      Started:{" "}
                      {new Date(inj.started_at).toLocaleString()}
                    </span>
                    {inj.finished_at && (
                      <span>
                        Finished:{" "}
                        {new Date(inj.finished_at).toLocaleString()}
                      </span>
                    )}
                  </div>
                  {inj.error && (
                    <div className="text-xs text-red-400 mt-1">
                      Error: {inj.error}
                    </div>
                  )}
                </div>
                <div className="flex gap-1.5">
                  {inj.status === "running" && (
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => handleStop(inj.id)}
                      title="Stop injection"
                    >
                      <SquareIcon className="size-4 text-red-400" />
                    </Button>
                  )}
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => handleDelete(inj.id)}
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
