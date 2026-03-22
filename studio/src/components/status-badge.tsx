import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

const statusConfig: Record<string, { className: string; dot: string }> = {
  idle: {
    className: "bg-zinc-500/10 text-zinc-400 border-zinc-700",
    dot: "bg-zinc-500",
  },
  running: {
    className: "bg-blue-500/10 text-blue-400 border-blue-800",
    dot: "bg-blue-500",
  },
  passed: {
    className: "bg-emerald-500/10 text-emerald-400 border-emerald-800",
    dot: "bg-emerald-500",
  },
  failed: {
    className: "bg-red-500/10 text-red-400 border-red-800",
    dot: "bg-red-500",
  },
  stopped: {
    className: "bg-amber-500/10 text-amber-400 border-amber-800",
    dot: "bg-amber-500",
  },
};

export function StatusBadge({ status }: { status: string }) {
  const config = statusConfig[status] || statusConfig.idle;
  const isRunning = status === "running";

  return (
    <Badge variant="outline" className={cn("gap-1.5", config.className)}>
      <span
        className={cn(
          "inline-block size-1.5 rounded-full",
          config.dot,
          isRunning && "animate-pulse-dot"
        )}
      />
      {status}
    </Badge>
  );
}
