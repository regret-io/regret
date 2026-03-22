"use client";

import { GaugeIcon } from "lucide-react";

export default function BenchmarkPage() {
  return (
    <div className="flex flex-col items-center justify-center py-24 gap-4">
      <div className="flex size-16 items-center justify-center rounded-2xl border border-zinc-800 bg-zinc-900">
        <GaugeIcon className="size-8 text-zinc-600" />
      </div>
      <h2 className="text-lg font-semibold text-zinc-300">Coming Soon</h2>
      <p className="text-sm text-zinc-500 max-w-md text-center">
        Benchmark mode will measure adapter throughput, latency percentiles,
        and resource utilization without verification overhead.
      </p>
    </div>
  );
}
