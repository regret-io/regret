"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { GaugeIcon } from "lucide-react";

export default function BenchmarkPage() {
  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-6">Benchmarks</h1>
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-muted-foreground">
            <GaugeIcon className="size-5" />
            Coming Soon
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">
            Benchmark mode will measure adapter throughput, latency percentiles,
            and resource utilization without verification overhead.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
