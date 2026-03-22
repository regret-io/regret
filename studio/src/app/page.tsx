"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { StatusBadge } from "@/components/status-badge";
import type { Hypothesis, DashboardStats } from "@/lib/api";
import { listHypotheses } from "@/lib/api";
import {
  FlaskConicalIcon,
  PlayIcon,
  CheckCircleIcon,
  XCircleIcon,
} from "lucide-react";

export default function DashboardPage() {
  const [hypotheses, setHypotheses] = useState<Hypothesis[]>([]);
  const [stats, setStats] = useState<DashboardStats>({
    total: 0,
    running: 0,
    passed: 0,
    failed: 0,
  });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    listHypotheses()
      .then((data) => {
        setHypotheses(data);
        setStats({
          total: data.length,
          running: data.filter((h) => h.status === "running").length,
          passed: data.filter((h) => h.status === "passed").length,
          failed: data.filter((h) => h.status === "failed").length,
        });
      })
      .catch(() => {
        // API unavailable – show empty state
      })
      .finally(() => setLoading(false));
  }, []);

  const recent = hypotheses.slice(0, 10);

  const cards = [
    {
      label: "Total Hypotheses",
      value: stats.total,
      icon: FlaskConicalIcon,
    },
    { label: "Running", value: stats.running, icon: PlayIcon },
    { label: "Passed", value: stats.passed, icon: CheckCircleIcon },
    { label: "Failed", value: stats.failed, icon: XCircleIcon },
  ];

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {cards.map(({ label, value, icon: Icon }) => (
          <Card key={label}>
            <CardHeader>
              <CardTitle className="flex items-center justify-between text-sm font-medium text-muted-foreground">
                {label}
                <Icon className="size-4" />
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {loading ? "-" : value}
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Recent Activity</CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <p className="text-sm text-muted-foreground">Loading...</p>
          ) : recent.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No hypotheses yet.{" "}
              <Link
                href="/hypotheses"
                className="underline underline-offset-2"
              >
                Create one
              </Link>
              .
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Generator</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Created</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {recent.map((h) => (
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
                      {new Date(h.created).toLocaleDateString()}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
