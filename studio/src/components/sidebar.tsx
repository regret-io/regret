"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import {
  FlaskConicalIcon,
  CpuIcon,
  PlugIcon,
  GaugeIcon,
} from "lucide-react";

const correctnessLinks = [
  { href: "/hypotheses", label: "Hypotheses", icon: FlaskConicalIcon },
  { href: "/generators", label: "Generators", icon: CpuIcon },
  { href: "/adapters", label: "Adapters", icon: PlugIcon },
];

const benchmarkLinks = [
  { href: "/benchmark", label: "Benchmarks", icon: GaugeIcon, comingSoon: true },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="sticky top-0 flex h-screen w-[220px] shrink-0 flex-col border-r border-zinc-800 bg-zinc-900">
      <Link
        href="/hypotheses"
        className="flex items-center gap-2 px-5 py-5 border-b border-zinc-800"
      >
        <FlaskConicalIcon className="size-5 text-zinc-400" />
        <span className="text-sm font-semibold tracking-tight text-zinc-100">
          Regret Studio
        </span>
      </Link>

      <nav className="flex-1 overflow-y-auto px-3 py-4 space-y-6">
        <div>
          <p className="px-2 mb-2 text-[11px] font-semibold uppercase tracking-wider text-zinc-500">
            Correctness
          </p>
          <div className="space-y-0.5">
            {correctnessLinks.map(({ href, label, icon: Icon }) => {
              const active = pathname.startsWith(href);
              return (
                <Link
                  key={href}
                  href={href}
                  className={cn(
                    "flex items-center gap-2.5 rounded-md px-2.5 py-2 text-sm font-medium transition-colors",
                    active
                      ? "bg-zinc-800 text-zinc-100"
                      : "text-zinc-400 hover:bg-zinc-800/60 hover:text-zinc-200"
                  )}
                >
                  <Icon className="size-4" />
                  {label}
                </Link>
              );
            })}
          </div>
        </div>

        <div>
          <p className="px-2 mb-2 text-[11px] font-semibold uppercase tracking-wider text-zinc-500">
            Benchmark
          </p>
          <div className="space-y-0.5">
            {benchmarkLinks.map(({ href, label, icon: Icon, comingSoon }) => {
              const active = pathname.startsWith(href);
              return (
                <Link
                  key={href}
                  href={href}
                  className={cn(
                    "flex items-center gap-2.5 rounded-md px-2.5 py-2 text-sm font-medium transition-colors",
                    active
                      ? "bg-zinc-800 text-zinc-100"
                      : "text-zinc-400 hover:bg-zinc-800/60 hover:text-zinc-200"
                  )}
                >
                  <Icon className="size-4" />
                  {label}
                  {comingSoon && (
                    <span className="ml-auto text-[10px] rounded bg-zinc-800 px-1.5 py-0.5 text-zinc-500">
                      soon
                    </span>
                  )}
                </Link>
              );
            })}
          </div>
        </div>
      </nav>
    </aside>
  );
}
