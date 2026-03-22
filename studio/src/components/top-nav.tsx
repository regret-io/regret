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

const tabs = [
  { href: "/hypotheses", label: "Hypotheses", icon: FlaskConicalIcon },
  { href: "/generators", label: "Generators", icon: CpuIcon },
  { href: "/adapters", label: "Adapters", icon: PlugIcon },
  { href: "/benchmark", label: "Benchmark", icon: GaugeIcon },
];

export function TopNav() {
  const pathname = usePathname();

  return (
    <header className="sticky top-0 z-50 flex h-12 items-center border-b border-zinc-800 bg-zinc-950/80 backdrop-blur-sm px-4 md:px-6">
      <Link href="/hypotheses" className="flex items-center gap-2 mr-8 shrink-0">
        <FlaskConicalIcon className="size-4 text-zinc-400" />
        <span className="text-sm font-semibold tracking-tight text-zinc-100">
          Regret Studio
        </span>
      </Link>

      <nav className="flex items-center gap-1">
        {tabs.map(({ href, label, icon: Icon }) => {
          const active =
            href === "/" ? pathname === "/" : pathname.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-1.5 rounded-md px-2.5 py-1.5 text-sm font-medium transition-colors",
                active
                  ? "bg-zinc-800 text-zinc-100"
                  : "text-zinc-500 hover:bg-zinc-900 hover:text-zinc-300"
              )}
            >
              <Icon className="size-3.5" />
              {label}
            </Link>
          );
        })}
      </nav>
    </header>
  );
}
