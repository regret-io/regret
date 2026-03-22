"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import {
  FlaskConicalIcon,
  CpuIcon,
  PlugIcon,
  ShieldCheckIcon,
  GaugeIcon,
  ZapIcon,
} from "lucide-react";

const navItems = [
  {
    label: "Correctness",
    icon: ShieldCheckIcon,
    children: [
      { href: "/hypotheses", label: "Hypotheses", icon: FlaskConicalIcon },
      { href: "/generators", label: "Generators", icon: CpuIcon },
      { href: "/adapters", label: "Adapters", icon: PlugIcon },
    ],
  },
  {
    label: "Benchmark",
    icon: GaugeIcon,
    children: [
      { href: "/benchmark", label: "Benchmarks", icon: ZapIcon },
    ],
  },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="hidden md:flex w-56 flex-col border-r bg-muted/30">
      <div className="flex h-12 items-center border-b px-4">
        <span className="text-sm font-semibold tracking-tight">
          Regret Studio
        </span>
      </div>
      <nav className="flex flex-1 flex-col gap-3 p-2 pt-3">
        {navItems.map((section) => {
          const Icon = section.icon;
          const anyActive = section.children.some((c) =>
            pathname.startsWith(c.href)
          );
          return (
            <div key={section.label}>
              <div
                className={cn(
                  "flex items-center gap-2 px-2.5 py-1 text-xs font-semibold uppercase tracking-wider",
                  anyActive ? "text-foreground" : "text-muted-foreground"
                )}
              >
                <Icon className="size-3.5" />
                {section.label}
              </div>
              <div className="flex flex-col gap-0.5 ml-3 border-l pl-2 mt-1">
                {section.children.map(({ href, label, icon: ChildIcon }) => {
                  const active = pathname.startsWith(href);
                  return (
                    <Link
                      key={href}
                      href={href}
                      className={cn(
                        "flex items-center gap-2 rounded-md px-2 py-1.5 text-sm font-medium transition-colors",
                        active
                          ? "bg-primary text-primary-foreground"
                          : "text-muted-foreground hover:bg-muted hover:text-foreground"
                      )}
                    >
                      <ChildIcon className="size-3.5" />
                      {label}
                    </Link>
                  );
                })}
              </div>
            </div>
          );
        })}
      </nav>
    </aside>
  );
}
