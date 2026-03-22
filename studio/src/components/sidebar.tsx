"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import {
  LayoutDashboardIcon,
  FlaskConicalIcon,
  CpuIcon,
  PlugIcon,
} from "lucide-react";

const links = [
  { href: "/", label: "Dashboard", icon: LayoutDashboardIcon },
  { href: "/hypotheses", label: "Hypotheses", icon: FlaskConicalIcon },
  { href: "/generators", label: "Generators", icon: CpuIcon },
  { href: "/adapters", label: "Adapters", icon: PlugIcon },
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
      <nav className="flex flex-1 flex-col gap-1 p-2">
        {links.map(({ href, label, icon: Icon }) => {
          const active =
            href === "/" ? pathname === "/" : pathname.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-2 rounded-md px-2.5 py-1.5 text-sm font-medium transition-colors",
                active
                  ? "bg-primary text-primary-foreground"
                  : "text-muted-foreground hover:bg-muted hover:text-foreground"
              )}
            >
              <Icon className="size-4" />
              {label}
            </Link>
          );
        })}
      </nav>
    </aside>
  );
}
