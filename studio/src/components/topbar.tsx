"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import {
  LayoutDashboardIcon,
  FlaskConicalIcon,
  CpuIcon,
  PlugIcon,
  MenuIcon,
} from "lucide-react";
import { useState } from "react";

const links = [
  { href: "/", label: "Dashboard", icon: LayoutDashboardIcon },
  { href: "/hypotheses", label: "Hypotheses", icon: FlaskConicalIcon },
  { href: "/generators", label: "Generators", icon: CpuIcon },
  { href: "/adapters", label: "Adapters", icon: PlugIcon },
];

export function Topbar() {
  const pathname = usePathname();
  const [open, setOpen] = useState(false);

  return (
    <header className="flex md:hidden h-12 items-center border-b px-4 gap-3">
      <button
        onClick={() => setOpen(!open)}
        className="p-1 rounded-md hover:bg-muted"
      >
        <MenuIcon className="size-5" />
      </button>
      <span className="text-sm font-semibold tracking-tight">
        Regret Studio
      </span>
      {open && (
        <nav className="absolute top-12 left-0 right-0 z-50 border-b bg-background p-2 flex flex-col gap-1">
          {links.map(({ href, label, icon: Icon }) => {
            const active =
              href === "/" ? pathname === "/" : pathname.startsWith(href);
            return (
              <Link
                key={href}
                href={href}
                onClick={() => setOpen(false)}
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
      )}
    </header>
  );
}
