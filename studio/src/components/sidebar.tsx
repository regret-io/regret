"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";
import { cn } from "@/lib/utils";
import { useAuth } from "@/lib/auth";
import {
  FlaskConicalIcon,
  CpuIcon,
  PlugIcon,
  GaugeIcon,
  PlayIcon,
  ZapIcon,
  ActivityIcon,
  LockIcon,
  UnlockIcon,
  LogOutIcon,
} from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

const readonlyLinks = [
  { href: "/runs", label: "Runs", icon: PlayIcon },
];

const adminLinks = [
  { href: "/runs", label: "Runs", icon: PlayIcon },
  { href: "/templates", label: "Templates", icon: FlaskConicalIcon },
  { href: "/generators", label: "Generators", icon: CpuIcon },
  { href: "/adapters", label: "Adapters", icon: PlugIcon },
];

const chaosLinks = [
  { href: "/chaos/scenarios", label: "Scenarios", icon: ZapIcon },
  { href: "/chaos/injections", label: "Injections", icon: ActivityIcon },
];

const benchmarkLinks = [
  { href: "/benchmark", label: "Benchmarks", icon: GaugeIcon, comingSoon: true },
];

interface NavLink {
  href: string;
  label: string;
  icon: typeof PlayIcon;
  comingSoon?: boolean;
}

function NavSection({ title, links, pathname }: { title: string; links: NavLink[]; pathname: string }) {
  return (
    <div>
      <p className="px-2 mb-2 text-[11px] font-semibold uppercase tracking-wider text-zinc-500">
        {title}
      </p>
      <div className="space-y-0.5">
        {links.map(({ href, label, icon: Icon, comingSoon }) => {
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
  );
}

function LoginDialog() {
  const { login } = useAuth();
  const [open, setOpen] = useState(false);
  const [pw, setPw] = useState("");
  const [error, setError] = useState("");

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    try {
      const encoded = btoa(`:${pw}`);
      const res = await fetch("/api/generators", {
        headers: { Authorization: `Basic ${encoded}` },
      });
      if (res.ok) {
        login(pw);
        setOpen(false);
        setPw("");
      } else {
        setError("Invalid password");
      }
    } catch {
      setError("Connection error");
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        className="flex items-center gap-2.5 rounded-md px-2.5 py-2 text-sm font-medium transition-colors text-zinc-400 hover:bg-zinc-800/60 hover:text-zinc-200 w-full"
      >
        <LockIcon className="size-4" />
        Admin Login
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Admin Login</DialogTitle>
          <DialogDescription>
            Enter the admin password to access all features.
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={handleSubmit} className="space-y-3">
          <Input
            type="password"
            placeholder="Password"
            value={pw}
            onChange={(e) => setPw(e.target.value)}
            autoFocus
          />
          {error && <p className="text-xs text-red-400">{error}</p>}
          <DialogFooter>
            <Button type="submit" disabled={!pw}>
              <UnlockIcon className="size-4 mr-1" />
              Login
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

export function Sidebar() {
  const pathname = usePathname();
  const { isAdmin, logout } = useAuth();

  const hypothesisLinks = isAdmin ? adminLinks : readonlyLinks;

  return (
    <aside className="sticky top-0 flex h-screen w-[220px] shrink-0 flex-col border-r border-zinc-800 bg-zinc-900">
      <Link
        href="/runs"
        className="flex items-center gap-2 px-5 py-5 border-b border-zinc-800"
      >
        <FlaskConicalIcon className="size-5 text-zinc-400" />
        <span className="text-sm font-semibold tracking-tight text-zinc-100">
          Regret Studio
        </span>
      </Link>

      <nav className="flex-1 overflow-y-auto px-3 py-4 space-y-6">
        <NavSection title="Hypotheses" links={hypothesisLinks} pathname={pathname} />
        {isAdmin && <NavSection title="Chaos" links={chaosLinks} pathname={pathname} />}
        {isAdmin && <NavSection title="Benchmark" links={benchmarkLinks} pathname={pathname} />}
      </nav>

      <div className="border-t border-zinc-800 px-3 py-3">
        {isAdmin ? (
          <button
            onClick={logout}
            className="flex items-center gap-2.5 rounded-md px-2.5 py-2 text-sm font-medium transition-colors text-zinc-400 hover:bg-zinc-800/60 hover:text-zinc-200 w-full"
          >
            <LogOutIcon className="size-4" />
            Logout
          </button>
        ) : (
          <LoginDialog />
        )}
      </div>
    </aside>
  );
}
