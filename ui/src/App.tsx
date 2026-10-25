import { GitMerge, Monitor, Moon, Plug, Sun, Workflow, type LucideIcon } from "lucide-react";
import { BrowserRouter, Link, NavLink, Route, Routes } from "react-router-dom";

import { EditorPage } from "./editor/EditorPage";
import { ConnectionsPage } from "./pages/ConnectionsPage";
import { PipelinesPage } from "./pages/PipelinesPage";
import { setThemePref, useTheme, type ThemePref } from "./theme";

const THEMES: { pref: ThemePref; label: string; icon: LucideIcon }[] = [
  { pref: "light", label: "Day", icon: Sun },
  { pref: "dark", label: "Night", icon: Moon },
  { pref: "system", label: "Auto", icon: Monitor },
];

function ThemeToggle() {
  const [pref] = useTheme();
  return (
    <div className="theme-toggle" role="radiogroup" aria-label="Theme">
      {THEMES.map(({ pref: value, label, icon: Icon }) => (
        <button
          key={value}
          role="radio"
          aria-checked={pref === value}
          title={value === "system" ? "Follow the system setting" : `${label} theme`}
          onClick={() => setThemePref(value)}
        >
          <Icon size={14} />
          <span className="label">{label}</span>
        </button>
      ))}
    </div>
  );
}

export function App() {
  return (
    <BrowserRouter>
      <div className="app">
        <header className="topbar">
          <Link to="/" className="brand">
            <span className="logo"><GitMerge size={16} strokeWidth={2.4} /></span>
            Tributary
          </Link>
          <nav>
            <NavLink to="/" end><Workflow size={15} />Pipelines</NavLink>
            <NavLink to="/connections"><Plug size={15} />Connections</NavLink>
          </nav>
          <span className="meta">Visual pipelines on Apache Spark</span>
          <ThemeToggle />
        </header>
        <main style={{ minHeight: 0, overflow: "auto" }}>
          <Routes>
            <Route path="/" element={<PipelinesPage />} />
            <Route path="/pipelines/:id" element={<EditorPage />} />
            <Route path="/connections" element={<ConnectionsPage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
