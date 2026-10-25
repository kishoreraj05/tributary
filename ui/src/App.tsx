import { BrowserRouter, NavLink, Route, Routes } from "react-router-dom";

import { EditorPage } from "./editor/EditorPage";
import { ConnectionsPage } from "./pages/ConnectionsPage";
import { PipelinesPage } from "./pages/PipelinesPage";

export function App() {
  return (
    <BrowserRouter>
      <div className="app">
        <header className="topbar">
          <span className="brand">Tributary</span>
          <nav>
            <NavLink to="/" end>Pipelines</NavLink>
            <NavLink to="/connections">Connections</NavLink>
          </nav>
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
