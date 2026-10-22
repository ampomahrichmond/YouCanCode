import { Route, Routes } from "react-router-dom";

import Layout from "./components/Layout";
import Dashboard from "./pages/Dashboard";
import Positions from "./pages/Positions";
import Settings from "./pages/Settings";
import Trades from "./pages/Trades";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="positions" element={<Positions />} />
        <Route path="trades" element={<Trades />} />
        <Route path="settings" element={<Settings />} />
      </Route>
    </Routes>
  );
}
