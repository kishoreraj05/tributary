import { useSyncExternalStore } from "react";

export type ThemePref = "light" | "dark" | "system";
export type Theme = "light" | "dark";

const KEY = "tributary.theme";
const media = window.matchMedia("(prefers-color-scheme: dark)");
const listeners = new Set<() => void>();

export function themePref(): ThemePref {
  try {
    const value = localStorage.getItem(KEY);
    return value === "light" || value === "dark" ? value : "system";
  } catch {
    return "system"; // storage blocked (private mode): follow the OS
  }
}

export function resolvedTheme(pref: ThemePref = themePref()): Theme {
  return pref === "system" ? (media.matches ? "dark" : "light") : pref;
}

/** Applies the theme to <html data-theme>, which the CSS tokens key off. */
export function applyTheme(): void {
  const theme = resolvedTheme();
  document.documentElement.dataset.theme = theme;
  document.documentElement.style.colorScheme = theme;
  listeners.forEach((listener) => listener());
}

export function setThemePref(pref: ThemePref): void {
  try {
    if (pref === "system") localStorage.removeItem(KEY);
    else localStorage.setItem(KEY, pref);
  } catch {
    /* not persisted, still applied for this session */
  }
  current = pref;
  applyTheme();
}

let current: ThemePref = themePref();
media.addEventListener("change", () => current === "system" && applyTheme());

export function onThemeChange(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** [preference, theme actually shown] — re-renders when either changes. */
export function useTheme(): [ThemePref, Theme] {
  const theme = useSyncExternalStore(onThemeChange, () => resolvedTheme(current));
  const pref = useSyncExternalStore(onThemeChange, () => current);
  return [pref, theme];
}
