import { createRoot } from "react-dom/client";
import { CacheProvider } from "@emotion/react";
import createCache from "@emotion/cache";
import { CssBaseline, ThemeProvider } from "@mui/material";
import { theme } from "./ui/sharedTheme";
import { createThemeStorageManager, MODE_STORAGE_KEY } from "./ui/themeStorage";
import { Monitor } from "./pages/Monitor";
import "./style.css";
const nonce = document.querySelector<HTMLMetaElement>(
  'meta[name="csp-nonce"]',
)?.content;
const cache = createCache({ key: "monitor", nonce });
createRoot(document.getElementById("root")!).render(
  <CacheProvider value={cache}>
    <ThemeProvider
      theme={theme}
      defaultMode="system"
      modeStorageKey={MODE_STORAGE_KEY}
      storageManager={createThemeStorageManager()}
    >
      <CssBaseline enableColorScheme />
      <Monitor />
    </ThemeProvider>
  </CacheProvider>,
);
