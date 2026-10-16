// Order matters: the JY bundle reads window.React once when it loads (RFC 027 §11.5).
import './brand/react-global';
import './brand/components/bundle.js';
import './brand/tokens/tokens.css';
import './brand/components/bundle.css';
import './styles/index.css';

import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { loadApi } from './api';
import { Store } from './state/store';
import { StoreContext } from './state/context';
import { App } from './App';

// Dev and screenshots: ?theme=dark|light sets the theme before the first paint.
const themeParam = new URLSearchParams(window.location.search).get('theme');
if (themeParam === 'dark' || themeParam === 'light')
  document.documentElement.setAttribute('data-theme', themeParam);

const root = createRoot(document.getElementById('root') as HTMLElement);

loadApi().then(
  (api) => {
    const store = new Store(api);
    root.render(
      <StrictMode>
        <StoreContext.Provider value={store}>
          <App />
        </StoreContext.Provider>
      </StrictMode>,
    );
    void store.init();
  },
  // a chunk that didn't load would otherwise leave an empty window
  (e: unknown) =>
    root.render(
      <p role="alert" className="load-error">
        truffle bench could not load: {e instanceof Error ? e.message : String(e)}
      </p>,
    ),
);
