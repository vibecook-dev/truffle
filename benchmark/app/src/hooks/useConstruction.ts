// Holding ⌥ shows the construction layer. One app-level flag, handled as in Home's
// App.jsx:240-289: ignored while typing, reset when the window loses focus. The root applies it
// as `.construction` and an aria-live region announces it (RFC 027 §11.4).

import { useEffect, useState } from 'react';

const isTyping = (el: Element | null) =>
  !!el &&
  (el.tagName === 'INPUT' ||
    el.tagName === 'TEXTAREA' ||
    el.tagName === 'SELECT' ||
    (el as HTMLElement).isContentEditable);

/** Dev only: ?construction=1 starts with the layer shown, for screenshots. */
const DEV_ON =
  import.meta.env.DEV &&
  typeof window !== 'undefined' &&
  new URLSearchParams(window.location.search).get('construction') === '1';

export function useConstruction(): boolean {
  const [on, setOn] = useState(DEV_ON);
  useEffect(() => {
    let alt = false;
    const down = (e: KeyboardEvent) => {
      if (e.key === 'Alt' && !alt && !isTyping(document.activeElement)) {
        alt = true;
        setOn(true);
      }
    };
    const up = (e: KeyboardEvent) => {
      if (e.key === 'Alt' && alt) {
        alt = false;
        setOn(false);
      }
    };
    const blur = () => {
      if (alt) {
        alt = false;
        setOn(false);
      }
    };
    window.addEventListener('keydown', down);
    window.addEventListener('keyup', up);
    window.addEventListener('blur', blur);
    return () => {
      window.removeEventListener('keydown', down);
      window.removeEventListener('keyup', up);
      window.removeEventListener('blur', blur);
    };
  }, []);
  return on;
}
