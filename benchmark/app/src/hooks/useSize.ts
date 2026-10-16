// Width of an element, following it with a ResizeObserver (charts are responsive).

import { useLayoutEffect, useRef, useState } from 'react';

export function useWidth<T extends HTMLElement>(initial = 600) {
  const ref = useRef<T | null>(null);
  const [width, setWidth] = useState(initial);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return undefined;
    const read = () => {
      const w = Math.floor(el.getBoundingClientRect().width);
      if (w > 0) setWidth(w);
    };
    read();
    if (typeof ResizeObserver === 'undefined') return undefined;
    const ro = new ResizeObserver(read);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return [ref, width] as const;
}
