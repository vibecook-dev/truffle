// vibecook sites: one script for truffle, ghosttea and mille. The same file in all three repositories
// (docs/assets/site.js); its source is the README kit (sites/shared/site.js). Every page reads fine with
// this file missing: it adds the theme switch, copy buttons, tabs, the contents filter and the version.
(() => {
  const root = document.documentElement;
  const $$ = (sel, el = document) => [...el.querySelectorAll(sel)];
  const store = {
    get: (k) => {
      try {
        return localStorage.getItem(k);
      } catch {
        return null;
      }
    },
    set: (k, v) => {
      try {
        localStorage.setItem(k, v);
      } catch {
        /* private window: the choice lasts for this page only */
      }
    },
  };

  // ── Theme: the system's until the reader picks one ──
  const dark = () =>
    root.dataset.theme ? root.dataset.theme === 'dark' : matchMedia('(prefers-color-scheme: dark)').matches;
  for (const b of $$('.theme')) {
    const label = () => b.setAttribute('aria-label', dark() ? 'Switch to the light theme' : 'Switch to the dark theme');
    label();
    b.addEventListener('click', () => {
      root.dataset.theme = dark() ? 'light' : 'dark';
      store.set('vibecook-theme', root.dataset.theme);
      label();
    });
  }

  // ── Copy ──
  const copy = async (text, b) => {
    let ok = true;
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // No clipboard permission (an embedded view, or an older browser): the long-standing way.
      const t = document.createElement('textarea');
      t.value = text;
      t.readOnly = true;
      t.style.cssText = 'position:fixed;top:0;opacity:0';
      document.body.append(t);
      t.select();
      try {
        ok = document.execCommand('copy');
      } catch {
        ok = false;
      }
      t.remove();
    }
    b.textContent = ok ? 'Copied' : 'Not copied';
    b.dataset.done = '';
    setTimeout(() => {
      b.textContent = 'Copy';
      delete b.dataset.done;
    }, 1600);
  };
  const button = () => {
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'copy';
    b.textContent = 'Copy';
    return b;
  };
  for (const cmd of $$('.cmd')) {
    const b = cmd.appendChild(button());
    b.addEventListener('click', () => copy(cmd.querySelector('code').textContent, b));
  }
  for (const block of $$('.code')) {
    const head = block.querySelector('header');
    if (!head) continue;
    const b = head.appendChild(button());
    b.addEventListener('click', () => copy(block.querySelector('pre:not([hidden])').textContent, b));
  }

  // ── Tabs: one code block, several files ──
  for (const list of $$('[role="tablist"]')) {
    const tabs = $$('[role="tab"]', list);
    const show = (tab, focus) => {
      for (const t of tabs) {
        const on = t === tab;
        t.setAttribute('aria-selected', on);
        t.tabIndex = on ? 0 : -1;
        document.getElementById(t.getAttribute('aria-controls')).hidden = !on;
      }
      if (focus) tab.focus();
    };
    list.addEventListener('click', (e) => {
      const tab = e.target.closest('[role="tab"]');
      if (tab) show(tab);
    });
    list.addEventListener('keydown', (e) => {
      const i = tabs.indexOf(document.activeElement);
      const step = { ArrowRight: 1, ArrowLeft: -1 }[e.key];
      if (i > -1 && step) show(tabs[(i + step + tabs.length) % tabs.length], true);
    });
  }

  // ── The version in the title block, read from npm so it never goes stale ──
  for (const el of $$('[data-npm]')) {
    fetch(`https://registry.npmjs.org/${el.dataset.npm}/latest`)
      .then((r) => (r.ok ? r.json() : null))
      .then((pkg) => {
        if (pkg && /^\d+\.\d+\.\d+/.test(pkg.version)) el.textContent = pkg.version;
      })
      .catch(() => {});
  }

  // ── The reference page's contents: where you are, and a filter ──
  const toc = document.querySelector('.toc');
  if (toc) {
    const links = $$('a[href^="#"]', toc);
    const targets = links.map((a) => document.getElementById(a.hash.slice(1))).filter(Boolean);
    const mark = () => {
      let current = targets[0];
      for (const t of targets) if (t.getBoundingClientRect().top < 140) current = t;
      // At the very bottom the last heading may never reach the top of the window.
      if (innerHeight + scrollY >= document.documentElement.scrollHeight - 2) current = targets.at(-1);
      for (const a of links) a.setAttribute('aria-current', a.hash === `#${current.id}`);
    };
    addEventListener('scroll', mark, { passive: true });
    mark();

    const input = toc.querySelector('input');
    const sections = $$('.prose > section');
    const empty = document.querySelector('.empty');
    input?.addEventListener('input', () => {
      const q = input.value.trim().toLowerCase();
      let shown = 0;
      for (const s of sections) {
        s.hidden = q !== '' && !s.textContent.toLowerCase().includes(q);
        if (!s.hidden) shown++;
      }
      for (const a of links) {
        const s = document.getElementById(a.hash.slice(1))?.closest('.prose > section');
        a.hidden = !!s?.hidden;
      }
      empty?.classList.toggle('on', shown === 0);
    });
    // The contents fold on a small window. Following a link there closes them again.
    const fold = toc.querySelector('details');
    const wide = matchMedia('(min-width: 981px)');
    const sync = () => {
      if (fold) fold.open = wide.matches;
    };
    wide.addEventListener('change', sync);
    sync();
    toc.addEventListener('click', (e) => {
      if (e.target.closest('a') && fold && !wide.matches) fold.open = false;
    });
  }
})();
