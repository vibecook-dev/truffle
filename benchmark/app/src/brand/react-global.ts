// The JY component bundle (components/bundle.js) is a classic script that reads `window.React`
// once when it loads. ES imports are hoisted, so this assignment lives in its own module and
// main.tsx imports it before the bundle. The bundle only uses createElement and basic hooks,
// so React 19 works once the global is set.
import * as React from 'react';

(window as unknown as { React: typeof React }).React = React;
