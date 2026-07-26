# @vibecook/truffle-react

React hooks for Truffle mesh networking and device-owned synchronized state.

```bash
pnpm add @vibecook/truffle @vibecook/truffle-react react
```

```tsx
const { peers, isStarted } = useMesh(node);
const { localData, allSlices, set } = useSyncedStore(node, 'app-state');
```

The package requires React 18 or newer. API documentation, setup, and examples
are in the [Truffle repository](https://github.com/vibecook-dev/truffle).
