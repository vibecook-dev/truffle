# @vibecook/truffle-react

React hooks for Truffle mesh networking and device-owned synchronized state.

```bash
pnpm add @vibecook/truffle @vibecook/truffle-react react
```

Pass the mesh returned by `createMeshNode()`, its `mesh.native`, or a separately
started `NapiNode` to the hooks inside your component.

```tsx
const { peers, isStarted } = useMesh(mesh);
const { isAuthRequired, authUrl } = useAuth(mesh);
const { localData, allSlices, set } = useSyncedStore(mesh, 'app-state');
```

The package requires React 18 or newer. API documentation, setup, and examples
are in the [Truffle repository](https://github.com/vibecook-dev/truffle).
