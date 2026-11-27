import type { MeshNode, NapiNode } from '@vibecook/truffle';

export function getNativeNode(node: MeshNode | NapiNode | null): NapiNode | null {
  return node && 'native' in node ? node.native : node;
}
