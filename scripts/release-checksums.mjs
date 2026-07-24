export const SIDECAR_ASSETS = [
  'tsnet-sidecar-darwin-arm64',
  'tsnet-sidecar-darwin-amd64',
  'tsnet-sidecar-linux-amd64',
  'tsnet-sidecar-linux-arm64',
  'tsnet-sidecar-windows-amd64.exe',
];

export const CHECKSUM_PATHS = [
  'packages/core/sidecar-checksums.json',
  'crates/truffle-sidecar/sidecar-checksums.json',
];

const sha256Pattern = /^[0-9a-f]{64}$/;
const hasOwn = (object, key) => Object.prototype.hasOwnProperty.call(object, key);

export function validateReleaseChecksums(
  checksumMaps,
  expectedVersion,
  { allowMissingChecksums = false, assets = SIDECAR_ASSETS, checksumPaths = CHECKSUM_PATHS } = {},
) {
  if (checksumMaps.length !== checksumPaths.length) {
    throw new Error('checksum map and path counts must match');
  }

  const versionIsAbsent = checksumMaps.every((map) => !hasOwn(map, expectedVersion));
  if (allowMissingChecksums && versionIsAbsent) return [];

  const errors = [];
  for (let index = 0; index < checksumMaps.length; index += 1) {
    const entry = checksumMaps[index][expectedVersion];
    for (const asset of assets) {
      if (!sha256Pattern.test(entry?.[asset] ?? '')) {
        errors.push(`${checksumPaths[index]}: missing valid ${expectedVersion}/${asset} checksum`);
      }
    }
  }

  for (const asset of assets) {
    const firstDigest = checksumMaps[0]?.[expectedVersion]?.[asset];
    for (let index = 1; index < checksumMaps.length; index += 1) {
      const digest = checksumMaps[index]?.[expectedVersion]?.[asset];
      if ((firstDigest || digest) && firstDigest !== digest) {
        errors.push(`sidecar checksum maps disagree for ${expectedVersion}/${asset}`);
        break;
      }
    }
  }

  return errors;
}
