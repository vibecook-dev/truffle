import { pathToFileURL } from 'node:url';

export const CRATES_IO_INDEX_URL = 'https://index.crates.io';

const crateNamePattern = /^[A-Za-z0-9_-]+$/;

export function sparseIndexPath(crateName) {
  if (!crateNamePattern.test(crateName)) {
    throw new Error(`invalid crate name: ${crateName}`);
  }

  const name = crateName.toLowerCase();
  switch (name.length) {
    case 1:
      return `1/${name}`;
    case 2:
      return `2/${name}`;
    case 3:
      return `3/${name[0]}/${name}`;
    default:
      return `${name.slice(0, 2)}/${name.slice(2, 4)}/${name}`;
  }
}

export function sparseIndexContainsVersion(indexContents, crateName, version) {
  const records = indexContents.split('\n').filter(Boolean);

  return records.some((line, index) => {
    let record;
    try {
      record = JSON.parse(line);
    } catch (error) {
      throw new Error(`invalid sparse-index JSON for ${crateName} on line ${index + 1}`, {
        cause: error,
      });
    }
    return record.name.toLowerCase() === crateName.toLowerCase() && record.vers === version;
  });
}

class SparseIndexRequestError extends Error {
  constructor(message, { retryable = false } = {}) {
    super(message);
    this.retryable = retryable;
  }
}

export async function getCrateVersionStatus(
  crateName,
  version,
  { fetchImpl = globalThis.fetch, indexUrl = CRATES_IO_INDEX_URL } = {},
) {
  if (!version) throw new Error('crate version is required');

  const url = `${indexUrl}/${sparseIndexPath(crateName)}`;
  let response;
  try {
    response = await fetchImpl(url, { headers: { accept: 'text/plain' } });
  } catch (error) {
    throw new SparseIndexRequestError(`failed to fetch ${url}: ${error.message}`, {
      retryable: true,
    });
  }

  if (response.status === 404) return 'missing';
  if (response.status !== 200) {
    throw new SparseIndexRequestError(
      `sparse-index lookup for ${crateName} returned HTTP ${response.status}`,
      {
        retryable: response.status === 429 || response.status >= 500,
      },
    );
  }

  const contents = await response.text();
  return sparseIndexContainsVersion(contents, crateName, version) ? 'published' : 'missing';
}

const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

async function getStatusWithRetry(crateName, version) {
  const attempts = 4;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await getCrateVersionStatus(crateName, version);
    } catch (error) {
      if (!error.retryable || attempt === attempts) throw error;
      const delayMilliseconds = 2 ** (attempt - 1) * 1000;
      console.error(
        `${error.message}; retrying in ${delayMilliseconds / 1000}s (${attempt}/${attempts})`,
      );
      await delay(delayMilliseconds);
    }
  }
}

async function main() {
  const args = process.argv.slice(2);
  const waitForPublished = args[0] === '--wait';
  if (waitForPublished) args.shift();

  const [crateName, version, ...extraArgs] = args;
  if (!crateName || !version || extraArgs.length > 0) {
    throw new Error('usage: node scripts/crates-io-version-status.mjs [--wait] <crate> <version>');
  }

  const timeoutMilliseconds = 5 * 60 * 1000;
  const deadline = Date.now() + timeoutMilliseconds;

  while (true) {
    const status = await getStatusWithRetry(crateName, version);
    if (!waitForPublished || status === 'published') {
      console.log(status);
      return;
    }
    if (Date.now() >= deadline) {
      throw new Error(
        `${crateName} ${version} was not visible in the crates.io sparse index after 5 minutes`,
      );
    }
    console.error(`${crateName} ${version} is not indexed yet; retrying in 10s`);
    await delay(10_000);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(`Error: ${error.message}`);
    process.exitCode = 1;
  });
}
