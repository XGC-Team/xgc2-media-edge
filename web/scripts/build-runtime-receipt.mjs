import { createHash } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { readdir, readFile, rename, writeFile } from 'node:fs/promises'
import { relative, resolve } from 'node:path'

const productRoot = resolve(import.meta.dirname, '../..')
const [output, os, architecture, version] = process.argv.slice(2)
if (!output || !os || !architecture || !version || process.argv.length !== 6) {
  throw new Error('Usage: node build-runtime-receipt.mjs OUTPUT OS ARCH VERSION')
}
const digest = bytes => createHash('sha256').update(bytes).digest('hex')
const fileHash = async path => digest(await readFile(path))
async function filesIn(path) {
  const result = []
  for (const entry of await readdir(path, { withFileTypes: true })) {
    const child = resolve(path, entry.name)
    if (entry.isDirectory()) result.push(...await filesIn(child))
    else if (entry.isFile()) result.push(child)
    else throw new Error(`Unexpected non-file build input: ${child}`)
  }
  return result.sort()
}
const sources = [resolve(productRoot, 'go.mod'), resolve(productRoot, 'go.sum'),
  resolve(productRoot, '.xgc2/scripts/build.sh'), resolve(import.meta.dirname, 'build-runtime-receipt.mjs'),
  ...await filesIn(resolve(productRoot, 'cmd')), ...await filesIn(resolve(productRoot, 'internal'))]
const source = await Promise.all(sources.sort().map(async path => ({ path: relative(productRoot, path), sha256: await fileHash(path) })))
const webReceipt = resolve(productRoot, 'internal/mediaedge/player/build-receipt.json')
const receipt = { schemaVersion: 1, builder: '.xgc2/scripts/build.sh', os, architecture, version,
  goVersion: execFileSync('go', ['version'], { encoding: 'utf8' }).trim(), source,
  webReceiptSha256: await fileHash(webReceipt), executableSha256: await fileHash(output) }
const destination = `${output}.receipt.json`
const temporary = `${destination}.tmp`
await writeFile(temporary, JSON.stringify(receipt, null, 2) + '\n')
await rename(temporary, destination)
