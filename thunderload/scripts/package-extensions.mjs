// Cross-platform replacement for scripts/package-extensions.ps1 (which is
// kept as-is for PowerShell users). Writes the same packages with the same
// contents and layout, using only Node built-ins — no zip binary needed:
//   dist/extensions/ThunderLoad-Chromium.zip  (manifest.json, no manifest.firefox.json)
//   dist/extensions/ThunderLoad-Firefox.zip   (manifest.firefox.json renamed to manifest.json)
//   dist/extensions/ThunderLoad-Firefox.xpi   (byte-identical copy of the Firefox zip)
// No .crx is produced on purpose: Chrome refuses to install a .crx from
// outside the Web Store, so the developer-mode path is ZIP + Load unpacked.
// Keep the two packagers in sync: same allowlist, same entry order, same
// fixed 1980-01-01 timestamps, same icons/ directory entry, same
// no-backslash verification after writing (see package-extensions.ps1).
// Run: node scripts/package-extensions.mjs
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { deflateRawSync } from "node:zlib";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const extDir = resolve(root, "extension");
const outDir = resolve(root, "dist", "extensions");

// Explicit allowlist: the ONLY files that ship in either package.
// A denylist (copy the whole dir, remove a few names) silently ships
// anything new that lands in extension/ — a mockup, a scratch file,
// a stray manifest — so both packagers build from this list and
// nothing else. The two manifests are handled per-browser below:
// Chromium ships manifest.json, Firefox ships manifest.firefox.json
// renamed to manifest.json.
const EXTENSION_FILES = [
  "background.js",
  "content.js",
  "hook.js",
  "popup.css",
  "popup.html",
  "popup.js",
  "icons/16.png",
  "icons/32.png",
  "icons/48.png",
  "icons/128.png",
];
const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();
function crc32(buffer) {
  let crc = 0xffffffff;
  for (const byte of buffer) crc = CRC_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

// Fixed 1980-01-01 DOS timestamp so repeated runs produce identical bytes.
const DOS_TIME = 0;
const DOS_DATE = 0x0021;

function zipFileEntry(name, data, { directory = false } = {}) {
  const nameBuffer = Buffer.from(name, "utf8");
  const compressed = directory ? Buffer.alloc(0) : deflateRawSync(data, { level: 9 });
  const crc = directory ? 0 : crc32(data);
  const method = directory ? 0 : 8;
  const header = Buffer.alloc(30);
  header.writeUInt32LE(0x04034b50, 0);
  header.writeUInt16LE(20, 4);
  header.writeUInt16LE(0x0800, 6); // UTF-8 filenames
  header.writeUInt16LE(method, 8);
  header.writeUInt16LE(DOS_TIME, 10);
  header.writeUInt16LE(DOS_DATE, 12);
  header.writeUInt32LE(crc, 14);
  header.writeUInt32LE(compressed.length, 18);
  header.writeUInt32LE(data.length, 22);
  header.writeUInt16LE(nameBuffer.length, 26);
  header.writeUInt16LE(0, 28);
  return { header, nameBuffer, compressed, crc, method, size: data.length };
}

function buildZip(entries) {
  const chunks = [];
  const central = [];
  let offset = 0;
  for (const entry of entries) {
    const part = zipFileEntry(entry.name, entry.data, { directory: entry.directory });
    chunks.push(part.header, part.nameBuffer, part.compressed);
    const centralHeader = Buffer.alloc(46);
    centralHeader.writeUInt32LE(0x02014b50, 0);
    centralHeader.writeUInt16LE(20, 4); // version made by
    centralHeader.writeUInt16LE(20, 6); // version needed
    centralHeader.writeUInt16LE(0x0800, 8);
    centralHeader.writeUInt16LE(part.method, 10);
    centralHeader.writeUInt16LE(DOS_TIME, 12);
    centralHeader.writeUInt16LE(DOS_DATE, 14);
    centralHeader.writeUInt32LE(part.crc, 16);
    centralHeader.writeUInt32LE(part.compressed.length, 20);
    centralHeader.writeUInt32LE(part.size, 24);
    centralHeader.writeUInt16LE(part.nameBuffer.length, 28);
    centralHeader.writeUInt16LE(0, 30); // extra
    centralHeader.writeUInt16LE(0, 32); // comment
    centralHeader.writeUInt16LE(0, 34); // disk
    centralHeader.writeUInt16LE(0, 36); // internal attrs
    centralHeader.writeUInt32LE(entry.directory ? 0x10 : 0, 38); // external attrs (DOS dir bit)
    centralHeader.writeUInt32LE(offset, 42);
    central.push(centralHeader, part.nameBuffer);
    offset += part.header.length + part.nameBuffer.length + part.compressed.length;
  }
  const centralSize = central.reduce((sum, chunk) => sum + chunk.length, 0);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(0, 4);
  end.writeUInt16LE(0, 6);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(centralSize, 12);
  end.writeUInt32LE(offset, 16);
  end.writeUInt16LE(0, 20);
  return Buffer.concat([...chunks, ...central, end]);
}

async function collectAllowlisted(manifestFile) {
  // Entries are built from EXTENSION_FILES only, in sorted order,
  // with forward-slash names (relative().split(sep).join("/") would
  // do the same, but the names here are already slash-separated, so
  // a Windows run cannot smuggle a backslash into the archive).
  // A missing allowlisted file fails the build (readFile throws)
  // instead of shipping a broken package.
  const entries = [{ name: "icons/", data: Buffer.alloc(0), directory: true }];
  const names = [...EXTENSION_FILES, manifestFile].sort();
  for (const name of names) {
    const archiveName = name === "manifest.firefox.json" ? "manifest.json" : name;
    entries.push({ name: archiveName, data: await readFile(join(extDir, name)), directory: false });
  }
  return entries;
}

// Verify what was written: no entry may contain a backslash, matching the
// check New-ForwardSlashZip runs in package-extensions.ps1. Reads the
// central directory (entry names live there) instead of trusting the
// names this script wrote.
async function verifyNoBackslashes(filePath) {
  const buffer = await readFile(filePath);
  // End of central directory: 22 bytes, no archive comment is written.
  const eocd = buffer.length - 22;
  if (buffer.readUInt32LE(eocd) !== 0x06054b50) {
    throw new Error(`Cannot verify ${filePath}: end-of-central-directory record not found`);
  }
  const count = buffer.readUInt16LE(eocd + 10);
  let offset = buffer.readUInt32LE(eocd + 16);
  for (let i = 0; i < count; i++) {
    if (buffer.readUInt32LE(offset) !== 0x02014b50) {
      throw new Error(`Cannot verify ${filePath}: central directory entry ${i} is malformed`);
    }
    const nameLength = buffer.readUInt16LE(offset + 28);
    const extraLength = buffer.readUInt16LE(offset + 30);
    const commentLength = buffer.readUInt16LE(offset + 32);
    const name = buffer.toString("utf8", offset + 46, offset + 46 + nameLength);
    if (name.includes("\\")) {
      throw new Error(`Archive contains a backslash entry name: ${name}`);
    }
    offset += 46 + nameLength + extraLength + commentLength;
  }
}

async function main() {
  await mkdir(outDir, { recursive: true });
  const chromium = await collectAllowlisted("manifest.json");
  const firefox = await collectAllowlisted("manifest.firefox.json");
  await writeFile(join(outDir, "ThunderLoad-Chromium.zip"), buildZip(chromium));
  const firefoxZip = buildZip(firefox);
  await writeFile(join(outDir, "ThunderLoad-Firefox.zip"), firefoxZip);
  // The .xpi is the Firefox package under its installable name.
  await writeFile(join(outDir, "ThunderLoad-Firefox.xpi"), firefoxZip);
  await verifyNoBackslashes(join(outDir, "ThunderLoad-Chromium.zip"));
  await verifyNoBackslashes(join(outDir, "ThunderLoad-Firefox.zip"));
  console.log("Extension packages written to " + outDir);
}

main().catch((error) => {
  console.error(error?.message || String(error));
  process.exit(1);
});
