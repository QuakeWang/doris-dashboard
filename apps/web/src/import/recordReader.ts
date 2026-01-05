export interface RecordBlock {
  raw: string;
}

export interface RecordReaderProgress {
  bytesRead: number;
  bytesTotal: number;
}

export interface RecordReaderOptions {
  onProgress?: (p: RecordReaderProgress) => void;
  signal?: AbortSignal;
  recordStartRegex?: RegExp;
}

const DEFAULT_RECORD_START_REGEX =
  /^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:[,.]\d{3,6})?(?:Z|[+-]\d{2}:?\d{2})?(?:\s+\[[^\]]+\])?\s+/;

export type LineReaderOptions = Omit<RecordReaderOptions, "recordStartRegex">;

export async function* iterateLines(
  file: Blob,
  options: LineReaderOptions = {}
): AsyncGenerator<string> {
  for await (const block of iterateRecordBlocks(file, { ...options, recordStartRegex: /^/ })) {
    yield block.raw;
  }
}

export async function* iterateRecordBlocks(
  file: Blob,
  options: RecordReaderOptions = {}
): AsyncGenerator<RecordBlock> {
  const recordStartRegex = options.recordStartRegex ?? DEFAULT_RECORD_START_REGEX;
  const bytesTotal = file.size;
  const stream = file.stream();
  const reader = stream.getReader();
  const decoder = new TextDecoder("utf-8");

  let bytesRead = 0;
  let buf = "";
  let current: string[] = [];
  let firstLine = true;

  const emitProgress = () => {
    if (options.onProgress) options.onProgress({ bytesRead, bytesTotal });
  };

  while (true) {
    if (options.signal?.aborted) throw new DOMException("Aborted", "AbortError");
    const { value, done } = await reader.read();
    if (done) break;
    bytesRead += value.byteLength;
    buf += decoder.decode(value, { stream: true });

    let start = 0;
    while (true) {
      const nl = buf.indexOf("\n", start);
      if (nl < 0) break;
      let line = buf.slice(start, nl);
      if (line.endsWith("\r")) line = line.slice(0, -1);
      if (firstLine) {
        firstLine = false;
        if (line.charCodeAt(0) === 0xfeff) line = line.slice(1);
      }
      start = nl + 1;

      if (recordStartRegex.test(line) && current.length > 0) {
        yield { raw: current.join("\n") };
        current = [];
      }
      current.push(line);
    }
    buf = buf.slice(start);

    emitProgress();
  }

  const tail = decoder.decode();
  if (tail) buf += tail;
  if (buf.length > 0) {
    let line = buf;
    if (line.endsWith("\r")) line = line.slice(0, -1);
    if (recordStartRegex.test(line) && current.length > 0) {
      yield { raw: current.join("\n") };
      current = [];
    }
    current.push(line);
  }
  if (current.length > 0) {
    yield { raw: current.join("\n") };
  }
}
