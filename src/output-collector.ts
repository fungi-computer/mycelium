import type { MuleToolExecutionPart } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

const capacity = 8192;
// Bound temporary decoded text and raw context even for a single huge write.
const decodeCapacity = capacity * 4;
const encoder = new TextEncoder();
// fatal: false is the existing default; Cloudflare's type requires the field.
const decoder = () =>
  new TextDecoder("utf-8", { ignoreBOM: true, fatal: false });
type Producer = { decoder: ReturnType<typeof decoder>; pending: Uint8Array };
type TextPart = {
  text: string;
  // Includes up to three trailing lookahead bytes, not charged to this part.
  // null denotes an atomic EOF emission.
  raw: Uint8Array | null;
  rawBytes: number;
  textBytes: number;
  newlines: number;
};

/** Complete decoded text, in completion order across stdout/stderr, not byte order. */
export class OutputCollector {
  readonly #producers = {
    stdout: { decoder: decoder(), pending: new Uint8Array(0) },
    stderr: { decoder: decoder(), pending: new Uint8Array(0) },
  } satisfies Record<"stdout" | "stderr", Producer>;
  readonly #probe = decoder();
  // One bounded ring of decoded chunks. TextDecoder owns every UTF-8 boundary.
  readonly #parts = Array.from(
    { length: capacity },
    (): TextPart | undefined => undefined,
  );
  #head = 0;
  #size = 0;
  #textBytes = 0;
  #newlines = 0;
  #droppedBytes = 0;
  #totalBytes = 0;
  #dirty = false;
  #closed = false;

  private constructor(private readonly wake: Queue.Queue<undefined>) {}

  static make() {
    return Queue.make<undefined>({ capacity: 1, strategy: "dropping" }).pipe(
      Effect.map((wake) => new OutputCollector(wake)),
    );
  }

  /** Never waits for a reader. An incomplete scalar stays with its producer. */
  write(stream: "stdout" | "stderr", bytes: Uint8Array): void {
    if (this.#closed || bytes.length === 0) return;
    const producer = this.#producers[stream];
    this.#totalBytes += bytes.length;
    for (let offset = 0; offset < bytes.length; offset += decodeCapacity) {
      const chunk = bytes.subarray(offset, offset + decodeCapacity);
      const text = producer.decoder.decode(chunk, { stream: true });
      const raw = new Uint8Array(producer.pending.length + chunk.length);
      raw.set(producer.pending);
      raw.set(chunk, producer.pending.length);
      const pending = this.#pendingLength(raw);
      producer.pending = raw.slice(raw.length - pending);
      this.#append(text, raw.length - pending, raw);
    }
    this.#dirty = true;
    Queue.offerUnsafe(this.wake, undefined);
  }

  #decodePrefix(raw: Uint8Array, length: number): string {
    const text = this.#probe.decode(raw.subarray(0, length), { stream: true });
    this.#probe.decode();
    return text;
  }

  #pendingLength(raw: Uint8Array): number {
    // UTF-8 has at most three incomplete bytes. Ask the maintained decoder
    // which suffix is still incomplete; do not inspect UTF-8 byte patterns.
    for (let length = 1; length <= Math.min(3, raw.length); length++) {
      const suffix = raw.subarray(raw.length - length);
      if (this.#decodePrefix(suffix, suffix.length).length === 0) return length;
    }
    return 0;
  }

  #evict(): void {
    const part = this.#parts[this.#head];
    if (part === undefined) throw new Error("Missing output ring entry");
    this.#parts[this.#head] = undefined;
    this.#head = (this.#head + 1) % capacity;
    this.#size--;
    this.#textBytes -= part.textBytes;
    this.#newlines -= part.newlines;
    this.#droppedBytes += part.rawBytes;
  }

  #trimHead(part: TextPart, textBytes: number, newlines: number): void {
    const raw = part.raw;
    if (raw === null) {
      this.#evict();
      return;
    }
    // Find the first decoder emission that pays both clipping debts. A native
    // decode can emit replacement + ASCII together, or emit a replacement
    // while buffering the next scalar. Keep those same emission boundaries.
    const enough = (length: number) => {
      const prefix = this.#decodePrefix(raw, length);
      return (
        encoder.encode(prefix).length >= textBytes &&
        prefix.split("\n").length - 1 >= newlines
      );
    };
    let high = Math.min(raw.length, Math.max(4, textBytes));
    while (high < raw.length && !enough(high))
      high = Math.min(raw.length, high * 2);
    let low = 0;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      if (enough(middle)) high = middle;
      else low = middle + 1;
    }
    const prefix = this.#decodePrefix(raw, low);
    if (prefix.length === part.text.length) {
      this.#evict();
      return;
    }
    const cut = low - this.#pendingLength(raw.subarray(0, low));
    const removedBytes = encoder.encode(prefix).length;
    const removedLines = prefix.split("\n").length - 1;
    part.raw = raw.slice(cut);
    // Decode the bounded remainder anew so a sliced string cannot retain the
    // larger temporary chunk. raw.slice also releases its original buffer.
    part.text = this.#decodePrefix(part.raw, part.raw.length);
    part.rawBytes -= cut;
    part.textBytes -= removedBytes;
    part.newlines -= removedLines;
    this.#textBytes -= removedBytes;
    this.#newlines -= removedLines;
    this.#droppedBytes += cut;
  }

  #append(text: string, rawBytes: number, raw: Uint8Array | null): void {
    if (text.length === 0) return;
    const part = {
      text,
      raw,
      rawBytes,
      textBytes: encoder.encode(text).length,
      newlines: text.split("\n").length - 1,
    };
    if (this.#size === capacity) this.#evict();
    this.#parts[(this.#head + this.#size) % capacity] = part;
    this.#size++;
    this.#textBytes += part.textBytes;
    this.#newlines += part.newlines;
    while (this.#textBytes > capacity || this.#newlines >= 200) {
      const head = this.#parts[this.#head];
      if (head === undefined) throw new Error("Missing output ring entry");
      const bytesToDrop = Math.max(0, this.#textBytes - capacity);
      const linesToDrop = Math.max(0, this.#newlines - 199);
      if (head.textBytes <= bytesToDrop || head.newlines < linesToDrop)
        this.#evict();
      else this.#trimHead(head, bytesToDrop, linesToDrop);
    }
  }

  snapshot() {
    let tail = "";
    for (let i = 0; i < this.#size; i++) {
      const part = this.#parts[(this.#head + i) % capacity];
      if (part === undefined) throw new Error("Missing output ring entry");
      tail += part.text;
    }
    return {
      tail,
      droppedBytes: this.#droppedBytes,
      totalBytes: this.#totalBytes,
    };
  }

  #update() {
    if (!this.#dirty) return undefined;
    this.#dirty = false;
    const { tail, ...data } = this.snapshot();
    return {
      type: "update",
      result: {
        isError: false,
        content: [{ type: "text", text: tail }],
        data,
      },
    } satisfies MuleToolExecutionPart;
  }

  /** EOF flush order is stdout then stderr; the final update precedes terminal. */
  finish() {
    if (this.#closed) return undefined;
    this.#closed = true;
    for (const producer of [this.#producers.stdout, this.#producers.stderr]) {
      const text = producer.decoder.decode();
      this.#append(text, producer.pending.length, null);
      producer.pending = new Uint8Array(0);
      if (text.length > 0) this.#dirty = true;
    }
    return this.#update();
  }

  get updates() {
    return Stream.fromQueue(this.wake).pipe(
      Stream.mapEffect(() =>
        Effect.sleep("100 millis").pipe(
          Effect.map(() => (this.#closed ? undefined : this.#update())),
        ),
      ),
      Stream.filter((part) => part !== undefined),
    );
  }

  dispose(): void {
    this.#closed = true;
    this.#parts.fill(undefined);
    this.#head = this.#size = this.#textBytes = this.#newlines = 0;
    this.#totalBytes = this.#droppedBytes = 0;
    this.#dirty = false;
    for (const producer of [this.#producers.stdout, this.#producers.stderr]) {
      producer.decoder.decode();
      producer.pending = new Uint8Array(0);
    }
  }
}
