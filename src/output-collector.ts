import type { MuleToolExecutionPart } from "@fungi.computer/mule/effect";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

const capacity = 8192;
const encoder = new TextEncoder();
// fatal: false is the existing default; Cloudflare's type requires the field.
const decoder = () =>
  new TextDecoder("utf-8", { ignoreBOM: true, fatal: false });
type Producer = { decoder: ReturnType<typeof decoder>; pendingBytes: number };
type TextPart = {
  text: string;
  rawBytes: number;
  textBytes: number;
  newlines: number;
};

/** Complete decoded text, in completion order across stdout/stderr, not byte order. */
export class OutputCollector {
  readonly #producers = {
    stdout: { decoder: decoder(), pendingBytes: 0 },
    stderr: { decoder: decoder(), pendingBytes: 0 },
  } satisfies Record<"stdout" | "stderr", Producer>;
  readonly #probe = decoder();
  // One bounded ring of complete decoder emissions. No UTF-8 slicing/parser.
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
    // Byte-sized decoder inputs let each retained emission carry its exact raw
    // cost, even when malformed input expands to replacement characters.
    for (let i = 0; i < bytes.length; i++) {
      const byte = bytes.subarray(i, i + 1);
      producer.pendingBytes++;
      const text = producer.decoder.decode(byte, { stream: true });
      if (text.length === 0) continue;
      // A decoder may emit a replacement for a prior incomplete sequence while
      // buffering the current byte as the start of the next scalar. Ask the
      // maintained decoder, rather than duplicating UTF-8 byte grammar.
      const pending =
        producer.pendingBytes > 1 &&
        this.#probe.decode(byte, { stream: true }).length === 0
          ? 1
          : 0;
      this.#probe.decode();
      this.#append(text, producer.pendingBytes - pending);
      producer.pendingBytes = pending;
    }
    this.#dirty = true;
    Queue.offerUnsafe(this.wake, undefined);
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

  #append(text: string, rawBytes: number): void {
    if (text.length === 0) return;
    const part = {
      text,
      rawBytes,
      textBytes: encoder.encode(text).length,
      newlines: text.split("\n").length - 1,
    };
    if (this.#size === capacity) this.#evict();
    this.#parts[(this.#head + this.#size) % capacity] = part;
    this.#size++;
    this.#textBytes += part.textBytes;
    this.#newlines += part.newlines;
    while (this.#textBytes > capacity || this.#newlines >= 200) this.#evict();
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
      this.#append(text, producer.pendingBytes);
      producer.pendingBytes = 0;
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
      producer.pendingBytes = 0;
    }
  }
}
