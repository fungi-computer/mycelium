import { CapabilityFailed } from "./sandbox-failure.js";

/**
 * Execution-scoped image collector, ported from the retained
 * `@shiit/mycelium/execute` collector.
 *
 * Per-execution isolation for out-of-band image content: a connector
 * operation (for example `images.view`) pushes one downsampled image through
 * its `InvocationContext.images`, keyed by the execution's id. After the run,
 * the execute tool drains the collector and appends the entries as
 * `{ type: "image" }` content after the text: the same `MuleToolResult`
 * content any other tool result carries, never a value the guest returns.
 *
 * - Per-execution isolation (the runtime's own execution id).
 * - Bounded capacity per execution ({@link MAX_EXECUTION_IMAGES}).
 * - In memory only: drained once, and cleared on dispose.
 */

/** A single image collected during an execute run. */
export type CollectedImage = Readonly<{
  /** Base64-encoded image bytes. */
  data: string;
  /** MIME type of the collected image. */
  mimeType: "image/jpeg" | "image/png" | "image/webp";
  /** Path the image was read from. */
  path: string;
  /** Byte length of the collected image payload. */
  bytes: number;
}>;

/** The collector as one execution's operations see it: push only. */
export type ExecutionImages = Readonly<{
  /**
   * Push one downsampled image and return its stable reference (`img_0`,
   * `img_1`, ...) to surface as a text handle. Throws {@link CapabilityFailed}
   * once the execution already holds the maximum.
   */
  push: (image: CollectedImage) => string;
}>;

/** Maximum number of out-of-band images retained for one execution. */
export const MAX_EXECUTION_IMAGES = 8;

export class ImageCollector {
  readonly #byExecution = new Map<string, CollectedImage[]>();
  readonly #maxPerExecution: number;

  constructor(maxPerExecution = MAX_EXECUTION_IMAGES) {
    this.#maxPerExecution = maxPerExecution;
  }

  /** Push one image for `executionId` and return its reference id. */
  push(executionId: string, image: CollectedImage): string {
    const images = this.#byExecution.get(executionId) ?? [];
    if (images.length >= this.#maxPerExecution)
      throw new CapabilityFailed({
        namespace: "execute",
        operation: "images",
        cause: {
          kind: "Unavailable",
          message: `Image collector cap reached for this execution (max ${String(this.#maxPerExecution)} images). Process images one at a time or write them to files.`,
        },
      });
    const ref = `img_${String(images.length)}`;
    images.push(Object.freeze({ ...image }));
    this.#byExecution.set(executionId, images);
    return ref;
  }

  /** Drain every image for `executionId` in push order and forget them. */
  drain(executionId: string): readonly CollectedImage[] {
    const images = this.#byExecution.get(executionId);
    if (images === undefined) return [];
    this.#byExecution.delete(executionId);
    return images;
  }

  /** Discard any pending images for `executionId`; safe to call twice. */
  dispose(executionId: string): void {
    this.#byExecution.delete(executionId);
  }
}
