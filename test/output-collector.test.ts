import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { expect, it } from "vitest";
import { OutputCollector } from "../src/output-collector.js";

const encode = (text: string) => new TextEncoder().encode(text);

it.each(["stdout", "stderr"] as const)(
  "keeps a split %s scalar independent of the other stream and clips complete text",
  (stream) => {
    const collector = Effect.runSync(OutputCollector.make());
    const other = stream === "stdout" ? "stderr" : "stdout";
    const euro = encode("€");
    collector.write(stream, euro.subarray(0, 2));
    collector.write(other, encode("warning\n"));
    expect(collector.snapshot()).toEqual({
      tail: "warning\n",
      totalBytes: 10,
      droppedBytes: 0,
    });
    collector.write(stream, euro.subarray(2));
    expect(collector.snapshot()).toEqual({
      tail: "warning\n€",
      totalBytes: 11,
      droppedBytes: 0,
    });
    collector.write(other, encode("a".repeat(8190)));
    expect(collector.snapshot()).toEqual({
      tail: "a".repeat(8190),
      totalBytes: 8201,
      droppedBytes: 11,
    });
  },
);

it("flushes incomplete stream suffixes in stdout/stderr order once before terminal and rejects late writes", () => {
  const collector = Effect.runSync(OutputCollector.make());
  collector.write("stderr", new Uint8Array([0xe2, 0x82]));
  collector.write("stdout", new Uint8Array([0xf0, 0x9f]));
  expect(collector.snapshot().tail).toBe("");
  expect(collector.finish()).toMatchObject({
    type: "update",
    result: {
      content: [{ type: "text", text: "\uFFFD\uFFFD" }],
      data: { totalBytes: 4, droppedBytes: 0 },
    },
  });
  expect(collector.finish()).toBeUndefined();
  collector.write("stdout", encode("late"));
  expect(collector.snapshot()).toEqual({
    tail: "\uFFFD\uFFFD",
    totalBytes: 4,
    droppedBytes: 0,
  });
});

it("counts raw bytes when malformed input emits a replacement and starts another scalar", () => {
  const collector = Effect.runSync(OutputCollector.make());
  collector.write("stdout", new Uint8Array([0xe2, 0xe2, 0x82]));
  collector.write("stderr", encode("warning\n"));
  collector.write("stdout", new Uint8Array([0xac]));
  expect(collector.snapshot()).toEqual({
    tail: "\uFFFDwarning\n€",
    totalBytes: 12,
    droppedBytes: 0,
  });
  collector.write("stderr", encode("a".repeat(8189)));
  expect(collector.snapshot()).toEqual({
    tail: `€${"a".repeat(8189)}`,
    totalBytes: 8201,
    droppedBytes: 9,
  });
});

it("retains split UTF-8 scalars and bounds a one MiB write to eight KiB", () => {
  const collector = Effect.runSync(OutputCollector.make());
  const scalar = encode("€");
  collector.write("stdout", scalar.subarray(0, 2));
  expect(collector.snapshot().tail).toBe("");
  collector.write("stdout", scalar.subarray(2));
  expect(collector.snapshot()).toEqual({
    tail: "€",
    totalBytes: 3,
    droppedBytes: 0,
  });
  collector.write("stderr", encode("a".repeat(1024 * 1024)));
  expect(collector.snapshot()).toEqual({
    tail: "a".repeat(8192),
    totalBytes: 1048579,
    droppedBytes: 1040387,
  });
  collector.write("stdout", encode("€".repeat(3000)));
  expect(collector.snapshot().tail).toBe("€".repeat(2730));
  collector.dispose();
  collector.write("stdout", encode("late"));
  expect(collector.snapshot()).toEqual({
    tail: "",
    totalBytes: 0,
    droppedBytes: 0,
  });
});

it("caps lines and binary replacement expansion without changing producer byte counts", () => {
  const lines = Effect.runSync(OutputCollector.make());
  lines.write("stdout", encode("x\n".repeat(500)));
  expect(lines.snapshot()).toEqual({
    tail: "x\n".repeat(199),
    totalBytes: 1000,
    droppedBytes: 602,
  });
  const binary = Effect.runSync(OutputCollector.make());
  binary.write("stdout", new Uint8Array(1024 * 1024).fill(255));
  const snapshot = binary.snapshot();
  expect(encode(snapshot.tail).length).toBeLessThanOrEqual(8192);
  expect(snapshot.tail).toContain("\uFFFD");
  expect(snapshot.totalBytes).toBe(1048576);
  expect(snapshot.droppedBytes).toBeGreaterThan(1040384);
});

it("coalesces all writes within a trailing 100 ms window using Effect's clock", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const collector = yield* OutputCollector.make();
        const tails: string[] = [];
        yield* collector.updates.pipe(
          Stream.runForEach((part) =>
            Effect.sync(() => {
              for (const content of part.result.content)
                tails.push(content.text);
            }),
          ),
          Effect.forkScoped,
        );
        collector.write("stdout", encode("first"));
        yield* TestClock.adjust("99 millis");
        expect(tails).toEqual([]);
        collector.write("stderr", encode(" second"));
        yield* TestClock.adjust("1 millis");
        expect(tails).toEqual(["first second"]);
        for (let i = 0; i < 100; i++) collector.write("stdout", encode("."));
        yield* TestClock.adjust("100 millis");
        expect(tails).toEqual([
          "first second",
          `first second${".".repeat(100)}`,
        ]);
        collector.dispose();
        yield* TestClock.adjust("1 second");
        expect(tails).toHaveLength(2);
      }),
    ).pipe(Effect.provide(TestClock.layer())),
  );
});
