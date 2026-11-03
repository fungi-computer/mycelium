import * as Schema from "effect/Schema";
import type { ConfirmRequest } from "../access.js";

const maxEntries = 6;
const maxText = 120;
const sensitiveName =
  /pass(word|wd)?|secret|token|credential|api[-_]?key|auth|cookie|private|env/i;

const Fields = Schema.Record(Schema.String, Schema.MutableJson);
const Text = Schema.String;
const Scalar = Schema.Union([Schema.Number, Schema.Boolean, Schema.Null]);
const List = Schema.Array(Schema.MutableJson);

type Shown = Readonly<{ text: string; hid: boolean }>;

function describe(name: string, value: Schema.MutableJson): Shown {
  if (sensitiveName.test(name)) return { text: `${name}: [hidden]`, hid: true };
  if (Schema.is(Text)(value)) {
    const long = value.length > maxText;
    const shown = JSON.stringify(long ? value.slice(0, maxText) : value);
    const note = long
      ? ` ... [truncated, ${String(value.length)} characters]`
      : "";
    return { text: `${name}: ${shown}${note}`, hid: long };
  }
  if (Schema.is(Scalar)(value))
    return { text: `${name}: ${String(value)}`, hid: false };
  const size = Schema.is(List)(value)
    ? value.length
    : Object.keys(value).length;
  return {
    text: `${name}: [${Schema.is(List)(value) ? "list" : "object"} of ${String(size)}, not shown]`,
    hid: true,
  };
}

/**
 * The human-visible approval prompt: module, operation and access, plus a
 * bounded view of the top-level input. Sensitive-named fields and nested or
 * long values are hidden, and the prompt says so.
 */
export function confirmationMessage(approval: ConfirmRequest): string {
  const head = `Allow ${approval.module}.${approval.operation} (${approval.access})?`;
  const fields = Schema.decodeUnknownOption(Fields)(approval.input);
  if (fields._tag === "None")
    return `${head}\ninput: [not shown]\nSome detail is hidden.`;
  const entries = Object.entries(fields.value);
  const shown = entries
    .slice(0, maxEntries)
    .map(([name, value]) => describe(name, value));
  const extra = entries.length - maxEntries;
  const hidden = extra > 0 || shown.some((candidate) => candidate.hid);
  return [
    head,
    ...shown.map((candidate) => candidate.text),
    ...(extra > 0 ? [`... ${String(extra)} more fields not shown`] : []),
    ...(hidden ? ["Some detail is hidden."] : []),
  ].join("\n");
}
