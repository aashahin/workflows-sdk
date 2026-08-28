export const MAX_WORKFLOW_HTTP_ERROR_BYTES = 4_096;
export const MAX_WORKFLOW_HTTP_SUCCESS_BYTES = 1_000_000;

export async function readBoundedResponseText(
  response: Response,
  maxBytes: number,
): Promise<{ text: string; truncated: boolean }> {
  if (!response.body) return { text: "", truncated: false };

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let retained = 0;
  let truncated = false;
  try {
    while (retained <= maxBytes) {
      const next = await reader.read();
      if (next.done) break;
      const remaining = maxBytes - retained;
      if (next.value.byteLength > remaining) {
        if (remaining > 0) chunks.push(next.value.subarray(0, remaining));
        truncated = true;
        break;
      }
      chunks.push(next.value);
      retained += next.value.byteLength;
    }
  } finally {
    if (truncated) await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }

  const bytes = new Uint8Array(
    chunks.reduce((total, chunk) => total + chunk.byteLength, 0),
  );
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return { text: new TextDecoder().decode(bytes), truncated };
}

export function summarizeResponseText(
  text: string,
  truncated = false,
): string {
  const summary = text
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return `${summary}${truncated ? "…" : ""}`;
}
