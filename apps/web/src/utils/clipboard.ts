export async function writeTextToClipboard(text: string): Promise<void> {
  const value = String(text ?? "");
  if (!value) return;

  const navClipboard = typeof navigator !== "undefined" ? navigator.clipboard : undefined;
  if (navClipboard?.writeText && typeof window !== "undefined" && window.isSecureContext) {
    await navClipboard.writeText(value);
    return;
  }

  if (typeof document === "undefined") throw new Error("Clipboard is unavailable");

  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.setAttribute("readonly", "true");
  textarea.style.position = "fixed";
  textarea.style.top = "0";
  textarea.style.left = "0";
  textarea.style.width = "1px";
  textarea.style.height = "1px";
  textarea.style.padding = "0";
  textarea.style.border = "0";
  textarea.style.outline = "0";
  textarea.style.boxShadow = "none";
  textarea.style.opacity = "0";

  document.body.appendChild(textarea);
  textarea.select();
  const ok = document.execCommand("copy");
  textarea.remove();
  if (!ok) throw new Error("Copy failed");
}
