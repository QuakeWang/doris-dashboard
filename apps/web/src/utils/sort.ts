export type AntdSortOrder = "ascend" | "descend" | null | undefined;

export function compareNullableNumber(
  a: number | null | undefined,
  b: number | null | undefined,
  sortOrder?: AntdSortOrder
): number {
  const av = a == null || !Number.isFinite(a) ? null : a;
  const bv = b == null || !Number.isFinite(b) ? null : b;

  const desired =
    av == null && bv == null
      ? 0
      : av == null
        ? 1
        : bv == null
          ? -1
          : sortOrder === "descend"
            ? bv - av
            : av - bv;

  return sortOrder === "descend" ? -desired : desired;
}
