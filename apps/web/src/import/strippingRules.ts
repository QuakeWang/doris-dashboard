export interface StrippingRule {
  name: string;
  pattern: string;
  flags: string;
  replace: string;
}

export interface CompiledStrippingRule {
  name: string;
  regex: RegExp;
  replace: string;
}

export interface StrippingResult {
  stripped: string;
  applied: string[];
}

export const DEFAULT_STRIPPING_RULES: StrippingRule[] = [
  {
    name: "strip_soft_delete_flag",
    pattern: "\\s+and\\s+is_deleted\\s*=\\s*\\?",
    flags: "gi",
    replace: "",
  },
  {
    name: "strip_tenant_id",
    pattern: "\\s+and\\s+tenant_id\\s*=\\s*\\?",
    flags: "gi",
    replace: "",
  },
];

export function compileStrippingRules(rules: StrippingRule[]): CompiledStrippingRule[] {
  return rules.map((r) => ({
    name: r.name,
    regex: new RegExp(r.pattern, r.flags),
    replace: r.replace,
  }));
}

export const DEFAULT_COMPILED_STRIPPING_RULES: CompiledStrippingRule[] =
  compileStrippingRules(DEFAULT_STRIPPING_RULES);

export function applyCompiledStrippingRules(
  sqlTemplate: string,
  rules: CompiledStrippingRule[]
): StrippingResult {
  let s = sqlTemplate;
  const applied: string[] = [];
  for (const r of rules) {
    const next = s.replace(r.regex, r.replace);
    if (next !== s) applied.push(r.name);
    s = next;
  }
  return { stripped: s.replace(/\s+/g, " ").trim(), applied };
}

export function applyStrippingRules(sqlTemplate: string, rules: StrippingRule[]): StrippingResult {
  return applyCompiledStrippingRules(sqlTemplate, compileStrippingRules(rules));
}
