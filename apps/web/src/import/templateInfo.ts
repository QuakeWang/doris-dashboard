import { guessTableFromSqlTemplate } from "./sqlTemplate";
import { DEFAULT_COMPILED_STRIPPING_RULES, applyCompiledStrippingRules } from "./strippingRules";

const TEMPLATE_INFO_CACHE_LIMIT = 50_000;
const templateInfoCache = new Map<
  string,
  { strippedTemplate: string; tableGuess: string | null }
>();

export function getTemplateInfo(baseTemplate: string): {
  strippedTemplate: string;
  tableGuess: string | null;
} {
  const cached = templateInfoCache.get(baseTemplate);
  if (cached) return cached;
  const strippedTemplate = applyCompiledStrippingRules(
    baseTemplate,
    DEFAULT_COMPILED_STRIPPING_RULES
  ).stripped;
  const tableGuess = guessTableFromSqlTemplate(baseTemplate);
  const next = { strippedTemplate, tableGuess };
  templateInfoCache.set(baseTemplate, next);
  if (templateInfoCache.size > TEMPLATE_INFO_CACHE_LIMIT) {
    const oldest = templateInfoCache.keys().next().value as string | undefined;
    if (oldest) templateInfoCache.delete(oldest);
  }
  return next;
}
