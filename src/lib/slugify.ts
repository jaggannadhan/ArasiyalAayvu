/**
 * Matches the Python slugify() used in candidate_transparency_ingest.py
 * so URL slugs are consistent with Firestore doc IDs.
 */
export function slugify(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")  // strip diacritics
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "unknown";
}
