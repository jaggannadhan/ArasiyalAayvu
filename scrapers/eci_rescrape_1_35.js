/**
 * ECI Results Re-scraper for ACs 1-35 (Tamil Nadu)
 *
 * Usage:
 *   1. Open https://results.eci.gov.in/ResultAc498/partywiseresult-S22.htm in Chrome
 *   2. Open DevTools Console (Cmd+Option+J / F12)
 *   3. Paste this entire script and press Enter
 *   4. Wait for all 35 ACs to be scraped (~3 min)
 *   5. JSON file downloads automatically when done
 *
 * The output file can be merged with tn_results_2026.json using the merge script.
 */

(async function scrapeACs1to35() {
  const STATE_CODE = "S22";
  const BASE = "https://results.eci.gov.in/ResultAcGenMay2026";
  const START_AC = 1;
  const END_AC = 35;
  const DELAY_MS = 2500; // delay between requests to avoid rate limiting

  const results = [];
  let completed = 0;
  const total = END_AC - START_AC + 1;

  // Progress bar
  const bar = document.createElement("div");
  bar.style.cssText =
    "position:fixed;top:0;left:0;right:0;z-index:99999;background:#111;color:#0f0;font:14px monospace;padding:12px 20px;";
  bar.textContent = `Scraping ACs ${START_AC}-${END_AC}... 0/${total}`;
  document.body.appendChild(bar);

  function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
  }

  for (let ac = START_AC; ac <= END_AC; ac++) {
    const url = `${BASE}/candidateswise-${STATE_CODE}${ac}.htm`;
    try {
      const resp = await fetch(url);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const html = await resp.text();

      const parser = new DOMParser();
      const doc = parser.parseFromString(html, "text/html");

      // Extract AC name from page title or heading
      const heading = doc.querySelector(".page-title, h3, h2, .const-name");
      let acName = "";
      if (heading) {
        // Format is usually "XX - NAME (General/SC/ST)"
        const m = heading.textContent.match(/\d+\s*[-–]\s*(.+?)(?:\s*\(|$)/);
        acName = m ? m[1].trim() : heading.textContent.trim();
      }

      // Parse candidate boxes
      const candBoxes = doc.querySelectorAll(".cand-box, .cand-info, tr[data-cand]");
      const candidates = [];

      if (candBoxes.length > 0) {
        candBoxes.forEach((box) => {
          // Try multiple selector patterns for candidate data
          const nameEl =
            box.querySelector(".cand-name, .cand_name, td:nth-child(2)") ||
            box.querySelector("div:nth-child(2)");
          const partyEl =
            box.querySelector(".party-name, .party_name, td:nth-child(3)") ||
            box.querySelector("div:nth-child(3)");
          const votesEl =
            box.querySelector(".votes, .vote-count, td:nth-child(5)") ||
            box.querySelector("div:nth-child(5)");
          const statusEl = box.querySelector(".status, .result-status");
          const photoEl = box.querySelector("img");

          const name = nameEl ? nameEl.textContent.trim() : "";
          const party = partyEl ? partyEl.textContent.trim() : "";
          const votesText = votesEl ? votesEl.textContent.replace(/[^0-9]/g, "") : "0";
          const votes = parseInt(votesText, 10) || 0;
          const status = statusEl
            ? statusEl.textContent.trim().toLowerCase().includes("won")
              ? "won"
              : "lost"
            : "";
          const photoUrl = photoEl ? photoEl.src : "";

          if (name) {
            candidates.push({
              name,
              party,
              votes,
              status,
              photo_url: photoUrl,
            });
          }
        });
      }

      // Fallback: try table rows if no cand-boxes
      if (candidates.length === 0) {
        const rows = doc.querySelectorAll("table.table tbody tr, table tbody tr");
        rows.forEach((row) => {
          const cells = row.querySelectorAll("td");
          if (cells.length >= 4) {
            const name = cells[1] ? cells[1].textContent.trim() : "";
            const party = cells[2] ? cells[2].textContent.trim() : "";
            const votesText = cells[cells.length - 2]
              ? cells[cells.length - 2].textContent.replace(/[^0-9]/g, "")
              : "0";
            const votes = parseInt(votesText, 10) || 0;
            const statusEl = cells[cells.length - 1];
            const status =
              statusEl && statusEl.textContent.toLowerCase().includes("won") ? "won" : "lost";
            const photoEl = row.querySelector("img");
            if (name) {
              candidates.push({
                name,
                party,
                votes,
                status,
                photo_url: photoEl ? photoEl.src : "",
              });
            }
          }
        });
      }

      // Sort by votes descending
      candidates.sort((a, b) => b.votes - a.votes);

      // Mark winner/runner-up
      if (candidates.length > 0 && !candidates.some((c) => c.status === "won")) {
        candidates[0].status = "won";
      }

      const winner = candidates.find((c) => c.status === "won") || candidates[0] || null;
      const runnerUp = candidates.filter((c) => c !== winner)[0] || null;
      const totalVotes = candidates.reduce((s, c) => s + c.votes, 0);
      const margin = winner && runnerUp ? winner.votes - runnerUp.votes : 0;

      const result = {
        ac_no: ac,
        ac_name: acName || `AC-${ac}`,
        winner: winner
          ? { name: winner.name, party: winner.party, votes: winner.votes, photo_url: winner.photo_url }
          : null,
        runner_up: runnerUp
          ? { name: runnerUp.name, party: runnerUp.party, votes: runnerUp.votes, photo_url: runnerUp.photo_url }
          : null,
        margin,
        total_votes: totalVotes,
        candidates,
        total_candidates: candidates.length,
      };

      results.push(result);
      completed++;
      const w = winner ? `${winner.name} (${winner.party})` : "?";
      bar.textContent = `[${completed}/${total}] AC-${ac} ${acName}: ${w}`;
      console.log(`AC-${ac} ${acName}: ${candidates.length} candidates, winner: ${w}`);

      if (candidates.length === 0) {
        console.warn(`  ⚠ AC-${ac}: No candidates found — page structure may differ`);
        result.error = "no_candidates_parsed";
      }
    } catch (err) {
      console.error(`AC-${ac}: ${err.message}`);
      results.push({ ac_no: ac, error: err.message });
      completed++;
      bar.textContent = `[${completed}/${total}] AC-${ac}: ERROR - ${err.message}`;
    }

    await sleep(DELAY_MS);
  }

  // Build output
  const output = {
    scraped_at: new Date().toISOString(),
    state: "Tamil Nadu",
    state_code: STATE_CODE,
    range: `AC ${START_AC}-${END_AC}`,
    total_acs: results.length,
    results,
  };

  // Download as JSON
  const blob = new Blob([JSON.stringify(output, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `tn_results_2026_ac1-35.json`;
  a.click();
  URL.revokeObjectURL(url);

  bar.style.background = "#050";
  bar.textContent = `Done! ${results.length} ACs scraped. File downloaded.`;
  console.log("=== DONE ===", JSON.stringify(output, null, 2));
})();
