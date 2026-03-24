import { createHash } from "crypto";

process.env.PLAYWRIGHT_BROWSERS_PATH = "0";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function parseRelativeDate(dateText) {
  const text = String(dateText || "").trim().toLowerCase();
  if (!text) {
    return null;
  }

  if (text.includes("hoy") || text.includes("today")) {
    return 0;
  }
  if (text.includes("ayer") || text.includes("yesterday")) {
    return 1;
  }

  const esMatch = text.match(/hace\s+(\d+)\s+(día|días|semana|semanas|mes|meses|año|años)/i);
  if (esMatch) {
    const value = Number(esMatch[1]);
    const unit = esMatch[2].toLowerCase();
    if (unit.startsWith("día")) return value;
    if (unit.startsWith("semana")) return value * 7;
    if (unit.startsWith("mes")) return value * 30;
    if (unit.startsWith("año")) return value * 365;
  }

  const enMatch = text.match(/(\d+)\s+(day|days|week|weeks|month|months|year|years)\s+ago/i);
  if (enMatch) {
    const value = Number(enMatch[1]);
    const unit = enMatch[2].toLowerCase();
    if (unit.startsWith("day")) return value;
    if (unit.startsWith("week")) return value * 7;
    if (unit.startsWith("month")) return value * 30;
    if (unit.startsWith("year")) return value * 365;
  }

  return null;
}

function reviewId(userName, comment, dateText) {
  return createHash("sha256")
    .update(`${userName}|${comment}|${dateText}`)
    .digest("hex")
    .slice(0, 20);
}

async function openReviewsPanel(page) {
  await handleConsentIfPresent(page);

  // If reviews are already visible in-place, keep going.
  if (
    (await page.locator('div[role="article"]').count()) > 0 ||
    (await page.locator("div.jftiEf").count()) > 0
  ) {
    return true;
  }

  // In some Maps layouts, a "Más reseñas" / "More reviews" control opens the full list.
  const moreReviews = page
    .locator('button:has-text("Más reseñas"), a:has-text("Más reseñas"), button:has-text("More reviews"), a:has-text("More reviews")')
    .first();
  if ((await moreReviews.count()) > 0) {
    try {
      await moreReviews.click({ timeout: 7000 });
      await page.waitForTimeout(1200);
      if (
        (await page.locator('div[role="feed"]').count()) > 0 ||
        (await page.locator('div[role="article"]').count()) > 0 ||
        (await page.locator("div.jftiEf").count()) > 0
      ) {
        return true;
      }
    } catch {
      // continue with other selectors
    }
  }

  // Fallback: click visible rating element to trigger reviews modal.
  const clickedRating = await page.evaluate(() => {
    const candidates = Array.from(
      document.querySelectorAll('[aria-label*="estrellas"], [aria-label*="stars"]')
    );
    for (const node of candidates) {
      const rect = node.getBoundingClientRect();
      if (rect.width <= 0 || rect.height <= 0) continue;
      let clickable = node;
      while (clickable && clickable !== document.body) {
        if (
          clickable.tagName === "BUTTON" ||
          clickable.tagName === "A" ||
          clickable.getAttribute("role") === "button" ||
          clickable.hasAttribute("jsaction")
        ) {
          clickable.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
          return true;
        }
        clickable = clickable.parentElement;
      }
    }
    return false;
  });
  if (clickedRating) {
    await page.waitForTimeout(1200);
    if (
      (await page.locator('div[role="feed"]').count()) > 0 ||
      (await page.locator('div[role="article"]').count()) > 0 ||
      (await page.locator("div.jftiEf").count()) > 0
    ) {
      return true;
    }
  }

  const selectors = [
    'button[jsaction*="pane.reviewChart"]',
    'button[aria-label*="reseñas"]',
    'button[aria-label*="reviews"]',
    'button:has-text("Reseñas")',
    'button:has-text("Reviews")',
    'a:has-text("Reseñas")',
    'a:has-text("Reviews")',
  ];

  let opened = false;
  for (const selector of selectors) {
    const target = page.locator(selector).first();
    if ((await target.count()) === 0) {
      continue;
    }
    try {
      await target.click({ timeout: 7000 });
      opened = true;
      break;
    } catch {
      // keep trying next selector
    }
  }

  if (!opened) {
    return false;
  }

  if ((await page.locator('div[role="feed"]').count()) > 0) {
    await page.locator('div[role="feed"]').first().waitFor({ timeout: 15000 });
  } else if ((await page.locator('div[role="article"]').count()) > 0) {
    await page.locator('div[role="article"]').first().waitFor({ timeout: 15000 });
  } else if ((await page.locator("div.jftiEf").count()) > 0) {
    await page.locator("div.jftiEf").first().waitFor({ timeout: 15000 });
  }
  return true;
}

async function handleConsentIfPresent(page) {
  const acceptCandidates = [
    page.locator('button:has-text("Aceptar todo")').first(),
    page.locator('button:has-text("Acepto")').first(),
    page.locator('button:has-text("Accept all")').first(),
    page.locator('button:has-text("I agree")').first(),
  ];

  for (const candidate of acceptCandidates) {
    if ((await candidate.count()) > 0) {
      try {
        await candidate.click({ timeout: 3000 });
        await page.waitForTimeout(800);
        return;
      } catch {
        // continue trying
      }
    }
  }
}

async function sortByMostRecent(page) {
  const targetLabels = ["más recientes", "newest", "most recent"];

  async function openSortMenu() {
    const sortButton = page
      .locator(
        'button[aria-label*="Ordenar"], button[aria-label*="Sort"], button:has-text("Ordenar"), button:has-text("Sort")'
      )
      .first();

    if (await sortButton.count()) {
      await sortButton.click({ timeout: 7000 });
      return true;
    }

    const altSort = page.locator('button[jsaction*="sort"]').first();
    if (await altSort.count()) {
      await altSort.click({ timeout: 7000 });
      return true;
    }

    return false;
  }

  async function getMenuOptions() {
    const options = page.locator('div[role="menu"] [role="menuitemradio"]');
    const count = await options.count();
    const rows = [];

    for (let i = 0; i < count; i += 1) {
      const option = options.nth(i);
      const text = ((await option.textContent()) || "").trim();
      const checked = ((await option.getAttribute("aria-checked")) || "").toLowerCase() === "true";
      if (!text) continue;
      rows.push({ index: i, text, checked });
    }
    return rows;
  }

  async function isMostRecentSelected() {
    const rows = await getMenuOptions();
    return rows.some((row) => {
      if (!row.checked) return false;
      const normalized = row.text.toLowerCase();
      return targetLabels.some((label) => normalized.includes(label));
    });
  }

  async function clickMostRecentOption() {
    const rows = await getMenuOptions();
    const target = rows.find((row) => {
      const normalized = row.text.toLowerCase();
      return targetLabels.some((label) => normalized.includes(label));
    });

    if (!target) return false;

    const options = page.locator('div[role="menu"] [role="menuitemradio"]');
    await options.nth(target.index).click({ timeout: 5000 });
    return true;
  }

  const opened = await openSortMenu();
  if (!opened) {
    return;
  }

  await sleep(500);
  const clicked = await clickMostRecentOption();
  if (!clicked) {
    // Keep default order when option not available in this locale/layout.
    await page.keyboard.press("Escape").catch(() => {});
    return;
  }

  await sleep(1200);

  // Validation pass: reopen and check selected option.
  const reopened = await openSortMenu();
  if (!reopened) return;
  await sleep(400);
  const selected = await isMostRecentSelected();
  const menuRows = await getMenuOptions();
  console.info("ABIS sort menu status", {
    selected_most_recent: selected,
    options: menuRows,
  });
  await page.keyboard.press("Escape").catch(() => {});
}

async function scrollReviewsFeed(page, cycles = 12) {
  if ((await page.locator('div[role="feed"]').count()) > 0) {
    const feed = page.locator('div[role="feed"]').first();
    await feed.waitFor({ timeout: 10000 });

    for (let i = 0; i < cycles; i += 1) {
      await feed.evaluate((element) => {
        element.scrollBy(0, Math.floor(element.clientHeight * 0.8));
      });
      await sleep(1300);
    }
    return;
  }

  // Fallback: find scrollable parent around visible review cards.
  const articles = page.locator('div[role="article"], div.jftiEf');
  if ((await articles.count()) === 0) {
    return;
  }

  for (let i = 0; i < cycles; i += 1) {
    await articles.first().evaluate((article) => {
      let node = article.parentElement;
      while (node) {
        const el = node;
        if (el.scrollHeight > el.clientHeight + 40) {
          el.scrollBy(0, Math.floor(el.clientHeight * 0.8));
          break;
        }
        node = node.parentElement;
      }
    });
    await sleep(1300);
  }
}

async function extractReviews(page, locationUrl) {
  async function firstText(node, selector) {
    const locator = node.locator(selector).first();
    if ((await locator.count()) === 0) {
      return "";
    }
    return ((await locator.textContent()) || "").trim();
  }

  async function firstAttr(node, selector, attr) {
    const locator = node.locator(selector).first();
    if ((await locator.count()) === 0) {
      return "";
    }
    return (await locator.getAttribute(attr)) || "";
  }

  const items = page.locator('div[role="article"], div.jftiEf');
  const count = await items.count();
  const out = [];

  for (let i = 0; i < count; i += 1) {
    const node = items.nth(i);
    const userName =
      (await firstText(node, ".d4r55")) ||
      (await firstText(node, 'button[aria-label*="perfil"], a[aria-label*="perfil"]')) ||
      "";

    const ratingLabel =
      (await firstAttr(
        node,
        'span[role="img"][aria-label*="estrellas"], span[role="img"][aria-label*="stars"]',
        "aria-label"
      )) ||
      "";
    const ratingMatch = String(ratingLabel).match(/(\d+([.,]\d+)?)/);
    const rating = ratingMatch ? Number(ratingMatch[1].replace(",", ".")) : 0;

    const comment =
      (await firstText(node, ".wiI7pd")) ||
      (await firstText(node, 'span[jscontroller*="MZnM8e"]')) ||
      "";

    const dateText =
      (await firstText(node, ".rsqaWe")) ||
      (await firstText(node, 'span[class*="rsqaWe"]')) ||
      "";

    if (!userName && !comment && !dateText) {
      continue;
    }

    out.push({
      id: reviewId(userName, comment, dateText),
      user_name: userName,
      rating,
      comment,
      date_text: dateText,
      location_url: locationUrl,
    });
  }

  return out;
}

export async function scrapeGoogleMapsReviews(locationUrl, options = {}) {
  const maxDays =
    Number.isFinite(Number(options?.maxDays)) && Number(options.maxDays) > 0
      ? Number(options.maxDays)
      : 30;
  const fallbackLimit =
    Number.isFinite(Number(options?.fallbackLimit)) && Number(options.fallbackLimit) > 0
      ? Number(options.fallbackLimit)
      : 25;
  const scrollCycles =
    Number.isFinite(Number(options?.scrollCycles)) && Number(options.scrollCycles) > 0
      ? Number(options.scrollCycles)
      : 12;
  const { chromium } = await import("playwright");
  const headless = process.env.MAPS_HEADLESS !== "false";
  const usePersistent = process.env.MAPS_USE_PERSISTENT_CONTEXT === "true";
  const profileDir = process.env.MAPS_PROFILE_DIR || ".pw-profile";
  const launchOptions = {
    headless,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-blink-features=AutomationControlled",
    ],
  };

  let browser = null;
  let context = null;
  let page = null;
  try {
    if (usePersistent) {
      context = await chromium.launchPersistentContext(profileDir, {
        ...launchOptions,
        channel: "chrome",
        locale: "es-MX",
      });
      page = context.pages()[0] || (await context.newPage());
    } else {
      browser = await chromium.launch(launchOptions);
      context = await browser.newContext({
        locale: "es-MX",
        userAgent:
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      });
      page = await context.newPage();
    }

    await page.goto(locationUrl, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });
    await page.waitForTimeout(2500);

    const reviewsPanelOpened = await openReviewsPanel(page);
    if (!reviewsPanelOpened) {
      const bodyText = ((await page.textContent("body")) || "").toLowerCase();
      if (bodyText.includes("vista limitada")) {
        throw new Error(
          "Google Maps returned 'vista limitada' and blocked reviews. Use persistent Chrome profile (MAPS_USE_PERSISTENT_CONTEXT=true, MAPS_HEADLESS=false) and sign in."
        );
      }
      return {
        reviews: [],
        filter_mode: "fallback_recent",
        total_extracted: 0,
      };
    }
    await sortByMostRecent(page);
    await scrollReviewsFeed(page, scrollCycles);

    const allReviews = await extractReviews(page, locationUrl);
    const reviewsWithinRange = allReviews.filter((review) => {
      const days = parseRelativeDate(review.date_text);
      return days !== null && days <= maxDays;
    });

    const selected =
      reviewsWithinRange.length > 0 ? reviewsWithinRange : allReviews.slice(0, fallbackLimit);

    return {
      reviews: selected,
      filter_mode: reviewsWithinRange.length > 0 ? `within_${maxDays}_days` : "fallback_recent",
      total_extracted: allReviews.length,
      applied_max_days: maxDays,
      applied_fallback_limit: fallbackLimit,
    };
  } finally {
    if (context) {
      await context.close().catch(() => {});
    }
    if (browser) {
      await browser.close().catch(() => {});
    }
  }
}
