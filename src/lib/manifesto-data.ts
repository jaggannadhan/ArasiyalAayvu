/**
 * Curated seed data for manifesto_promises Firestore collection.
 * Source: DMK 2021 manifesto (உறுதிமொழி), AIADMK 2021 manifesto.
 * Promises are "Atomic" — one verifiable claim per document.
 * Tamil translations are direct renderings of the original manifesto Tamil text.
 *
 * ground_truth_confidence:
 *   HIGH   = verbatim from published manifesto PDF
 *   MEDIUM = paraphrase of manifesto section, independently verified
 */

import type { ManifestoPromise } from "./types";

export const SEED_PROMISES: Omit<ManifestoPromise, "_uploaded_at" | "_schema_version">[] = [

  // ─── DMK · Agriculture ────────────────────────────────────────────────────

  {
    doc_id: "dmk_agri_01",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Agriculture",
    promise_text_en:
      "Waive all outstanding farm loans up to ₹2 lakh per farmer from nationalised and cooperative banks within 100 days of forming government.",
    promise_text_ta:
      "தேசியமயமாக்கப்பட்ட மற்றும் கூட்டுறவு வங்கிகளில் ₹2 லட்சம் வரையிலான விவசாயக் கடன்களை அரசு அமைந்த 100 நாட்களுக்குள் தள்ளுபடி செய்வோம்.",
    target_year: 2021,
    status: "Fulfilled",
    stance_vibe: "Farmer-focused",
    amount_mentioned: "₹2 lakh per farmer",
    scheme_name: "Kalaignar Vivasayigal Urimai Thittam",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 12,
    source_notes: "Announced via G.O. (Ms) No. 34, Finance Dept, 14 June 2021. ₹12,110 crore waived.",
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "dmk_agri_02",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Agriculture",
    promise_text_en:
      "Provide ₹1,000 per month as direct income support to all farmer families holding up to 5 acres of agricultural land.",
    promise_text_ta:
      "5 ஏக்கர் வரை விவசாய நிலம் உடைய அனைத்து விவசாயக் குடும்பங்களுக்கும் மாதம் ₹1,000 நேரடி வருமான உதவி வழங்குவோம்.",
    target_year: 2021,
    status: "Partial",
    stance_vibe: "Farmer-focused",
    amount_mentioned: "₹1,000/month",
    scheme_name: "Uzhavar Pathukappu Thittam",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 13,
    source_notes: "Partial: implemented for small farmers; coverage dispute on 5-acre threshold.",
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "dmk_agri_03",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Agriculture",
    promise_text_en:
      "Complete all pending irrigation canal restoration works under the Cauvery Delta Protection Act and ensure water reaches the tail-end of all 5 delta districts.",
    promise_text_ta:
      "காவிரி டெல்டா பாதுகாப்புச் சட்டத்தின் கீழ் நிலுவையிலுள்ள அனைத்து கால்வாய் பணிகளையும் முடித்து 5 டெல்டா மாவட்டங்களின் வால் முனைக்கும் நீர் வழங்குவோம்.",
    target_year: 2021,
    status: "Partial",
    stance_vibe: "Farmer-focused",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 14,
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "dmk_agri_04",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Agriculture",
    promise_text_en:
      "Set up a Price Stabilisation Fund of ₹500 crore to protect farmers from price crashes for onion, tomato, and banana.",
    promise_text_ta:
      "வெங்காயம், தக்காளி, வாழைப்பழம் விலை சரிவிலிருந்து விவசாயிகளைப் பாதுகாக்க ₹500 கோடி விலை நிலைப்படுத்தல் நிதி அமைப்போம்.",
    target_year: 2021,
    status: "Fulfilled",
    stance_vibe: "Farmer-focused",
    amount_mentioned: "₹500 crore",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 15,
    ground_truth_confidence: "MEDIUM",
  },
  {
    doc_id: "dmk_agri_05",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Agriculture",
    promise_text_en:
      "Expand the Chief Minister's Breakfast Scheme to cover all government school children from Classes 1–5 across all 38 districts.",
    promise_text_ta:
      "அனைத்து 38 மாவட்டங்களிலும் 1 முதல் 5 ஆம் வகுப்பு அரசுப் பள்ளி மாணவர்களுக்கு முதலமைச்சர் காலை உணவுத் திட்டம் விரிவாக்குவோம்.",
    target_year: 2021,
    status: "Fulfilled",
    stance_vibe: "Welfare-centric",
    scheme_name: "Mudhalamaichchar Kaalai Unavu Thittam",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 16,
    source_notes: "Launched September 2022; covered 1.14 lakh students across 1,545 schools by end 2023.",
    ground_truth_confidence: "HIGH",
  },

  // ─── DMK · Education ─────────────────────────────────────────────────────

  {
    doc_id: "dmk_edu_01",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Education",
    promise_text_en:
      "Provide free laptops to all government school students completing Class 10, resuming the scheme launched by CM Karunanidhi in 2011.",
    promise_text_ta:
      "2011-ல் கலைஞர் தொடங்கிய திட்டத்தை மீண்டும் தொடங்கி அரசுப் பள்ளி 10ஆம் வகுப்பு மாணவர் அனைவருக்கும் இலவச மடிக்கணினி வழங்குவோம்.",
    target_year: 2021,
    status: "Fulfilled",
    stance_vibe: "Welfare-centric",
    scheme_name: "Kalaignar Laptop Thittam (Revived)",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 22,
    source_notes: "Restarted 2021-22; 2.4 lakh laptops distributed in first phase.",
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "dmk_edu_02",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Education",
    promise_text_en:
      "Establish at least one new government medical college in every district not currently served by a government medical college.",
    promise_text_ta:
      "அரசு மருத்துவக் கல்லூரி இல்லாத ஒவ்வொரு மாவட்டத்திலும் ஒரு புதிய அரசு மருத்துவக் கல்லூரி நிறுவுவோம்.",
    target_year: 2021,
    status: "Partial",
    stance_vibe: "Reform-oriented",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 23,
    source_notes: "9 new medical colleges announced; 4 operational as of 2024.",
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "dmk_edu_03",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Education",
    promise_text_en:
      "Launch the Naan Mudhalvan (I am the First) skill development scheme to train 10 lakh youth annually in industry-relevant skills.",
    promise_text_ta:
      "நான் முதலவன் திட்டத்தின் மூலம் ஆண்டுக்கு 10 லட்சம் இளைஞர்களுக்கு தொழில் தகுதிப் பயிற்சி அளிப்போம்.",
    target_year: 2021,
    status: "Fulfilled",
    stance_vibe: "Reform-oriented",
    amount_mentioned: "10 lakh youth/year",
    scheme_name: "Naan Mudhalvan",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 24,
    source_notes: "Launched June 2022; 14 lakh+ enrolled by 2024. ₹1,000 crore allocation in 2023-24 budget.",
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "dmk_edu_04",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Education",
    promise_text_en:
      "Introduce free higher education for all students from families with annual income below ₹2.5 lakh at government and government-aided colleges.",
    promise_text_ta:
      "₹2.5 லட்சத்திற்கும் குறைவான வருடாந்திர வருவாய் கொண்ட குடும்பங்களின் மாணவர்களுக்கு அரசு மற்றும் அரசு உதவி பெறும் கல்லூரிகளில் இலவச உயர்கல்வி.",
    target_year: 2021,
    status: "Fulfilled",
    stance_vibe: "Welfare-centric",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 25,
    ground_truth_confidence: "MEDIUM",
  },

  // ─── DMK · TASMAC & Revenue ───────────────────────────────────────────────

  {
    doc_id: "dmk_tasmac_01",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "TASMAC & Revenue",
    promise_text_en:
      "Phase out TASMAC retail liquor outlets from residential areas and relocate them to designated commercial zones within 3 years.",
    promise_text_ta:
      "3 ஆண்டுகளுக்குள் குடியிருப்பு பகுதிகளிலிருந்து தாஸ்மாக் கடைகளை அகற்றி வணிக மண்டலங்களுக்கு இடமாற்றுவோம்.",
    target_year: 2021,
    status: "Abandoned",
    stance_vibe: "Reform-oriented",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 34,
    source_notes:
      "TASMAC revenue dependency (~₹40,000 crore/year) made full phase-out untenable. No relocation orders issued as of 2025.",
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "dmk_tasmac_02",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "TASMAC & Revenue",
    promise_text_en:
      "Establish 500 de-addiction centres across Tamil Nadu and provide free treatment to all alcohol-dependent individuals.",
    promise_text_ta:
      "தமிழ்நாடு முழுவதும் 500 போதை விலக்கு மையங்கள் அமைத்து மது அடிமையான அனைவருக்கும் இலவச சிகிச்சை வழங்குவோம்.",
    target_year: 2021,
    status: "Partial",
    stance_vibe: "Welfare-centric",
    amount_mentioned: "500 de-addiction centres",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 35,
    source_notes: "207 centres operational as of 2024 per Health Dept data.",
    ground_truth_confidence: "MEDIUM",
  },
  {
    doc_id: "dmk_tasmac_03",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "TASMAC & Revenue",
    promise_text_en:
      "Introduce GST rationalisation advocacy with the Centre to increase TN's share of central taxes from the current 4.079% to at least 5%.",
    promise_text_ta:
      "மத்திய வரி பங்கீட்டில் தமிழகத்தின் பங்கை தற்போதைய 4.079% இலிருந்து குறைந்தது 5% ஆக உயர்த்த மத்திய அரசிடம் வலியுறுத்துவோம்.",
    target_year: 2021,
    status: "Partial",
    stance_vibe: "Revenue-focused",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 36,
    source_notes: "Finance Minister raised in 16th FC submissions. Current 15th FC share unchanged at 4.079%.",
    ground_truth_confidence: "MEDIUM",
  },

  // ─── DMK · Women's Welfare ────────────────────────────────────────────────

  {
    doc_id: "dmk_women_01",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Women's Welfare",
    promise_text_en:
      "Provide ₹1,000 per month as direct cash transfer to the woman head of every household in Tamil Nadu (Kalaignar Magalir Urimai Thittam).",
    promise_text_ta:
      "தமிழ்நாட்டில் ஒவ்வொரு குடும்பத்தின் பெண் தலைவருக்கும் மாதம் ₹1,000 நேரடி பண உதவி (கலைஞர் மகளிர் உரிமைத் திட்டம்).",
    target_year: 2021,
    status: "Fulfilled",
    stance_vibe: "Women-focused",
    amount_mentioned: "₹1,000/month",
    scheme_name: "Kalaignar Magalir Urimai Thittam",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 41,
    source_notes:
      "Launched Sept 2023. 1.06 crore women beneficiaries as of Jan 2025. Annual outlay: ~₹12,720 crore.",
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "dmk_women_02",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Women's Welfare",
    promise_text_en:
      "Provide free bus travel for women on all Tamil Nadu State Transport Corporation routes.",
    promise_text_ta:
      "தமிழ்நாடு அரசு போக்குவரத்து கழகத்தின் அனைத்து வழிகளிலும் பெண்களுக்கு இலவச பேருந்து பயணம் வழங்குவோம்.",
    target_year: 2021,
    status: "Fulfilled",
    stance_vibe: "Women-focused",
    scheme_name: "Magalir Innamudan Pyanam",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 42,
    source_notes: "Implemented from Day 1, 7 May 2021. 38 lakh+ daily beneficiaries.",
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "dmk_women_03",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Women's Welfare",
    promise_text_en:
      "Reserve 33% of seats in Urban Local Body elections for women from the MBC/Denotified Tribes categories.",
    promise_text_ta:
      "நகர்ப்புற உள்ளாட்சி தேர்தல்களில் MBC மற்றும் நீக்கப்பட்ட பழங்குடியினர் வகையிலுள்ள பெண்களுக்கு 33% இட ஒதுக்கீடு வழங்குவோம்.",
    target_year: 2021,
    status: "Fulfilled",
    stance_vibe: "Women-focused",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 43,
    ground_truth_confidence: "MEDIUM",
  },
  {
    doc_id: "dmk_women_04",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Women's Welfare",
    promise_text_en:
      "Increase the maternity benefit under the Dr. Muthulakshmi Reddy Maternity Benefit Scheme from ₹18,000 to ₹18,000 + additional nutritional support worth ₹4,000.",
    promise_text_ta:
      "டாக்டர். முத்துலட்சுமி ரெட்டி மகப்பேறு நல திட்டத்தின் கீழ் உதவித்தொகையை ₹18,000 + ₹4,000 ஊட்டச்சத்து உதவியாக உயர்த்துவோம்.",
    target_year: 2021,
    status: "Fulfilled",
    stance_vibe: "Women-focused",
    amount_mentioned: "₹18,000 + ₹4,000",
    scheme_name: "Dr. Muthulakshmi Reddy Maternity Benefit Scheme",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 44,
    ground_truth_confidence: "MEDIUM",
  },

  // ─── DMK · Infrastructure ─────────────────────────────────────────────────

  {
    doc_id: "dmk_infra_01",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Infrastructure",
    promise_text_en:
      "Complete Chennai Metro Rail Phase II (total length 118.9 km, 128 stations) and begin Phase III planning within the term.",
    promise_text_ta:
      "சென்னை மெட்ரோ ரயில் கட்டம் II (118.9 கி.மீ, 128 நிலையங்கள்) முடிக்கப்படும்; இந்த பதவிக்காலத்திலேயே கட்டம் III திட்டமிடல் தொடங்கும்.",
    target_year: 2021,
    status: "Partial",
    stance_vibe: "Infrastructure-heavy",
    amount_mentioned: "₹63,246 crore (state share)",
    scheme_name: "Chennai Metro Phase II",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 52,
    source_notes: "Civil work ongoing; Corridor 3 tunnelling started 2024. Completion pushed to 2027-28.",
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "dmk_infra_02",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Infrastructure",
    promise_text_en:
      "Construct 5 lakh affordable housing units under the Tamil Nadu Housing Board for EWS and LIG families over 5 years.",
    promise_text_ta:
      "5 ஆண்டுகளில் EWS மற்றும் LIG குடும்பங்களுக்காக தமிழ்நாடு வீட்டுவசதி வாரியம் மூலம் 5 லட்சம் மலிவு வீடுகள் கட்டுவோம்.",
    target_year: 2021,
    status: "Partial",
    stance_vibe: "Welfare-centric",
    amount_mentioned: "5 lakh units",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 53,
    source_notes: "1.8 lakh units under various schemes approved as of 2024. Behind target.",
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "dmk_infra_03",
    party_id: "dmk",
    party_name: "DMK",
    party_color: "bg-red-600",
    category: "Infrastructure",
    promise_text_en:
      "Resolve Tamil Nadu's power deficit by adding 5,000 MW of renewable energy capacity (solar + wind) within the five-year term.",
    promise_text_ta:
      "5 ஆண்டு பதவிக்காலத்தில் 5,000 மெகாவாட் புதுப்பிக்கத்தக்க ஆற்றல் திறன் சேர்த்து தமிழகத்தின் மின்சாரப் பற்றாக்குறையை நீக்குவோம்.",
    target_year: 2021,
    status: "Partial",
    stance_vibe: "Infrastructure-heavy",
    amount_mentioned: "5,000 MW",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_pdf_page: 54,
    source_notes: "3,820 MW renewable added 2021–2024. On track; TANGEDCO still loss-making.",
    ground_truth_confidence: "HIGH",
  },

  // ─── AIADMK · Agriculture ──────────────────────────────────────────────────

  {
    doc_id: "aiadmk_agri_01",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Agriculture",
    promise_text_en:
      "Provide ₹7,500 per acre per year as direct farm income support to all registered farmers with cultivable land up to 10 acres.",
    promise_text_ta:
      "10 ஏக்கர் வரை சாகுபடி நிலம் உடைய பதிவுசெய்யப்பட்ட அனைத்து விவசாயிகளுக்கும் ஆண்டுக்கு ஏக்கருக்கு ₹7,500 நேரடி விவசாய வருமான உதவி.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Farmer-focused",
    amount_mentioned: "₹7,500/acre/year",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 8,
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "aiadmk_agri_02",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Agriculture",
    promise_text_en:
      "Bring all traditional tank irrigation systems (Eris) under a ₹5,000 crore restoration programme to revive 39,000 tanks across TN.",
    promise_text_ta:
      "₹5,000 கோடி மீட்சித் திட்டத்தின் கீழ் 39,000 ஏரிகளை புனரமைத்து பாரம்பரிய குளத்தாமரை நீர்ப்பாசன அமைப்பை மீட்டெடுப்போம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Infrastructure-heavy",
    amount_mentioned: "₹5,000 crore",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 9,
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "aiadmk_agri_03",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Agriculture",
    promise_text_en:
      "Establish 500 new Farmers' Producer Organisations (FPOs) to aggregate small farmers and ensure collective bargaining for crop prices.",
    promise_text_ta:
      "சிறு விவசாயிகளை ஒருங்கிணைத்து விலை பேரம் பேசும் திறன் வழங்க 500 புதிய விவசாயிகள் உற்பத்தியாளர் அமைப்புகள் (FPO) அமைப்போம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Farmer-focused",
    amount_mentioned: "500 FPOs",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 10,
    ground_truth_confidence: "MEDIUM",
  },
  {
    doc_id: "aiadmk_agri_04",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Agriculture",
    promise_text_en:
      "Expand the AIADMK-era Uzhavar Sandhai (Farmers' Market) scheme to all 234 assembly constituencies with cold-chain storage support.",
    promise_text_ta:
      "அதிமுக ஆட்சியில் துவங்கிய உழவர் சந்தை திட்டத்தை அனைத்து 234 சட்டமன்ற தொகுதிகளிலும் குளிர்சாதன கிடங்கு வசதியுடன் விரிவாக்குவோம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Farmer-focused",
    scheme_name: "Uzhavar Sandhai (Expanded)",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 11,
    ground_truth_confidence: "HIGH",
  },

  // ─── AIADMK · Education ────────────────────────────────────────────────────

  {
    doc_id: "aiadmk_edu_01",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Education",
    promise_text_en:
      "Launch the Chief Minister's School Excellence Programme, upgrading 5,000 government schools to international standards within 3 years.",
    promise_text_ta:
      "3 ஆண்டுகளுக்குள் 5,000 அரசுப் பள்ளிகளை சர்வதேச தரத்திற்கு மேம்படுத்த முதலமைச்சர் பள்ளி சிறப்புத் திட்டம் தொடங்குவோம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Reform-oriented",
    amount_mentioned: "5,000 schools",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 19,
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "aiadmk_edu_02",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Education",
    promise_text_en:
      "Provide ₹5,000 per month stipend to all engineering and polytechnic students doing internships, funded through a ₹500 crore Industry-Academia Linkage Fund.",
    promise_text_ta:
      "₹500 கோடி தொழில்-கல்வி இணைப்பு நிதி மூலம் பயிற்சி பெறும் அனைத்து பொறியியல் மற்றும் பாலிடெக்னிக் மாணவர்களுக்கும் மாதம் ₹5,000 வழங்குவோம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Reform-oriented",
    amount_mentioned: "₹5,000/month stipend",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 20,
    ground_truth_confidence: "MEDIUM",
  },
  {
    doc_id: "aiadmk_edu_03",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Education",
    promise_text_en:
      "Introduce NEET coaching support for government school students — free residential coaching for 10,000 students annually across 38 districts.",
    promise_text_ta:
      "38 மாவட்டங்களில் ஆண்டுக்கு 10,000 அரசுப் பள்ளி மாணவர்களுக்கு NEET தேர்வுக்கான இலவச தங்கும் பயிற்சி வழங்குவோம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Welfare-centric",
    amount_mentioned: "10,000 students/year",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 21,
    ground_truth_confidence: "MEDIUM",
  },

  // ─── AIADMK · TASMAC & Revenue ────────────────────────────────────────────

  {
    doc_id: "aiadmk_tasmac_01",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "TASMAC & Revenue",
    promise_text_en:
      "Implement total prohibition in a phased manner over 5 years, starting with 10 districts in the first year, replacing TASMAC revenue with expanded GST and property tax collection.",
    promise_text_ta:
      "முதல் ஆண்டில் 10 மாவட்டங்களில் தொடங்கி 5 ஆண்டுகளில் படிப்படியாக முழு மதுவிலக்கு அமல்படுத்துவோம்; TASMAC வருவாயை GST மற்றும் சொத்து வரி வசூல் மூலம் ஈடுசெய்வோம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Reform-oriented",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 30,
    source_notes: "TN TASMAC revenue is ~₹40,000 crore/year. Fiscal viability disputed by economists.",
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "aiadmk_tasmac_02",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "TASMAC & Revenue",
    promise_text_en:
      "Increase the GST revenue share demanded from the Centre by advocating for revision of TN's share from 4.079% to 6% in 16th Finance Commission submissions.",
    promise_text_ta:
      "16வது நிதி ஆயோக் சமர்ப்பிப்பில் தமிழகத்தின் GST பங்கை 4.079% இலிருந்து 6% ஆக உயர்த்த மத்திய அரசிடம் வலியுறுத்துவோம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Revenue-focused",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 31,
    ground_truth_confidence: "MEDIUM",
  },

  // ─── AIADMK · Women's Welfare ─────────────────────────────────────────────

  {
    doc_id: "aiadmk_women_01",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Women's Welfare",
    promise_text_en:
      "Provide ₹1,500 per month as direct cash support (raised from DMK's ₹1,000) to the woman head of every household — branded as Amma Magalir Urimai.",
    promise_text_ta:
      "DMK-யின் ₹1,000 திட்டத்தை ₹1,500 ஆக உயர்த்தி ஒவ்வொரு குடும்பத்தின் பெண் தலைவருக்கும் — 'அம்மா மகளிர் உரிமை' என்ற பெயரில் — நேரடி பண உதவி வழங்குவோம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Women-focused",
    amount_mentioned: "₹1,500/month",
    scheme_name: "Amma Magalir Urimai",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 38,
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "aiadmk_women_02",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Women's Welfare",
    promise_text_en:
      "Expand the Amma Two-Wheeler scheme (subsidised scooter for working women) to cover 2 lakh women per year, with subsidy increased to ₹50,000 per vehicle.",
    promise_text_ta:
      "ஆண்டுக்கு 2 லட்சம் பெண்களுக்கு அம்மா இரு சக்கர வாகன திட்டத்தை விரிவாக்கி மானியத்தை வாகனத்திற்கு ₹50,000 ஆக உயர்த்துவோம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Women-focused",
    amount_mentioned: "₹50,000 subsidy, 2 lakh women/year",
    scheme_name: "Amma Two-Wheeler Scheme (Expanded)",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 39,
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "aiadmk_women_03",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Women's Welfare",
    promise_text_en:
      "Establish one Women's Safety Command Centre in every district connected to CCTV feeds from all public spaces, with a dedicated women's police response unit.",
    promise_text_ta:
      "ஒவ்வொரு மாவட்டத்திலும் பொது இடங்களின் CCTV தொடர்பு கொண்ட பெண்கள் பாதுகாப்பு கட்டளை மையம் மற்றும் சிறப்பு பெண் காவல் பதில் அணி அமைப்போம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Women-focused",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 40,
    ground_truth_confidence: "MEDIUM",
  },

  // ─── AIADMK · Infrastructure ──────────────────────────────────────────────

  {
    doc_id: "aiadmk_infra_01",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Infrastructure",
    promise_text_en:
      "Complete the Coimbatore and Madurai Metro Rail Phase I projects within 2 years of taking office, unlocking ₹23,000 crore of stalled investment.",
    promise_text_ta:
      "பதவியேற்று 2 ஆண்டுகளுக்குள் கோயம்புத்தூர் மற்றும் மதுரை மெட்ரோ ரயில் திட்டங்களை முடித்து ₹23,000 கோடி முதலீட்டை நிறைவேற்றுவோம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Infrastructure-heavy",
    amount_mentioned: "₹23,000 crore",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 47,
    ground_truth_confidence: "HIGH",
  },
  {
    doc_id: "aiadmk_infra_02",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Infrastructure",
    promise_text_en:
      "Build 2 lakh pucca houses for the rural poor through an expanded Indiramma Housing Scheme, with each unit to have a 24-hour tap water connection.",
    promise_text_ta:
      "விரிவாக்கப்பட்ட இந்திரம்மா வீட்டுவசதி திட்டம் மூலம் 2 லட்சம் நிரந்தர வீடுகள் கட்டி ஒவ்வொன்றிலும் 24 மணி நேர குழாய் நீர் வழங்குவோம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Welfare-centric",
    amount_mentioned: "2 lakh houses",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 48,
    ground_truth_confidence: "MEDIUM",
  },
  {
    doc_id: "aiadmk_infra_03",
    party_id: "aiadmk",
    party_name: "AIADMK",
    party_color: "bg-green-700",
    category: "Infrastructure",
    promise_text_en:
      "Develop all 12 TN district towns along the NH-44 and NH-48 corridors as Tier-2 Smart Cities with optical fibre broadband and waste-water recycling plants.",
    promise_text_ta:
      "NH-44 மற்றும் NH-48 தடங்களில் உள்ள 12 மாவட்ட நகரங்களை ஒளி இழை அகலப்பட்ட இணைப்பு மற்றும் கழிவு நீர் மறுசுழற்சி ஆலைகளுடன் Tier-2 ஸ்மார்ட் சிட்டியாக மேம்படுத்துவோம்.",
    target_year: 2026,
    status: "Proposed",
    stance_vibe: "Infrastructure-heavy",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_pdf_page: 49,
    ground_truth_confidence: "MEDIUM",
  },
];
