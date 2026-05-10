#!/usr/bin/env bash
set -euo pipefail

REPO="${1:-mowlsint/Voodoo_Dashboard}"

if ! command -v gh >/dev/null 2>&1; then
  echo "ERROR: GitHub CLI 'gh' not found."
  exit 1
fi

upsert_label() {
  local name="$1"
  local color="$2"
  local description="$3"

  local encoded
  encoded="$(python3 - <<PY
import urllib.parse
print(urllib.parse.quote("""$name""", safe=""))
PY
)"

  if gh api "repos/$REPO/labels/$encoded" >/dev/null 2>&1; then
    echo "exists, skipped: $name"
  else
    gh label create "$name" --repo "$REPO" --color "$color" --description "$description" >/dev/null
    echo "created: $name"
  fi
}

echo "Creating missing VOODOO global regional labels in $REPO"
upsert_label "REG:ATLANTIC" "7057FF" "Ocean region: Atlantic"
upsert_label "REG:INDIAN_OCEAN" "C2E0C6" "Ocean region: Indian Ocean"
upsert_label "REG:INDO_PACIFIC" "00B8D9" "Theatre region: Indo-Pacific"
upsert_label "REG:EAST_ASIA" "00B8D9" "Region: East Asia maritime theatre"
upsert_label "REG:NORTH_AMERICA" "A2EEEF" "Region: North America maritime theatre"
upsert_label "REG:ARCTIC" "BFD4F2" "Region: Arctic"
upsert_label "REG:PERSIAN_GULF" "D93F0B" "Region: Persian Gulf / Arabian Gulf"
upsert_label "REG:GULF_OF_OMAN" "D93F0B" "Region: Gulf of Oman"
upsert_label "REG:STRAIT_HORMUZ" "D93F0B" "Chokepoint: Strait of Hormuz"
upsert_label "REG:IRAN_COAST" "D93F0B" "Region: Iranian coast"
upsert_label "REG:ARABIAN_SEA" "D93F0B" "Region: Arabian Sea"
upsert_label "REG:RED_SEA" "B60205" "Region: Red Sea"
upsert_label "REG:BAB_EL_MANDEB" "B60205" "Chokepoint: Bab el-Mandeb"
upsert_label "REG:GULF_OF_ADEN" "B60205" "Region: Gulf of Aden"
upsert_label "REG:HORN_OF_AFRICA" "B60205" "Region: Horn of Africa"
upsert_label "REG:WEST_AFRICA" "5319E7" "Region: West Africa maritime theatre"
upsert_label "REG:GULF_OF_GUINEA" "5319E7" "Region: Gulf of Guinea"
upsert_label "REG:CAPE_VERDE_APPROACHES" "5319E7" "Region: Cape Verde approaches"
upsert_label "REG:CARIBBEAN" "FBCA04" "Region: Caribbean Sea"
upsert_label "REG:SOUTH_AMERICA_EAST" "FBCA04" "Region: South America east coast"
upsert_label "REG:SOUTH_AMERICA_WEST" "FBCA04" "Region: South America west coast"
upsert_label "REG:PACIFIC_SOUTH_AMERICA" "FBCA04" "Region: Pacific South America"
upsert_label "REG:PACIFIC_CENTRAL_AMERICA" "FBCA04" "Region: Pacific Central America"
upsert_label "REG:MALACCA_STRAIT" "00B8D9" "Chokepoint: Malacca Strait"
upsert_label "REG:SINGAPORE_STRAIT" "00B8D9" "Chokepoint: Singapore Strait"
upsert_label "REG:SOUTH_CHINA_SEA" "00B8D9" "Region: South China Sea"
upsert_label "REG:TAIWAN_STRAIT" "00B8D9" "Chokepoint: Taiwan Strait"
upsert_label "REG:EAST_CHINA_SEA" "00B8D9" "Region: East China Sea"
upsert_label "REG:KOREA_STRAIT" "00B8D9" "Chokepoint: Korea Strait"
upsert_label "REG:GIUK_GAP" "BFD4F2" "Region: Greenland-Iceland-UK Gap"
upsert_label "REG:US_EAST_COAST" "A2EEEF" "Region: US East Coast"
echo "Done."
