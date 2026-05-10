#!/usr/bin/env bash
set -euo pipefail

REPO="${1:-mowlsint/Voodoo_Dashboard}"

if ! command -v gh >/dev/null 2>&1; then
  echo "ERROR: GitHub CLI 'gh' not found. Install gh or create labels manually."
  exit 1
fi

echo "Creating/updating VOODOO regional labels in $REPO"

upsert_label() {
  local name="$1"
  local color="$2"
  local description="$3"

  if gh label view "$name" --repo "$REPO" >/dev/null 2>&1; then
    gh label edit "$name" --repo "$REPO" --color "$color" --description "$description" >/dev/null
    echo "updated: $name"
  else
    gh label create "$name" --repo "$REPO" --color "$color" --description "$description" >/dev/null
    echo "created: $name"
  fi
}

upsert_label "REG:NORTH_SEA" "0E8A16" "Key region: North Sea / Nordsee"
upsert_label "REG:BRITISH_ISLES" "5319E7" "Key region: British Isles maritime approaches"
upsert_label "REG:IRISH_SEA" "1D76DB" "Key region: Irish Sea"
upsert_label "REG:BALTIC" "0052CC" "Key region: Baltic Sea / Ostsee"
upsert_label "REG:HIGH_NORTH_ARCTIC" "BFD4F2" "Key region: High North / Arctic Ocean"
upsert_label "REG:CHANNEL" "D4C5F9" "Key region: English Channel / \u00c4rmelkanal"
upsert_label "REG:BISCAY_ATLANTIC_APPROACHES" "7057FF" "Key region: Bay of Biscay and Atlantic approaches"
upsert_label "REG:MEDITERRANEAN" "FBCA04" "Key region: Mediterranean Sea"
upsert_label "REG:BLACK_SEA" "24292F" "Key region: Black Sea"
upsert_label "REG:OTHER" "BDBDBD" "Region: Other / not yet classified"
upsert_label "REG:GER_BIGHT" "0E8A16" "Subregion: German Bight / Deutsche Bucht"
upsert_label "REG:SOUTHERN_NORTH_SEA" "0E8A16" "Subregion: Southern North Sea"
upsert_label "REG:DOGGER_BANK" "0E8A16" "Subregion: Dogger Bank"
upsert_label "REG:SKAGERRAK" "0E8A16" "Subregion: Skagerrak"
upsert_label "REG:KATTEGAT" "0052CC" "Subregion: Kattegat"
upsert_label "REG:WADDEN_SEA" "0E8A16" "Subregion: Wadden Sea / Wattenmeer"
upsert_label "REG:NORWEGIAN_COAST_SOUTH" "0E8A16" "Subregion: Southern Norwegian coast"
upsert_label "REG:DANISH_STRAITS" "0052CC" "Subregion: Danish Straits / D\u00e4nische Meerengen"
upsert_label "REG:ORESUND" "0052CC" "Subregion: \u00d8resund / \u00d6resund"
upsert_label "REG:GREAT_BELT" "0052CC" "Subregion: Great Belt / Gro\u00dfer Belt"
upsert_label "REG:LITTLE_BELT" "0052CC" "Subregion: Little Belt / Kleiner Belt"
upsert_label "REG:FEHMARNBELT" "0052CC" "Subregion: Fehmarnbelt"
upsert_label "REG:BORNHOLM" "0052CC" "Subregion: Bornholm / Bornholm Basin"
upsert_label "REG:GOTLAND_SEA" "0052CC" "Subregion: Gotland Sea / Gotlandsee"
upsert_label "REG:GULF_OF_FINLAND" "0052CC" "Subregion: Gulf of Finland / Finnischer Meerbusen"
upsert_label "REG:GULF_OF_BOTHNIA" "0052CC" "Subregion: Gulf of Bothnia / Bottnischer Meerbusen"
upsert_label "REG:KALININGRAD_APPROACHES" "0052CC" "Subregion: Kaliningrad approaches"
upsert_label "REG:ORKNEY_SHETLAND" "5319E7" "Subregion: Orkney / Shetland"
upsert_label "REG:HEBRIDES" "5319E7" "Subregion: Hebrides / Scottish west coast"
upsert_label "REG:CELTIC_SEA" "1D76DB" "Subregion: Celtic Sea"
upsert_label "REG:DOVER_STRAIT" "D4C5F9" "Subregion: Dover Strait / Stra\u00dfe von Dover"
upsert_label "REG:WESTERN_CHANNEL" "D4C5F9" "Subregion: Western English Channel"
upsert_label "REG:SOLENT_PORTSMOUTH" "D4C5F9" "Subregion: Solent / Portsmouth approaches"
upsert_label "REG:BAY_OF_BISCAY" "7057FF" "Subregion: Bay of Biscay / Biskaya"
upsert_label "REG:WESTERN_APPROACHES" "7057FF" "Subregion: Western Approaches"
upsert_label "REG:USHANT_OUESSANT" "7057FF" "Subregion: Ushant / Ouessant"
upsert_label "REG:IBERIAN_ATLANTIC" "7057FF" "Subregion: Iberian Atlantic coast"
upsert_label "REG:STRAIT_GIBRALTAR" "FBCA04" "Subregion: Strait of Gibraltar"
upsert_label "REG:ALBORAN_SEA" "FBCA04" "Subregion: Alboran Sea"
upsert_label "REG:WESTERN_MED" "FBCA04" "Subregion: Western Mediterranean"
upsert_label "REG:CENTRAL_MED" "FBCA04" "Subregion: Central Mediterranean"
upsert_label "REG:ADRIATIC" "FBCA04" "Subregion: Adriatic Sea"
upsert_label "REG:IONIAN" "FBCA04" "Subregion: Ionian Sea"
upsert_label "REG:AEGEAN" "FBCA04" "Subregion: Aegean Sea"
upsert_label "REG:EASTERN_MED" "FBCA04" "Subregion: Eastern Mediterranean"
upsert_label "REG:CYPRUS_LEVANT" "FBCA04" "Subregion: Cyprus / Levant"
upsert_label "REG:SUEZ_APPROACHES" "FBCA04" "Subregion: Suez approaches"
upsert_label "REG:BOSPORUS" "24292F" "Subregion: Bosporus"
upsert_label "REG:DARDANELLES" "24292F" "Subregion: Dardanelles"
upsert_label "REG:SEA_OF_MARMARA" "24292F" "Subregion: Sea of Marmara"
upsert_label "REG:BLACK_SEA_WEST" "24292F" "Subregion: Western Black Sea"
upsert_label "REG:BLACK_SEA_NORTH" "24292F" "Subregion: Northern Black Sea / Ukraine approaches"
upsert_label "REG:CRIMEA_APPROACHES" "24292F" "Subregion: Crimea approaches"
upsert_label "REG:SEA_OF_AZOV" "24292F" "Subregion: Sea of Azov"

echo "Done."
