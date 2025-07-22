import pandas as pd
from faker import Faker
import random

fake = Faker()

# Expanded Nepali business suffixes
suffixes = [
    "KIRANA PASAL", "TEA CORNER", "MOBILE REPAIR", "STATIONERY", "CLOTH STORE",
    "SWEET HOUSE", "MINI MART", "ELECTRONICS", "RESTAURANT", "PANI PASAL",
    "FANCY STORE", "AGRO FARM", "PHARMACY", "CYCLE WORKSHOP", "BEAUTY PARLOR",
    "HARDWARE CENTER", "COLD STORE", "ELECTRICALS", "PHOTO STUDIO", "TRAVELS",
    "TAILORS", "SARI PALACE", "COMPUTER CENTER", "BOOK DEPOT", "COPY HOUSE",
    "SNACKS CENTER", "DAIRY SHOP", "FISH MARKET", "MEAT SHOP", "FLOWER GARDEN"
]

# Expanded Nepali cities and areas
cities_areas = {
    "KATHMANDU": ["ASON", "THAMEL", "NEW ROAD", "PUTALISADAK", "BAGBAZAR", "KUMARIPATI"],
    "POKHARA": ["LAKESIDE", "BAIDAM", "MAHENDRAPOOL", "PRITHVI CHOWK", "HARICHOWK"],
    "BIRATNAGAR": ["NEPALGUNJ ROAD", "CHOWK BAZAR", "ADARSHNAGAR", "TINSULIA"],
    "LALITPUR": ["PULCHOWK", "JHAMSIKHEL", "KUMARIPATI", "SATDOBATO"],
    "BHAKTAPUR": ["DURBAR SQUARE", "SADAAKHUTI", "THIMI", "CHANGUNARAYAN"],
    "CHITWAN": ["NARAYANGHAT", "SAURAHA", "RATNANAGAR", "BHARATPUR"],
    "DHARAN": ["BUSBAR", "BHANU CHOWK", "TINSULIA", "VIDHYUT CHOWK"],
    "JANAKPUR": ["RAMANANDA CHOWK", "STATION ROAD", "SHIVMANDIR"],
    "HETAUDA": ["MAKAHILI CHOWK", "HIGHWAY CHOWK", "BANJARIYA"],
    "BUTWAL": ["TINKUNE", "GOLPARK", "TILOTTAMA"],
    "NEPALGUNJ": ["KOHALPUR", "MAHENDRAPUR", "BANSGHAT"],
    "ITAHARI": ["HIGHWAY CHOWK", "GOLCHHA CINEMA ROAD", "SUNSARI"],
    "DHANKUTA": ["BANSGHARI", "PAKHRIBAS", "AHALE"],
    "BIRGUNJ": ["ADARSHNAGAR", "SIMSAR", "PRASAUNI"],
    "TULSIPUR": ["MAHENDRANAGAR", "GAIRE", "TULSI BAGAR"]
}

# Generate 6000 entries
data = []
for _ in range(6000):
    city = random.choice(list(cities_areas.keys()))
    area = random.choice(cities_areas[city])
    name = fake.first_name() + " " + random.choice(suffixes)
    location = f"{city}, {area}"
    data.append([name, location])

# Create DataFrame & save as CSV
df = pd.DataFrame(data, columns=["Company_Name", "Location"])
df.to_csv("nepali_shops_6000.csv", index=False)

print("CSV file generated: 'nepali_shops_6000.csv'")